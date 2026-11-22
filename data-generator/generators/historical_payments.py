"""Parallel, restart-safe historical payment loader. Run: python main.py historical_payments_load.

The PostgreSQL progress table is authoritative. YAML is a human-readable progress mirror.
Do not run multiple instances or mutate source orders while this loader is running.
"""
from __future__ import annotations

import hashlib
import logging
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql
import yaml

ROOT = Path(__file__).resolve().parent.parent
DB_CONFIG = ROOT / 'config' / 'database.yaml'
SETTINGS = ROOT / 'config' / 'historical_payments.yaml'
CHECKPOINT = ROOT / 'checkpoints' / 'historical_payments.yaml'
LOG = logging.getLogger('historical_payments')
DEFAULTS = dict(seed=20261121, workers=5, batch_orders=20000, shard_size=250000,
                retry_probability=0.012, auth_capture_probability=0.10,
                refund_probability=0.008, pending_probability=0.0)
PROVIDERS = [('stripe', 'Stripe'), ('paypal', 'PayPal'), ('adyen', 'Adyen'),
             ('braintree', 'Braintree')]
LOCK = threading.Lock()


def read_yaml(path):
    if not path.exists():
        return {}
    with path.open(encoding='utf8') as f:
        result = yaml.safe_load(f) or {}
    if not isinstance(result, dict):
        raise ValueError(f'Expected YAML mapping: {path}')
    return result


def connect():
    config = read_yaml(DB_CONFIG)
    cfg = config.get('postgres', config)
    password = os.environ.get(str(cfg['password_env'])) if cfg.get('password_env') else cfg.get('password')
    if cfg.get('password_env') and password is None:
        raise ValueError('Missing configured password environment variable')
    return psycopg.connect(host=cfg.get('host', 'localhost'), port=int(cfg.get('port', 5432)),
                           dbname=cfg.get('database', cfg.get('dbname')),
                           user=cfg.get('user'), password=password)


def h(*values):
    return int.from_bytes(hashlib.blake2b('|'.join(map(str, values)).encode(), digest_size=8).digest(), 'big')


def checkpoint(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT shard_start, next_order_id, shard_end FROM db_project_payments.synthetic_payment_progress ORDER BY shard_start')
        shards = [{'start': a, 'next_order_id': b, 'end': c} for a, b, c in cur.fetchall()]
    path = CHECKPOINT
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.yaml.tmp')
    with LOCK:
        with tmp.open('w', encoding='utf8') as f:
            yaml.safe_dump({'note': 'Informational only. PostgreSQL progress table is authoritative.',
                            'shards': shards}, f, sort_keys=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)


def setup(conn, cfg):
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS db_project_payments.synthetic_payment_progress (
                shard_start BIGINT PRIMARY KEY, shard_end BIGINT NOT NULL,
                next_order_id BIGINT NOT NULL, seed BIGINT NOT NULL,
                processed_orders BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now())''')
            for code, name in PROVIDERS:
                cur.execute('''INSERT INTO db_project_payments.payment_providers
                    (provider_code, provider_name) VALUES (%s,%s)
                    ON CONFLICT (provider_code) DO NOTHING''', (code, name))
            cur.execute('''SELECT provider_code, provider_id FROM db_project_payments.payment_providers
                           WHERE provider_code = ANY(%s)''', ([p[0] for p in PROVIDERS],))
            providers = dict(cur.fetchall())
            if len(providers) != len(PROVIDERS):
                raise RuntimeError('Missing payment providers')
            cur.execute('SELECT min(order_id), max(order_id) FROM db_project_orders.orders')
            minimum, maximum = cur.fetchone()
            if minimum is None:
                raise RuntimeError('Orders table is empty')
            cur.execute('SELECT count(*) FROM db_project_payments.synthetic_payment_progress')
            existing = cur.fetchone()[0]
            if not existing:
                for start in range(minimum, maximum + 1, cfg['shard_size']):
                    end = min(start + cfg['shard_size'] - 1, maximum)
                    cur.execute('''INSERT INTO db_project_payments.synthetic_payment_progress
                        (shard_start, shard_end, next_order_id, seed) VALUES (%s,%s,%s,%s)''',
                        (start, end, start, cfg['seed']))
            else:
                cur.execute('''SELECT count(*) FROM db_project_payments.synthetic_payment_progress
                               WHERE seed <> %s OR shard_end - shard_start + 1 > %s''',
                            (cfg['seed'], cfg['shard_size']))
                if cur.fetchone()[0]:
                    raise RuntimeError('Checkpoint incompatible with seed or shard_size')
                cur.execute('SELECT min(shard_start), max(shard_end) FROM db_project_payments.synthetic_payment_progress')
                old_min, old_max = cur.fetchone()
                if (old_min, old_max) != (minimum, maximum):
                    raise RuntimeError('Order ID range changed since initial run')
    return providers


def reserve_ids(cur, table, column, count):
    if not count:
        return iter(())
    cur.execute('SELECT nextval(pg_get_serial_sequence(%s,%s)) FROM generate_series(1,%s)',
                (f'db_project_payments.{table}', column, count))
    return iter(row[0] for row in cur.fetchall())


def copy_rows(cur, table, cols, rows):
    if not rows:
        return
    statement = sql.SQL('COPY {}.{} ({}) FROM STDIN').format(
        sql.Identifier('db_project_payments'), sql.Identifier(table),
        sql.SQL(', ').join(map(sql.Identifier, cols)))
    with cur.copy(statement) as cp:
        for row in rows:
            cp.write_row(row)


INTENT_COLS = ('payment_intent_id', 'order_id', 'customer_id', 'provider_id',
               'payment_method_id', 'idempotency_key', 'provider_intent_reference',
               'amount', 'currency_code', 'status', 'created_at', 'updated_at')
TX_COLS = ('payment_transaction_id', 'payment_intent_id', 'transaction_type',
           'status', 'amount', 'currency_code', 'provider_transaction_reference',
           'failure_code', 'failure_message', 'created_at', 'completed_at')


def build_order(order, cfg, providers):
    order_id, customer_id, status, currency, subtotal, discount, tax, shipping, placed, cancelled = order
    amount = subtotal - discount + tax + shipping
    if amount <= 0:
        return [], []  # Intent schema requires positive amounts.
    rng = random.Random(h(cfg['seed'], order_id))
    provider = providers[rng.choices(['stripe', 'paypal', 'adyen', 'braintree'],
                                    weights=[65, 22, 10, 3])[0]]
    cancelled_order = status == 'cancelled'
    pending_order = status == 'pending_payment'
    # Each intent: (provider_id, idempotency_key, provider_ref, amount, currency, status, created, updated, events)
    intents = []
    def make_intent(n, state, start, events):
        ref = f'SYN-PI-{cfg["seed"]}-{order_id}-{n}'
        last = max([start] + [event[5] or event[4] for event in events])
        intents.append((provider, ref, ref, amount, currency, state, start, last, events))

    if pending_order:
        make_intent(1, 'created', placed, [])
    elif cancelled_order:
        # A cancelled order usually has no capture; some authorizations are voided.
        if rng.random() < 0.35:
            auth = placed + timedelta(seconds=15)
            void = (cancelled or placed + timedelta(minutes=30))
            if void < auth:
                void = auth
            make_intent(1, 'cancelled', placed, [
                ('authorization', 'succeeded', amount, None, auth, auth),
                ('void', 'succeeded', amount, None, void, void)])
        else:
            make_intent(1, 'cancelled', placed, [])
    else:
        if rng.random() < cfg['retry_probability']:
            failed = placed + timedelta(seconds=12)
            make_intent(1, 'failed', placed, [
                ('authorization', 'failed', amount, 'card_declined', failed, failed)])
            placed = placed + timedelta(minutes=3)
            n = 2
        else:
            n = 1
        captured = placed + timedelta(seconds=20 + rng.randrange(100))
        events = []
        if rng.random() < cfg['auth_capture_probability']:
            auth = placed + timedelta(seconds=10)
            events.append(('authorization', 'succeeded', amount, None, auth, auth))
            captured = placed + timedelta(minutes=2 + rng.randrange(90))
        events.append(('capture', 'succeeded', amount, None, captured, captured))
        final_status = 'captured'
        # Refund event without payment_refunds row is allowed by schema, but not a complete refund workflow.
        if rng.random() < cfg['refund_probability']:
            refund_time = captured + timedelta(days=1 + rng.randrange(12))
            if refund_time <= cfg['_now']:
                refund_amount = amount if rng.random() < 0.6 else (amount * Decimal('0.25')).quantize(Decimal('0.0001'))
                if refund_amount > 0:
                    events.append(('refund', 'succeeded', refund_amount, None, refund_time, refund_time))
        make_intent(n, final_status, placed, events)
    return intents, [(order_id, customer_id)]


def run_shard(shard, cfg, providers):
    start, end, next_id = shard
    with connect() as conn:
        processed = 0
        while next_id <= end:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute('''SELECT next_order_id FROM db_project_payments.synthetic_payment_progress
                                   WHERE shard_start=%s FOR UPDATE''', (start,))
                    next_id = cur.fetchone()[0]
                    if next_id > end:
                        break
                    cur.execute('''SELECT order_id, customer_id, status, currency_code, subtotal,
                                          discount_total, tax_total, shipping_total, placed_at, cancelled_at
                                   FROM db_project_orders.orders
                                   WHERE order_id >= %s AND order_id <= %s
                                   ORDER BY order_id LIMIT %s''', (next_id, end, cfg['batch_orders']))
                    orders = cur.fetchall()
                    if not orders:
                        cur.execute('''UPDATE db_project_payments.synthetic_payment_progress
                                       SET next_order_id=%s, updated_at=now() WHERE shard_start=%s''',
                                    (end + 1, start))
                        break
                    generated = []
                    for order in orders:
                        its, _ = build_order(order, cfg, providers)
                        generated.append((order, its))
                    intent_count = sum(len(its) for _, its in generated)
                    tx_count = sum(len(it[-1]) for _, its in generated for it in its)
                    intent_ids = reserve_ids(cur, 'payment_intents', 'payment_intent_id', intent_count)
                    tx_ids = reserve_ids(cur, 'payment_transactions', 'payment_transaction_id', tx_count)
                    intents, transactions = [], []
                    for order, items in generated:
                        order_id, customer_id = order[:2]
                        for provider, key, ref, amount, currency, state, created, updated, events in items:
                            intent_id = next(intent_ids)
                            intents.append((intent_id, order_id, customer_id, provider, None,
                                            key, ref, amount, currency, state, created, updated))
                            for event_index, (kind, tx_state, tx_amount, failure, event_created, event_completed) in enumerate(events, 1):
                                transactions.append((next(tx_ids), intent_id, kind, tx_state, tx_amount,
                                                     currency, f'SYN-TX-{cfg["seed"]}-{order_id}-{key.rsplit("-",1)[-1]}-{event_index}',
                                                     failure, 'Synthetic card decline' if failure else None,
                                                     event_created, event_completed))
                    copy_rows(cur, 'payment_intents', INTENT_COLS, intents)
                    copy_rows(cur, 'payment_transactions', TX_COLS, transactions)
                    last_id = orders[-1][0]
                    cur.execute('''UPDATE db_project_payments.synthetic_payment_progress
                                   SET next_order_id=%s, processed_orders=processed_orders+%s,
                                       updated_at=now() WHERE shard_start=%s''',
                                (last_id + 1, len(orders), start))
            next_id = last_id + 1
            processed += len(orders)
            if processed % (cfg['batch_orders'] * 10) < cfg['batch_orders']:
                LOG.info('Shard %s: %s orders processed this run, next ID %s', start, processed, next_id)
    return processed


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    cfg = {**DEFAULTS, **read_yaml(SETTINGS)}
    for key in ('workers', 'batch_orders', 'shard_size'):
        cfg[key] = int(cfg[key])
        if cfg[key] < 1:
            raise ValueError(f'{key} must be positive')
    for key in ('retry_probability', 'auth_capture_probability', 'refund_probability'):
        if not 0 <= float(cfg[key]) <= 1:
            raise ValueError(f'{key} must be between 0 and 1')
    from datetime import datetime, timezone
    cfg['_now'] = datetime.now(timezone.utc)
    with connect() as conn:
        providers = setup(conn, cfg)
        with conn.cursor() as cur:
            cur.execute('''SELECT shard_start, shard_end, next_order_id
                           FROM db_project_payments.synthetic_payment_progress
                           WHERE next_order_id <= shard_end ORDER BY shard_start''')
            shards = cur.fetchall()
        conn.commit()
        checkpoint(conn)
    LOG.info('Remaining shards: %s, workers: %s', len(shards), cfg['workers'])
    with ThreadPoolExecutor(max_workers=cfg['workers']) as pool:
        futures = [pool.submit(run_shard, shard, cfg, providers) for shard in shards]
        for future in as_completed(futures):
            future.result()  # propagate errors; DB batch transactions already committed stay committed
            with connect() as conn:
                checkpoint(conn)
    LOG.info('Completed all payment shards')


if __name__ == '__main__':
    main()
