"""Resumable synthetic inventory balance loader for PostgreSQL 15+.

Run: python main.py inventory_load (or python generators/inventory.py).
Requires: pip install 'psycopg[binary]' PyYAML

Design:
* Exactly 2% high, 13% medium, 35% low, 50% unstocked, rounded by
  largest remainders, among the ACTUAL variant rows in the database.
* 'Very high' is a configurable subset of high, not an additional share.
* Target 17,500,000 balances by default; configurable 15M-20M.
* All balances have lot_id=NULL and quantity_reserved=0. Inventory lots
  and reservations are intentionally not generated.
* Deterministic assignment, batched COPY staging, idempotent ON CONFLICT.
* Checkpoint advances only after the corresponding DB transaction commits.
* Requires the catalog variant set and warehouse/bin layout to remain fixed
  during the run. A new run after changing either requires a fresh checkpoint.

IMPORTANT: This creates ONE balance per (variant, warehouse) at most, and
uses a different warehouse for additional balances. It does NOT imply that
one physical bin holds only one SKU: many variants can share a bin.
"""
from __future__ import annotations

import hashlib
import logging
import math
import os
from pathlib import Path

import psycopg
from psycopg import sql
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / 'config' / 'database.yaml'
CHECKPOINT = ROOT / 'checkpoints' / 'inventory.yaml'
LOG = logging.getLogger('inventory')
DEFAULT_TARGET = 17_500_000
DEFAULT_BATCH = 20_000
DEFAULT_SEED = 20261113
MIN_ID = 1
MAX_ID = 28_673_165


def yaml_read(path):
    if not path.exists():
        return {}
    with path.open(encoding='utf-8') as f:
        result = yaml.safe_load(f) or {}
    if not isinstance(result, dict):
        raise ValueError(f'Expected YAML mapping in {path}')
    return result


def atomic_yaml(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.yaml.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        yaml.safe_dump(state, f, sort_keys=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def connect():
    config = yaml_read(CONFIG)
    pg = config.get('postgres', config)
    if not isinstance(pg, dict):
        raise ValueError('database.yaml must contain a postgres mapping')
    password = pg.get('password')
    if pg.get('password_env'):
        password = os.environ.get(str(pg['password_env']))
        if password is None:
            raise ValueError(f"Environment variable {pg['password_env']} is not set")
    kwargs = dict(host=pg.get('host', 'localhost'), port=int(pg.get('port', 5432)),
                  dbname=pg.get('database', pg.get('dbname')), user=pg.get('user'),
                  password=password)
    return psycopg.connect(**{k: v for k, v in kwargs.items() if v is not None})


def h64(*parts):
    data = ':'.join(map(str, parts)).encode()
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), 'big')


def shares(n):
    """Exact total with largest-remainder allocation."""
    weights = [('high', 2), ('medium', 13), ('low', 35), ('unstocked', 50)]
    counts = {name: n * weight // 100 for name, weight in weights}
    remainder = n - sum(counts.values())
    order = sorted(weights, key=lambda item: (-(n * item[1] % 100),
                                               [x[0] for x in weights].index(item[0])))
    for name, _ in order[:remainder]:
        counts[name] += 1
    return counts


def coprime_multiplier(n, seed):
    if n <= 1:
        return 1
    a = (h64(seed, 'permutation') % (n - 1)) + 1
    while math.gcd(a, n) != 1:
        a = a + 1 if a < n - 1 else 1
    return a


def tier_for_rank(rank, n, a, offset, counts):
    # Bijection on ranks 0..n-1: yields EXACT tier counts, independent
    # of gaps in variant_id or physical row order.
    p = (rank * a + offset) % n
    if p < counts['high']:
        return 'high', p
    p -= counts['high']
    if p < counts['medium']:
        return 'medium', p
    p -= counts['medium']
    if p < counts['low']:
        return 'low', p
    return 'unstocked', p - counts['low']


def extra_distribution(counts, target, warehouse_count):
    stocked = counts['high'] + counts['medium'] + counts['low']
    if target < stocked:
        raise ValueError(f'Target {target:,} is smaller than stocked variant count {stocked:,}')
    if target > stocked * warehouse_count:
        raise ValueError('Target exceeds one balance per variant per warehouse capacity')
    # Allocate extra warehouse appearances to high first, then medium,
    # then low; each variant can have at most one row per warehouse.
    remaining = target - stocked
    plan = {}
    for tier in ('high', 'medium', 'low'):
        count = counts[tier]
        capacity = count * (warehouse_count - 1)
        take = min(remaining, capacity)
        plan[tier] = divmod(take, count) if count else (0, 0)
        remaining -= take
    if remaining:
        raise ValueError('Insufficient warehouse capacity')
    return plan


def load_bins(conn):
    # ~1M (warehouse_id, bin_id) pairs; modest RAM compared to caching
    # millions of variant records. Only active, pickable bins are used.
    result = {}
    with conn.cursor(name='inventory_bin_stream') as cur:
        cur.execute('''SELECT warehouse_id, bin_id
                       FROM db_project_warehouse.storage_bins
                       WHERE is_active AND is_pickable
                       ORDER BY warehouse_id, bin_id''')
        for wid, bid in cur:
            result.setdefault(wid, []).append(bid)
    return result


def verify_catalog(conn):
    with conn.cursor() as cur:
        cur.execute('''SELECT count(*), min(variant_id), max(variant_id)
                       FROM db_project_catalog.product_variants''')
        n, lo, hi = cur.fetchone()
    if not n:
        raise ValueError('No product variants found')
    if lo != MIN_ID or hi != MAX_ID:
        raise ValueError(f'Variant ID bounds changed: got {lo}..{hi}, expected {MIN_ID}..{MAX_ID}')
    return n


def prepare_stage(conn):
    with conn.cursor() as cur:
        cur.execute('''CREATE TEMP TABLE IF NOT EXISTS inventory_stage (
            warehouse_id bigint NOT NULL, bin_id bigint NOT NULL,
            variant_id bigint NOT NULL, quantity_on_hand bigint NOT NULL
        ) ON COMMIT PRESERVE ROWS''')
    conn.commit()


def flush(conn, rows):
    if not rows:
        return
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute('TRUNCATE inventory_stage')
            with cur.copy('''COPY inventory_stage
                (warehouse_id, bin_id, variant_id, quantity_on_hand)
                FROM STDIN''') as cp:
                for row in rows:
                    cp.write_row(row)
            # A replay after a crash is safe. Repeated rows are ignored.
            # Stage rows are unique by construction (one warehouse per
            # variant), and the selected bin is deterministic.
            cur.execute('''INSERT INTO db_project_inventory.inventory_balances
                (warehouse_id, bin_id, variant_id, lot_id,
                 quantity_on_hand, quantity_reserved)
                SELECT warehouse_id, bin_id, variant_id, NULL,
                       quantity_on_hand, 0
                FROM inventory_stage
                ON CONFLICT (bin_id, variant_id, lot_key) DO NOTHING''')


def quantity(tier, rank, seed, very_high_percent):
    if tier == 'high':
        # Very high is a SUBSET of the 2% high-demand group.
        if h64(seed, 'veryhigh', rank) % 100 < very_high_percent:
            return 500 + h64(seed, 'qty-vhigh', rank) % 4501
        return 100 + h64(seed, 'qty-high', rank) % 401
    if tier == 'medium':
        return 20 + h64(seed, 'qty-medium', rank) % 81
    return 1 + h64(seed, 'qty-low', rank) % 20


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    config = yaml_read(CONFIG).get('inventory', {})
    if not isinstance(config, dict):
        raise ValueError('inventory configuration must be a mapping')
    target = int(config.get('target_balances', DEFAULT_TARGET))
    batch_size = int(config.get('batch_size', DEFAULT_BATCH))
    seed = int(config.get('seed', DEFAULT_SEED))
    very_high_percent = int(config.get('very_high_percent_of_high', 10))
    if not 15_000_000 <= target <= 20_000_000:
        raise ValueError('target_balances must be between 15M and 20M')
    if batch_size < 1 or not 0 <= very_high_percent <= 100:
        raise ValueError('Invalid batch_size or very_high_percent_of_high')

    # Separate connections: the named read cursor streams variants while
    # the write connection commits batches without invalidating that cursor.
    with connect() as read_conn, connect() as write_conn:
        n = verify_catalog(read_conn)
        bin_map = load_bins(read_conn)
        if not bin_map:
            raise ValueError('No active, pickable storage bins found')
        warehouse_ids = sorted(bin_map)
        w = len(warehouse_ids)
        counts = shares(n)
        plan = extra_distribution(counts, target, w)
        multiplier = coprime_multiplier(n, seed)
        offset = h64(seed, 'offset') % n
        signature = dict(variant_count=n, min_variant_id=MIN_ID,
                         max_variant_id=MAX_ID, warehouse_ids=warehouse_ids,
                         bin_counts={str(k): len(v) for k, v in bin_map.items()},
                         target=target, seed=seed, very_high_percent=very_high_percent,
                         multiplier=multiplier, offset=offset)
        signature_hash = hashlib.sha256(
            yaml.safe_dump(signature, sort_keys=True).encode()).hexdigest()
        state = yaml_read(CHECKPOINT)
        if state and state.get('signature') != signature_hash:
            raise ValueError('Inventory source/configuration changed; checkpoint cannot safely resume')
        completed_rank = int(state.get('completed_rank', 0))
        if not 0 <= completed_rank <= n:
            raise ValueError('Invalid inventory checkpoint rank')
        LOG.info('Variants=%s, tiers=%s, warehouses=%s, target=%s, resume rank=%s',
                 f'{n:,}', counts, w, f'{target:,}', f'{completed_rank:,}')
        prepare_stage(write_conn)
        # Use an index scan over actual IDs, NOT generate_series(1, max_id).
        # This correctly handles any gaps in IDENTITY IDs.
        with read_conn.cursor(name='inventory_variant_stream') as cur:
            cur.execute('''SELECT variant_id FROM db_project_catalog.product_variants
                           ORDER BY variant_id''')
            rank = 0
            rows = []
            last_rank = completed_rank
            for (variant_id,) in cur:
                if rank < completed_rank:
                    rank += 1
                    continue
                tier, within_tier = tier_for_rank(rank, n, multiplier, offset, counts)
                if tier != 'unstocked':
                    base, remainder = plan[tier]
                    # Exactly 'remainder' variants in this tier get one
                    # additional warehouse; no probabilistic count drift.
                    appearances = 1 + base + (within_tier < remainder)
                    start = h64(seed, 'warehouse', variant_id) % w
                    qty = quantity(tier, within_tier, seed, very_high_percent)
                    for j in range(appearances):
                        wid = warehouse_ids[(start + j) % w]
                        bins = bin_map[wid]
                        bid = bins[h64(seed, 'bin', variant_id, wid) % len(bins)]
                        rows.append((wid, bid, variant_id, qty))
                rank += 1
                last_rank = rank
                if len(rows) >= batch_size:
                    flush(write_conn, rows)
                    atomic_yaml(CHECKPOINT, dict(signature=signature_hash,
                                                 completed_rank=last_rank,
                                                 completed=last_rank == n))
                    rows.clear()
                    if last_rank % 1_000_000 < batch_size:
                        LOG.info('Processed %s/%s variants', f'{last_rank:,}', f'{n:,}')
            flush(write_conn, rows)
            atomic_yaml(CHECKPOINT, dict(signature=signature_hash,
                                         completed_rank=last_rank, completed=True))
        LOG.info('Finished. Target rows=%s; verify actual count with SQL.', f'{target:,}')


if __name__ == '__main__':
    main()
