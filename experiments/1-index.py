#!/usr/bin/env python3
"""Benchmark three PostgreSQL orders queries with and without secondary indexes.

Run only against a disposable/development database. Index DDL takes locks.
"""
import argparse
import csv
import os
import statistics
import time
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import psycopg
import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_CONFIG = PROJECT_ROOT / 'data-generator' / 'config' / 'database.yaml'
DEFAULT_OUTPUT = SCRIPT_DIR / 'results'

SCHEMA = 'db_project_orders'
TABLE = f'{SCHEMA}.orders'
INDEXES = {
    'idx_orders_customer_placed': f'CREATE INDEX idx_orders_customer_placed ON {TABLE} (customer_id, placed_at DESC)',
    'idx_orders_status_placed': f'CREATE INDEX idx_orders_status_placed ON {TABLE} (status, placed_at DESC)',
    'idx_orders_placed': f'CREATE INDEX idx_orders_placed ON {TABLE} (placed_at DESC)',
}
QUERIES = [
    ('Q1 Customer lookup', 'SELECT order_id, placed_at, grand_total FROM db_project_orders.orders WHERE customer_id = %s ORDER BY placed_at DESC LIMIT 20', 'idx_orders_customer_placed'),
    ('Q2 Recent delivered', "SELECT order_id, placed_at, grand_total FROM db_project_orders.orders WHERE status = 'delivered' ORDER BY placed_at DESC LIMIT 100", 'idx_orders_status_placed'),
    ('Q3 Weekly revenue', "SELECT placed_at::date AS day, SUM(grand_total) AS revenue FROM db_project_orders.orders WHERE placed_at >= %s AND placed_at < %s GROUP BY placed_at::date ORDER BY day", 'idx_orders_placed'),
]


def connection_kwargs(config_path):
    config = yaml.safe_load(Path(config_path).read_text()) or {}
    data = config.get('postgres', config.get('database', config))
    if not isinstance(data, dict):
        raise ValueError('Expected a postgres: mapping in database.yaml')

    kwargs = {}
    for target, source_keys in {
        'host': ('host',), 'port': ('port',),
        'dbname': ('dbname', 'database', 'name'),
        'user': ('user', 'username'),
    }.items():
        value = next((data[key] for key in source_keys if data.get(key) is not None), None)
        if value is not None:
            kwargs[target] = value

    password_env = data.get('password_env')
    if password_env:
        password = os.environ.get(password_env)
        if not password:
            raise RuntimeError(
                f'Environment variable {password_env} is not set. '
                f'Export it before running the benchmark.'
            )
        kwargs['password'] = password
    elif data.get('password') is not None:
        kwargs['password'] = data['password']
    elif os.environ.get('PGPASSWORD'):
        kwargs['password'] = os.environ['PGPASSWORD']
    return kwargs


def index_exists(conn, name):
    return conn.execute('SELECT to_regclass(%s)', (f'{SCHEMA}.{name}',)).fetchone()[0] is not None


def get_parameters(conn):
    # Choose a real customer, so Q1 does not accidentally measure a missing key.
    customer = conn.execute(f'SELECT customer_id FROM {TABLE} ORDER BY order_id DESC LIMIT 1').fetchone()
    dates = conn.execute(f'SELECT max(placed_at) FROM {TABLE}').fetchone()
    if not customer or not dates[0]:
        raise RuntimeError('orders is empty')
    end = dates[0]
    from datetime import timedelta
    start = end - timedelta(days=7)
    return [(customer[0],), (), (start, end)]


def measure(conn, sql, params, repetitions, timeout_ms):
    values = []
    # Fetch all rows so every run does equivalent work; timing is client-observed latency.
    for _ in range(repetitions):
        start = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cur.fetchall()
        values.append((time.perf_counter() - start) * 1000)
    return values


def run_group(conn, params, repetitions, timeout_ms):
    results = {}
    for (label, sql, _), param in zip(QUERIES, params):
        # Warm-up is not included in the statistics.
        measure(conn, sql, param, 1, timeout_ms)
        samples = measure(conn, sql, param, repetitions, timeout_ms)
        results[label] = samples
        print(f'  {label}: median {statistics.median(samples):,.2f} ms; samples {[round(x, 2) for x in samples]}', flush=True)
    return results


def plot(indexed, unindexed, output):
    labels = list(indexed)
    x = list(range(len(labels)))
    fig, ax = plt.subplots(figsize=(11, 6))
    width = 0.36
    left = ax.bar([i - width / 2 for i in x], [statistics.median(indexed[k]) for k in labels],
                  width, label='With secondary indexes')
    right = ax.bar([i + width / 2 for i in x], [statistics.median(unindexed[k]) for k in labels],
                   width, label='Without secondary indexes')
    ax.set_xticks(x, labels)
    ax.set_ylabel('Median query latency (ms)')
    ax.set_title('PostgreSQL orders: secondary-index performance')
    ax.legend()
    ax.grid(axis='y', alpha=0.2)
    ax.set_axisbelow(True)
    for bars in (left, right):
        for bar in bars:
            ax.annotate(f'{bar.get_height():,.1f}', (bar.get_x() + bar.get_width()/2, bar.get_height()),
                        xytext=(0, 3), textcoords='offset points', ha='center', va='bottom', fontsize=9)
    fig.tight_layout()
    fig.savefig(output, dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default=str(DEFAULT_CONFIG))
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT))
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--timeout-ms', type=int, default=120000)
    parser.add_argument('--confirm-drop', action='store_true', help='Required: allows dropping three existing secondary indexes')
    args = parser.parse_args()
    if not args.confirm_drop:
        parser.error('Add --confirm-drop after reviewing the indexes that will be temporarily dropped.')
    if args.repetitions < 1:
        parser.error('--repetitions must be >= 1')
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(**connection_kwargs(args.config), autocommit=True) as conn:
        conn.execute("SET application_name = 'orders_index_benchmark'")
        conn.execute(
            "SELECT set_config('statement_timeout', %s, false)",
            (str(args.timeout_ms),)
        )
        conn.execute(
            "SELECT set_config('lock_timeout', %s, false)",
            ("30000",)
        )
        missing = [name for name in INDEXES if not index_exists(conn, name)]
        if missing:
            raise RuntimeError(f'Expected indexes missing; refusing to proceed: {missing}')
        params = get_parameters(conn)
        print('Running indexed queries...', flush=True)
        indexed = run_group(conn, params, args.repetitions, args.timeout_ms)
        dropped = []
        try:
            # CONCURRENTLY minimizes blocking of concurrent reads/writes; each statement autocommits.
            for name in INDEXES:
                print(f'Dropping {name}...', flush=True)
                conn.execute(f'DROP INDEX CONCURRENTLY {SCHEMA}.{name}')
                dropped.append(name)
            # Disable parallel seq scans for consistent single-worker baseline? No: keep planner defaults.
            # Avoid PostgreSQL auto-creating hypothetical indexes: no such behavior by default.
            print('Running unindexed queries...', flush=True)
            unindexed = run_group(conn, params, args.repetitions, args.timeout_ms)
        finally:
            # Attempt to restore all missing indexes even if one restoration fails.
            # A failed CONCURRENTLY operation can leave an INVALID index; clean it up first.
            errors = []
            for name, ddl in INDEXES.items():
                try:
                    row = conn.execute('''SELECT i.indisvalid FROM pg_index i
                        JOIN pg_class c ON c.oid=i.indexrelid
                        JOIN pg_namespace n ON n.oid=c.relnamespace
                        WHERE n.nspname=%s AND c.relname=%s''', (SCHEMA, name)).fetchone()
                    if row is not None and not row[0]:
                        conn.execute(f'DROP INDEX CONCURRENTLY {SCHEMA}.{name}')
                        row = None
                    if row is None:
                        print(f'Restoring {name} (may take several minutes)...', flush=True)
                        conn.execute(ddl.replace('CREATE INDEX ', 'CREATE INDEX CONCURRENTLY ', 1))
                except Exception as exc:
                    errors.append(f'{name}: {exc}')
                    print(f'ERROR restoring {name}: {exc}', flush=True)
            if errors:
                raise RuntimeError('INDEX RESTORATION FAILED. Restore manually: ' + '; '.join(errors))
        csv_path = out / 'index_latency.csv'
        with csv_path.open('w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['query', 'condition', 'run', 'latency_ms'])
            for condition, data in [('with_index', indexed), ('without_index', unindexed)]:
                for label, values in data.items():
                    for i, ms in enumerate(values, 1):
                        writer.writerow([label, condition, i, f'{ms:.4f}'])
        image_path = out / 'index_latency.png'
        plot(indexed, unindexed, image_path)
        print(f'Indexes restored. Results: {csv_path} and {image_path}')
        print('NOTE: Other indexes (primary keys, unique constraints) remain in place.')

if __name__ == '__main__':
    main()
