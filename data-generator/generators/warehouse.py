"""Synthetic, resumable PostgreSQL warehouse loader.

Run via `python main.py warehouse_load` (add dispatcher), or `python generators/warehouse.py`.
Requires: pip install 'psycopg[binary]' PyYAML

Checkpoints are advisory. Database uniqueness constraints are the source of truth;
all IDs are resolved from stable codes on resume. One warehouse is processed by
one worker, with a separate connection. All inserts are committed per batch.
"""
from __future__ import annotations

import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import psycopg
from psycopg import sql
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / 'config' / 'database.yaml'
CHECKPOINT_DIR = ROOT / 'checkpoints' / 'warehouse_files'
SCHEMA = 'db_project_warehouse'
LOG = logging.getLogger('warehouse')

# Override these in config/database.yaml under `warehouse:`.
DEFAULTS = {
    'warehouses': 50,
    'zones_per_warehouse': 8,
    'aisles_per_zone': 13,
    'racks_per_aisle': 19,
    'bins_per_rack': 10,
    'batch_size': 10000,
    'workers': 4,
    'seed': 20261113,
}

# City, state, postal prefix, IANA timezone. Locations are plausible, street
# addresses generated below are deliberately fictitious, not real facilities.
US_LOCATIONS = [
    ('New York', 'NY', '100', 'America/New_York'),
    ('Buffalo', 'NY', '142', 'America/New_York'),
    ('Boston', 'MA', '021', 'America/New_York'),
    ('Philadelphia', 'PA', '191', 'America/New_York'),
    ('Pittsburgh', 'PA', '152', 'America/New_York'),
    ('Washington', 'DC', '200', 'America/New_York'),
    ('Baltimore', 'MD', '212', 'America/New_York'),
    ('Charlotte', 'NC', '282', 'America/New_York'),
    ('Raleigh', 'NC', '276', 'America/New_York'),
    ('Atlanta', 'GA', '303', 'America/New_York'),
    ('Savannah', 'GA', '314', 'America/New_York'),
    ('Jacksonville', 'FL', '322', 'America/New_York'),
    ('Orlando', 'FL', '328', 'America/New_York'),
    ('Miami', 'FL', '331', 'America/New_York'),
    ('Tampa', 'FL', '336', 'America/New_York'),
    ('Columbus', 'OH', '432', 'America/New_York'),
    ('Cleveland', 'OH', '441', 'America/New_York'),
    ('Cincinnati', 'OH', '452', 'America/New_York'),
    ('Detroit', 'MI', '482', 'America/Detroit'),
    ('Indianapolis', 'IN', '462', 'America/Indiana/Indianapolis'),
    ('Chicago', 'IL', '606', 'America/Chicago'),
    ('Milwaukee', 'WI', '532', 'America/Chicago'),
    ('Minneapolis', 'MN', '554', 'America/Chicago'),
    ('Fargo', 'ND', '581', 'America/Chicago'),
    ('Sioux Falls', 'SD', '571', 'America/Chicago'),
    ('Omaha', 'NE', '681', 'America/Chicago'),
    ('Kansas City', 'MO', '641', 'America/Chicago'),
    ('St. Louis', 'MO', '631', 'America/Chicago'),
    ('Nashville', 'TN', '372', 'America/Chicago'),
    ('Memphis', 'TN', '381', 'America/Chicago'),
    ('Louisville', 'KY', '402', 'America/New_York'),
    ('Birmingham', 'AL', '352', 'America/Chicago'),
    ('New Orleans', 'LA', '701', 'America/Chicago'),
    ('Dallas', 'TX', '752', 'America/Chicago'),
    ('Fort Worth', 'TX', '761', 'America/Chicago'),
    ('Houston', 'TX', '770', 'America/Chicago'),
    ('Austin', 'TX', '787', 'America/Chicago'),
    ('San Antonio', 'TX', '782', 'America/Chicago'),
    ('Oklahoma City', 'OK', '731', 'America/Chicago'),
    ('Denver', 'CO', '802', 'America/Denver'),
    ('Salt Lake City', 'UT', '841', 'America/Denver'),
    ('Albuquerque', 'NM', '871', 'America/Denver'),
    ('Phoenix', 'AZ', '850', 'America/Phoenix'),
    ('Tucson', 'AZ', '857', 'America/Phoenix'),
    ('Las Vegas', 'NV', '891', 'America/Los_Angeles'),
    ('Los Angeles', 'CA', '900', 'America/Los_Angeles'),
    ('San Diego', 'CA', '921', 'America/Los_Angeles'),
    ('San Francisco', 'CA', '941', 'America/Los_Angeles'),
    ('Sacramento', 'CA', '958', 'America/Los_Angeles'),
    ('Portland', 'OR', '972', 'America/Los_Angeles'),
    ('Seattle', 'WA', '981', 'America/Los_Angeles'),
    ('Spokane', 'WA', '992', 'America/Los_Angeles'),
    ('Boise', 'ID', '837', 'America/Boise'),
]


def read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open('r', encoding='utf-8') as stream:
        result = yaml.safe_load(stream) or {}
    if not isinstance(result, dict):
        raise ValueError(f'Expected YAML mapping in {path}')
    return result


def connect() -> psycopg.Connection:
    cfg = read_yaml(CONFIG_FILE)
    pg = cfg.get('postgres', cfg)
    if not isinstance(pg, dict):
        raise ValueError('database.yaml must contain a postgres mapping')
    password = pg.get('password')
    if pg.get('password_env'):
        password = os.environ.get(str(pg['password_env']))
        if password is None:
            raise ValueError(f"Environment variable {pg['password_env']} is not set")
    args = dict(host=pg.get('host', 'localhost'), port=int(pg.get('port', 5432)),
                dbname=pg.get('database', pg.get('dbname')), user=pg.get('user'),
                password=password)
    return psycopg.connect(**{k: v for k, v in args.items() if v is not None})


def write_checkpoint(index: int, value: dict) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    path = CHECKPOINT_DIR / f'WH-{index:03d}.yaml'
    tmp = path.with_suffix('.yaml.tmp')
    with tmp.open('w', encoding='utf-8') as stream:
        yaml.safe_dump(value, stream, sort_keys=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def copy_rows(cur, table: str, columns: tuple[str, ...], rows: list[tuple]) -> None:
    if not rows:
        return
    statement = sql.SQL('COPY {} ({}) FROM STDIN').format(
        sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, columns)))
    with cur.copy(statement) as cp:
        for row in rows:
            cp.write_row(row)


def insert_new(cur, table: str, columns: tuple[str, ...], rows: list[tuple],
               conflict_cols: tuple[str, ...], batch_size: int) -> None:
    """COPY into connection-local staging, then idempotent INSERT into destination.

    Stage is created with the destination's column types, without its constraints.
    The unique conflict target ensures replay does not create duplicate records.
    """
    if not rows:
        return
    temp = f'stage_{table}'
    col_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
    target = sql.Identifier(SCHEMA, table)
    cur.execute(sql.SQL('CREATE TEMP TABLE IF NOT EXISTS {} ON COMMIT PRESERVE ROWS AS '
                        'SELECT {} FROM {} WITH NO DATA').format(
                            sql.Identifier(temp), col_sql, target))
    for start in range(0, len(rows), batch_size):
        part = rows[start:start + batch_size]
        cur.execute(sql.SQL('TRUNCATE {}').format(sql.Identifier(temp)))
        copy_rows(cur, temp, columns, part)
        cur.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {} '
                            'ON CONFLICT ({}) DO NOTHING').format(
            target, col_sql, col_sql, sql.Identifier(temp),
            sql.SQL(', ').join(map(sql.Identifier, conflict_cols))))
        cur.connection.commit()


def fetch_codes(cur, table: str, id_column: str, code_column: str,
                parent_column: str, parent_id: int) -> dict[str, int]:
    cur.execute(sql.SQL('SELECT {}, {} FROM {} WHERE {} = %s').format(
        sql.Identifier(code_column), sql.Identifier(id_column),
        sql.Identifier(SCHEMA, table), sql.Identifier(parent_column)), (parent_id,))
    return dict(cur.fetchall())


def create_warehouse(cur, index: int, seed: int) -> int:
    # Stable location mapping across reruns; existing rows are never overwritten.
    city, state, zip_prefix, tz = US_LOCATIONS[(index - 1) % len(US_LOCATIONS)]
    rng = random.Random(seed + index)
    code = f'WH-{index:03d}'
    warehouse_type = 'fulfillment'
    area = round(rng.uniform(15000, 90000), 2)
    opened = date(2010, 1, 1) + timedelta(days=rng.randint(0, 5500))
    cur.execute(f'''INSERT INTO {SCHEMA}.warehouses
        (warehouse_code, warehouse_name, warehouse_type, status,
         address_line1, city, region, postal_code, country_code,
         timezone, floor_area_sq_m, opened_on)
        VALUES (%s, %s, %s, 'active', %s, %s, %s, %s, 'US', %s, %s, %s)
        ON CONFLICT (warehouse_code) DO NOTHING''',
        (code, f'{city} Simulation Fulfillment Center {index:03d}', warehouse_type,
         f'{1000 + index} Simulation Logistics Way', city, state,
         f'{zip_prefix}{index % 100:02d}', tz, area, opened))
    cur.execute(f'SELECT warehouse_id FROM {SCHEMA}.warehouses WHERE warehouse_code = %s', (code,))
    return cur.fetchone()[0]


def process_warehouse(index: int, cfg: dict) -> None:
    batch_size = cfg['batch_size']
    with connect() as conn:
        # Ensure setup is committed before beginning the batch transactions.
        with conn.cursor() as cur:
            warehouse_id = create_warehouse(cur, index, cfg['seed'])
        conn.commit()

        # A hierarchy is built using IDs read back from the DB, never assumed
        # to be contiguous or to match sequence-generated values.
        with conn.cursor() as cur:
            zone_rows = []
            for z in range(1, cfg['zones_per_warehouse'] + 1):
                zone_type = 'storage'
                zone_rows.append((warehouse_id, f'Z{z:03d}', f'Storage Zone {z:03d}', zone_type, True))
            insert_new(cur, 'warehouse_zones',
                       ('warehouse_id', 'zone_code', 'zone_name', 'zone_type', 'is_active'),
                       zone_rows, ('warehouse_id', 'zone_code'), batch_size)
            zone_ids = fetch_codes(cur, 'warehouse_zones', 'zone_id', 'zone_code',
                                   'warehouse_id', warehouse_id)
            conn.commit()
            write_checkpoint(index, {'warehouse_code': f'WH-{index:03d}', 'phase': 'zones'})

            aisle_rows = []
            for z in range(1, cfg['zones_per_warehouse'] + 1):
                zone_id = zone_ids[f'Z{z:03d}']
                for a in range(1, cfg['aisles_per_zone'] + 1):
                    aisle_rows.append((warehouse_id, zone_id, f'A{a:03d}', True))
            insert_new(cur, 'storage_aisles',
                       ('warehouse_id', 'zone_id', 'aisle_code', 'is_active'),
                       aisle_rows, ('zone_id', 'aisle_code'), batch_size)
            cur.execute(f'''SELECT zone_id, aisle_code, aisle_id
                            FROM {SCHEMA}.storage_aisles WHERE warehouse_id = %s''', (warehouse_id,))
            aisle_ids = {(z, code): aid for z, code, aid in cur.fetchall()}
            conn.commit()
            write_checkpoint(index, {'warehouse_code': f'WH-{index:03d}', 'phase': 'aisles'})

            rack_rows = []
            for z in range(1, cfg['zones_per_warehouse'] + 1):
                zone_id = zone_ids[f'Z{z:03d}']
                for a in range(1, cfg['aisles_per_zone'] + 1):
                    aisle_id = aisle_ids[(zone_id, f'A{a:03d}')]
                    for r in range(1, cfg['racks_per_aisle'] + 1):
                        rack_rows.append((warehouse_id, zone_id, aisle_id,
                                          f'R{r:03d}', 5000, True))
            insert_new(cur, 'storage_racks',
                       ('warehouse_id', 'zone_id', 'aisle_id', 'rack_code',
                        'max_load_kg', 'is_active'), rack_rows,
                       ('aisle_id', 'rack_code'), batch_size)
            cur.execute(f'''SELECT aisle_id, rack_code, rack_id, zone_id
                            FROM {SCHEMA}.storage_racks WHERE warehouse_id = %s''', (warehouse_id,))
            rack_ids = {(aid, code): (rid, zid) for aid, code, rid, zid in cur.fetchall()}
            conn.commit()
            write_checkpoint(index, {'warehouse_code': f'WH-{index:03d}', 'phase': 'racks'})

            bin_rows = []
            for (aisle_id, rack_code), (rack_id, zone_id) in rack_ids.items():
                for b in range(1, cfg['bins_per_rack'] + 1):
                    bin_rows.append((warehouse_id, zone_id, aisle_id, rack_id,
                                     f'B{b:03d}', 1 + (b - 1) // 5, 'standard',
                                     100, 200, True, True))
                    if len(bin_rows) >= batch_size:
                        insert_new(cur, 'storage_bins',
                                   ('warehouse_id', 'zone_id', 'aisle_id', 'rack_id',
                                    'bin_code', 'level_number', 'bin_type',
                                    'capacity_units', 'max_load_kg', 'is_pickable', 'is_active'),
                                   bin_rows, ('rack_id', 'bin_code'), batch_size)
                        bin_rows.clear()
            if bin_rows:
                insert_new(cur, 'storage_bins',
                           ('warehouse_id', 'zone_id', 'aisle_id', 'rack_id',
                            'bin_code', 'level_number', 'bin_type',
                            'capacity_units', 'max_load_kg', 'is_pickable', 'is_active'),
                           bin_rows, ('rack_id', 'bin_code'), batch_size)
            conn.commit()
            write_checkpoint(index, {'warehouse_code': f'WH-{index:03d}', 'phase': 'completed'})
    LOG.info('Completed warehouse WH-%03d', index)


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s')
    raw = read_yaml(CONFIG_FILE).get('warehouse', {})
    if not isinstance(raw, dict):
        raise ValueError('warehouse configuration must be a YAML mapping')
    cfg = {key: int(raw.get(key, value)) for key, value in DEFAULTS.items()}
    if any(value < 1 for value in cfg.values()):
        raise ValueError('All warehouse configuration values must be positive integers')
    if cfg['warehouses'] > 999:
        raise ValueError('warehouses must be <= 999 (warehouse code format WH-###)')
    if cfg['bins_per_rack'] > 999 or cfg['racks_per_aisle'] > 999 or cfg['aisles_per_zone'] > 999 or cfg['zones_per_warehouse'] > 999:
        raise ValueError('Hierarchy dimensions must be <= 999 (code format ###)')
    expected = {
        'warehouses': cfg['warehouses'],
        'zones': cfg['warehouses'] * cfg['zones_per_warehouse'],
        'aisles': cfg['warehouses'] * cfg['zones_per_warehouse'] * cfg['aisles_per_zone'],
        'racks': cfg['warehouses'] * cfg['zones_per_warehouse'] * cfg['aisles_per_zone'] * cfg['racks_per_aisle'],
        'bins': cfg['warehouses'] * cfg['zones_per_warehouse'] * cfg['aisles_per_zone'] * cfg['racks_per_aisle'] * cfg['bins_per_rack'],
    }
    LOG.info('Target records: %s', expected)
    failures = []
    with ThreadPoolExecutor(max_workers=cfg['workers'], thread_name_prefix='warehouse') as pool:
        futures = {pool.submit(process_warehouse, index, cfg): index
                   for index in range(1, cfg['warehouses'] + 1)}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                LOG.exception('Failed warehouse WH-%03d', futures[future])
                failures.append(futures[future])
    if failures:
        raise RuntimeError(f'Failed warehouse IDs: {failures}; rerun to resume')
    LOG.info('Warehouse generation complete')


if __name__ == '__main__':
    main()
