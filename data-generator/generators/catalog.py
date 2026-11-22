"""Parallel, resumable Amazon Reviews 2023 catalog loader (PostgreSQL 15+).

Replace your project's catalog.py with this file. Run: python main.py catalog_load
Requires: pip install 'psycopg[binary]' PyYAML

Existing checkpoints/catalog.yaml is read as a legacy starting point.
New checkpoints are written to checkpoints/catalog_files/<filename>.yaml.
Keep the legacy checkpoint file: it defines which earlier files were completed.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path

import psycopg
from psycopg import sql
import yaml

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / 'aws-reviews23-dataset' / 'decompressed'
CONFIG_FILE = ROOT / 'config' / 'database.yaml'
CHECKPOINT_FILE = ROOT / 'checkpoints' / 'catalog.yaml'
FILE_CHECKPOINT_DIR = ROOT / 'checkpoints' / 'catalog_files'
SCHEMA = 'db_project_catalog'
LOG = logging.getLogger('catalog')
DEFAULT_BATCH_SIZE = 5000
DEFAULT_WORKERS = 3


def read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f'Expected YAML mapping in {path}')
    return data


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


def atomic_yaml(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', encoding='utf-8') as f:
        yaml.safe_dump(value, f, sort_keys=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def compact(value: object, limit: int | None = None) -> str:
    if value is None:
        return ''
    if isinstance(value, list):
        value = '\n'.join(str(x) for x in value if x is not None)
    elif isinstance(value, dict):
        value = json.dumps(value, ensure_ascii=False)
    result = str(value).strip()
    return result[:limit] if limit else result


@lru_cache(maxsize=100000)
def digest(value: str, length: int = 20) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()[:length]


def slug(value: str, max_length: int = 180) -> str:
    base = re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-') or 'item'
    suffix = digest(value, 12)
    return base[:max_length - len(suffix) - 1].rstrip('-') + '-' + suffix


def category_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    if stem.startswith('meta_'):
        stem = stem[5:]
    return stem.replace('_', ' ').strip()


def parse_price(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        price = Decimal(str(value).strip().replace(',', '').replace('$', ''))
        if not price.is_finite() or price < 0 or price >= Decimal('1000000000000000'):
            return None
        return price.quantize(Decimal('0.0001'))
    except (InvalidOperation, ValueError):
        return None


def ensure_category(cur, name: str) -> int:
    category_slug = slug(name)
    cur.execute(f'''INSERT INTO {SCHEMA}.product_categories (category_name, category_slug)
                    VALUES (%s, %s) ON CONFLICT (category_slug)
                    DO UPDATE SET category_name = EXCLUDED.category_name
                    RETURNING category_id''', (name[:150], category_slug))
    return cur.fetchone()[0]


def ensure_price_list(cur) -> int:
    cur.execute(f'''INSERT INTO {SCHEMA}.price_lists
                 (price_list_code, price_list_name, currency_code, sales_channel, country_code)
                 VALUES ('AMZ_USD', 'Amazon metadata USD', 'USD', 'online', 'US')
                 ON CONFLICT (price_list_code) DO UPDATE
                 SET price_list_name = EXCLUDED.price_list_name
                 RETURNING price_list_id''')
    return cur.fetchone()[0]


def checkpoint_path(filename: str) -> Path:
    # Hex encoding avoids path traversal and filename collisions.
    return FILE_CHECKPOINT_DIR / (filename.encode('utf-8').hex() + '.yaml')


def load_position(path: Path, legacy_name: str | None, legacy_line: int):
    state = read_yaml(checkpoint_path(path.name))
    if state:
        if state.get('file') != path.name:
            raise ValueError(f'Checkpoint filename mismatch: {path.name}')
        if state.get('size') != path.stat().st_size:
            raise ValueError(f'Source file size changed: {path.name}; cannot safely resume')
        line = state.get('line')
        offset = state.get('offset')
        if not isinstance(line, int) or line < 0 or not isinstance(offset, int) or not 0 <= offset <= path.stat().st_size:
            raise ValueError(f'Invalid per-file checkpoint for {path.name}')
        return line, offset
    if legacy_name == path.name:
        return legacy_line, None
    return 0, 0


def prepare_temp_tables(cur):
    # Temp tables belong to the worker's connection; no global staging conflicts.
    cur.execute('''CREATE TEMP TABLE stage_products (
        code text PRIMARY KEY, brand text, title text NOT NULL, slug text NOT NULL,
        description text, sku text NOT NULL, price numeric
    ) ON COMMIT PRESERVE ROWS''')
    cur.execute('''CREATE TEMP TABLE stage_attributes (
        code text NOT NULL, attr_code text NOT NULL, attr_name text NOT NULL, attr_value text NOT NULL
    ) ON COMMIT PRESERVE ROWS''')
    cur.execute('''CREATE TEMP TABLE stage_images (
        code text NOT NULL, image_url text NOT NULL, alt_text text,
        sort_order integer NOT NULL, is_primary boolean NOT NULL
    ) ON COMMIT PRESERVE ROWS''')
    cur.execute('''CREATE TEMP TABLE new_products (
        code text PRIMARY KEY, product_id bigint NOT NULL
    ) ON COMMIT PRESERVE ROWS''')


def copy_rows(cur, table: str, columns: tuple[str, ...], rows: list[tuple]):
    if not rows:
        return
    query = sql.SQL('COPY {} ({}) FROM STDIN').format(
        sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, columns)))
    with cur.copy(query) as cp:
        for row in rows:
            cp.write_row(row)


def transform_batch(items: list[dict]):
    products, attrs, images = [], [], []
    seen_codes = set()
    for item in items:
        asin = compact(item.get('parent_asin'), 64)
        title = compact(item.get('title'), 255)
        if not asin or not title:
            continue
        code = 'AMZ-' + digest(asin, 32)
        if code in seen_codes:
            continue
        seen_codes.add(code)
        details = item.get('details') if isinstance(item.get('details'), dict) else {}
        brand = compact(details.get('Brand') or item.get('store'), 150)
        description = compact(item.get('description'))
        features = compact(item.get('features'))
        if features:
            description += ('\n\nFeatures:\n' if description else 'Features:\n') + features
        products.append((code, brand or None, title, slug('amazon-' + asin, 280),
                         description or None, 'SKU-' + digest(asin, 32), parse_price(item.get('price'))))
        for key, value in details.items():
            name, content = compact(key), compact(value)
            if name and content:
                attrs.append((code, 'amz_' + digest(name, 32), name[:150], content))
        raw_images = item.get('images')
        if isinstance(raw_images, list):
            seen_urls = set()
            for img in raw_images:
                if not isinstance(img, dict):
                    continue
                url = compact(img.get('large') or img.get('hi_res') or img.get('thumb'))
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    images.append((code, url, title, len(seen_urls) - 1, len(seen_urls) == 1))
    return products, attrs, images


def flush_batch(conn, items: list[dict], category_id: int, price_list_id: int) -> int:
    products, attrs, images = transform_batch(items)
    with conn.transaction():
        with conn.cursor() as cur:
            for table in ('stage_products', 'stage_attributes', 'stage_images', 'new_products'):
                cur.execute(sql.SQL('TRUNCATE {}').format(sql.Identifier(table)))
            copy_rows(cur, 'stage_products',
                      ('code', 'brand', 'title', 'slug', 'description', 'sku', 'price'), products)
            copy_rows(cur, 'stage_attributes', ('code', 'attr_code', 'attr_name', 'attr_value'), attrs)
            copy_rows(cur, 'stage_images', ('code', 'image_url', 'alt_text', 'sort_order', 'is_primary'), images)
            if not products:
                return 0

            # Distinct brand names use case-insensitive grouping, matching original resolver.
            # ON CONFLICT DO NOTHING respects the existing unique index/constraint.
            cur.execute(f'''INSERT INTO {SCHEMA}.brands (brand_name)
                            SELECT DISTINCT ON (lower(brand)) brand
                            FROM stage_products WHERE brand IS NOT NULL
                            ORDER BY lower(brand), brand
                            ON CONFLICT DO NOTHING''')

            # Only products newly inserted by this batch get child rows.
            cur.execute(f'''WITH inserted AS (
                INSERT INTO {SCHEMA}.products
                    (product_code, brand_id, product_name, product_slug, description, status)
                SELECT s.code, b.brand_id, s.title, s.slug, s.description, 'active'
                FROM stage_products s
                LEFT JOIN {SCHEMA}.brands b ON lower(b.brand_name) = lower(s.brand)
                ON CONFLICT (product_code) DO NOTHING
                RETURNING product_code, product_id
            ) INSERT INTO new_products (code, product_id)
              SELECT product_code, product_id FROM inserted''')

            cur.execute(f'''INSERT INTO {SCHEMA}.product_variants
                           (product_id, sku, variant_name, status)
                           SELECT n.product_id, s.sku, 'Default', 'active'
                           FROM new_products n JOIN stage_products s USING (code)''')
            cur.execute(f'''INSERT INTO {SCHEMA}.product_category_assignments
                           (product_id, category_id, is_primary)
                           SELECT product_id, %s, TRUE FROM new_products''', (category_id,))

            # Register only attributes referenced by newly inserted products.
            cur.execute(f'''INSERT INTO {SCHEMA}.product_attributes
                           (attribute_code, attribute_name, data_type, applies_to)
                           SELECT DISTINCT ON (a.attr_code) a.attr_code, a.attr_name, 'text', 'product'
                           FROM stage_attributes a JOIN new_products n USING (code)
                           ORDER BY a.attr_code, a.attr_name
                           ON CONFLICT (attribute_code) DO NOTHING''')
            cur.execute(f'''INSERT INTO {SCHEMA}.product_attribute_values
                           (attribute_id, product_id, value_text)
                           SELECT pa.attribute_id, n.product_id, a.attr_value
                           FROM stage_attributes a
                           JOIN new_products n USING (code)
                           JOIN {SCHEMA}.product_attributes pa ON pa.attribute_code = a.attr_code''')
            cur.execute(f'''INSERT INTO {SCHEMA}.product_images
                           (product_id, image_url, alt_text, sort_order, is_primary)
                           SELECT n.product_id, i.image_url, i.alt_text, i.sort_order, i.is_primary
                           FROM stage_images i JOIN new_products n USING (code)''')
            cur.execute(f'''INSERT INTO {SCHEMA}.product_prices
                           (variant_id, price_list_id, amount, valid_from)
                           SELECT v.variant_id, %s, s.price, CURRENT_TIMESTAMP
                           FROM new_products n
                           JOIN stage_products s USING (code)
                           JOIN {SCHEMA}.product_variants v ON v.product_id = n.product_id
                           WHERE s.price IS NOT NULL''', (price_list_id,))
            cur.execute('SELECT count(*) FROM new_products')
            return cur.fetchone()[0]


def process_file(path: Path, legacy_name: str | None, legacy_line: int,
                 category_id: int, price_list_id: int, batch_size: int) -> tuple[str, int]:
    line, offset = load_position(path, legacy_name, legacy_line)
    inserted_total = 0
    with connect() as conn:
        with conn.cursor() as cur:
            prepare_temp_tables(cur)
        conn.commit()  # Ensure each subsequent batch is a top-level committed transaction.
        # Binary file positioning makes offset stable and O(1) on resume.
        with path.open('rb') as stream:
            if offset is not None:
                stream.seek(offset)
            else:
                # Legacy checkpoint is line-based, so skip once to migrate.
                for _ in range(line):
                    if not stream.readline():
                        raise ValueError(f'Legacy checkpoint line exceeds {path.name}')
            while True:
                items = []
                lines_read = 0
                while lines_read < batch_size:
                    raw = stream.readline()
                    if not raw:
                        break
                    lines_read += 1
                    if not raw.strip():
                        continue
                    try:
                        obj = json.loads(raw)
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        LOG.warning('Skipping malformed JSON %s:%d: %s', path.name, line + lines_read, exc)
                        continue
                    if isinstance(obj, dict):
                        items.append(obj)
                    else:
                        LOG.warning('Skipping non-object JSON %s:%d', path.name, line + lines_read)
                if lines_read == 0:
                    break
                next_offset = stream.tell()
                count = flush_batch(conn, items, category_id, price_list_id)
                # Save only AFTER successful DB commit; replay is safe via product_code uniqueness.
                line += lines_read
                inserted_total += count
                atomic_yaml(checkpoint_path(path.name), dict(
                    file=path.name, line=line, offset=next_offset, size=path.stat().st_size,
                    completed=next_offset == path.stat().st_size))
                LOG.info('%s: line %d, inserted +%d (worker total %d)',
                         path.name, line, count, inserted_total)
    LOG.info('Completed %s: %d new products', path.name, inserted_total)
    return path.name, inserted_total


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s')
    files = sorted(DATA_DIR.glob('*.jsonl'), key=lambda p: p.name)
    if not files:
        raise FileNotFoundError(f'No .jsonl files found in {DATA_DIR}')
    legacy = read_yaml(CHECKPOINT_FILE)
    legacy_name = legacy.get('file') or None
    legacy_line = legacy.get('line') or 0
    if not isinstance(legacy_line, int) or legacy_line < 0:
        raise ValueError('Legacy checkpoint line must be a nonnegative integer')
    if legacy_name is not None and legacy_name not in {p.name for p in files}:
        raise ValueError(f'Legacy checkpoint references missing file: {legacy_name}')
    cfg = read_yaml(CONFIG_FILE).get('loading', {})
    batch_size = int(cfg.get('batch_size', DEFAULT_BATCH_SIZE))
    workers = int(cfg.get('workers', DEFAULT_WORKERS))
    if batch_size < 1 or workers < 1:
        raise ValueError('loading.batch_size and loading.workers must be >= 1')

    # This initial setup is intentionally serial, avoiding duplicate category/price-list races.
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                categories = {p.name: ensure_category(cur, category_from_filename(p.name)) for p in files}
                price_list_id = ensure_price_list(cur)

    pending = []
    for path in files:
        # Legacy checkpoint means all alphabetically earlier files were finished.
        if legacy_name and path.name < legacy_name:
            continue
        line, offset = load_position(path, legacy_name, legacy_line)
        if offset is not None and offset == path.stat().st_size:
            LOG.info('Skipping already completed %s', path.name)
            continue
        pending.append(path)
    LOG.info('Processing %d files with %d workers, batch_size=%d', len(pending), workers, batch_size)
    failures = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='catalog') as pool:
        futures = {pool.submit(process_file, p, legacy_name, legacy_line,
                               categories[p.name], price_list_id, batch_size): p for p in pending}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                LOG.exception('Failed processing %s', futures[future].name)
                failures.append(futures[future].name)
    if failures:
        raise RuntimeError('Failed catalog files (rerun to resume): ' + ', '.join(failures))
    LOG.info('All catalog files processed')


if __name__ == '__main__':
    main()
