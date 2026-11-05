"""Stream Amazon Reviews 2023 metadata into db_project_catalog.

Run from the project root through: python main.py catalog_load
Requires: pip install 'psycopg[binary]' PyYAML
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg
from psycopg import sql
import yaml

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "aws-reviews23-dataset" / "decompressed"
CONFIG_FILE = ROOT / "config" / "database.yaml"
CHECKPOINT_FILE = ROOT / "checkpoints" / "catalog.yaml"
SCHEMA = "db_project_catalog"
BATCH_SIZE = 500
LOG = logging.getLogger("catalog")


def read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        value = yaml.safe_load(f)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return value


def connect() -> psycopg.Connection:
    cfg = read_yaml(CONFIG_FILE)
    pg = cfg.get("postgres", cfg)
    if not isinstance(pg, dict):
        raise ValueError("database.yaml must contain a postgres mapping")
    password = pg.get("password")
    if pg.get("password_env"):
        password = os.environ.get(str(pg["password_env"]))
        if password is None:
            raise ValueError(f"Environment variable {pg['password_env']} is not set")
    kwargs = {
        "host": pg.get("host", "localhost"),
        "port": int(pg.get("port", 5432)),
        "dbname": pg.get("database", pg.get("dbname")),
        "user": pg.get("user"),
        "password": password,
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return psycopg.connect(**kwargs)


def checkpoint() -> tuple[str | None, int]:
    state = read_yaml(CHECKPOINT_FILE)
    name = state.get("file") or None
    line = state.get("line") or 0
    if not isinstance(line, int) or line < 0:
        raise ValueError("Checkpoint line must be a nonnegative integer")
    if name is not None and (not isinstance(name, str) or Path(name).name != name):
        raise ValueError("Checkpoint file must be a plain filename")
    return name, line


def save_checkpoint(filename: str, line: int) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = CHECKPOINT_FILE.with_suffix(".yaml.tmp")
    with temporary.open("w", encoding="utf-8") as f:
        yaml.safe_dump({"file": filename, "line": line}, f, sort_keys=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, CHECKPOINT_FILE)


def compact(value: object, limit: int | None = None) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = "\n".join(str(x) for x in value if x is not None)
    elif isinstance(value, dict):
        value = json.dumps(value, ensure_ascii=False)
    result = str(value).strip()
    return result[:limit] if limit else result


def digest(value: str, length: int = 20) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def slug(value: str, max_length: int = 180) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "item"
    suffix = digest(value, 12)
    return base[: max_length - len(suffix) - 1].rstrip("-") + "-" + suffix


def category_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    if stem.startswith("meta_"):
        stem = stem[5:]
    return stem.replace("_", " ").strip()


def get_or_create_category(cur: psycopg.Cursor, name: str) -> int:
    category_slug = slug(name)
    cur.execute(
        f"""INSERT INTO {SCHEMA}.product_categories (category_name, category_slug)
            VALUES (%s, %s)
            ON CONFLICT (category_slug) DO UPDATE
                SET category_name = EXCLUDED.category_name
            RETURNING category_id""",
        (name[:150], category_slug),
    )
    return cur.fetchone()[0]


def get_or_create_brand(cur: psycopg.Cursor, name: str) -> int | None:
    name = compact(name, 150)
    if not name:
        return None
    cur.execute(
        f"""INSERT INTO {SCHEMA}.brands (brand_name) VALUES (%s)
            ON CONFLICT DO NOTHING RETURNING brand_id""",
        (name,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        f"SELECT brand_id FROM {SCHEMA}.brands WHERE lower(brand_name) = lower(%s)",
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Could not resolve brand {name!r}")
    return row[0]


def get_attribute(cur: psycopg.Cursor, name: str) -> int:
    # Hash ensures distinct source keys remain distinct after truncation.
    code = "amz_" + digest(name, 32)
    cur.execute(
        f"""INSERT INTO {SCHEMA}.product_attributes
            (attribute_code, attribute_name, data_type, applies_to)
            VALUES (%s, %s, 'text', 'product')
            ON CONFLICT (attribute_code) DO UPDATE
                SET attribute_name = EXCLUDED.attribute_name
            RETURNING attribute_id""",
        (code, name[:150]),
    )
    return cur.fetchone()[0]


def parse_price(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        raw = str(value).strip().replace(",", "").replace("$", "")
        price = Decimal(raw)
        if not price.is_finite() or price < 0 or price >= Decimal("1000000000000000"):
            return None
        return price.quantize(Decimal("0.0001"))
    except (InvalidOperation, ValueError):
        return None


def ensure_price_list(cur: psycopg.Cursor) -> int:
    cur.execute(
        f"""INSERT INTO {SCHEMA}.price_lists
            (price_list_code, price_list_name, currency_code, sales_channel, country_code)
            VALUES ('AMZ_USD', 'Amazon metadata USD', 'USD', 'online', 'US')
            ON CONFLICT (price_list_code) DO UPDATE
                SET price_list_name = EXCLUDED.price_list_name
            RETURNING price_list_id"""
    )
    return cur.fetchone()[0]


def insert_product(cur: psycopg.Cursor, item: dict, category_id: int, price_list_id: int) -> bool:
    asin = compact(item.get("parent_asin"), 64)
    title = compact(item.get("title"), 255)
    if not asin or not title:
        return False

    # Stable across restarts, and short enough for product_code and SKU.
    code = "AMZ-" + digest(asin, 32)
    product_slug = slug("amazon-" + asin, 280)
    details = item.get("details") if isinstance(item.get("details"), dict) else {}
    brand_name = compact(details.get("Brand") or item.get("store"), 150)

    # Existing product means an already committed row. Skip to make replay safe.
    cur.execute(f"SELECT 1 FROM {SCHEMA}.products WHERE product_code = %s", (code,))
    if cur.fetchone():
        return False

    brand_id = get_or_create_brand(cur, brand_name)
    description = compact(item.get("description"))
    features = compact(item.get("features"))
    if features:
        description = description + ("\n\nFeatures:\n" if description else "Features:\n") + features

    cur.execute(
        f"""INSERT INTO {SCHEMA}.products
            (product_code, brand_id, product_name, product_slug, description, status)
            VALUES (%s, %s, %s, %s, %s, 'active') RETURNING product_id""",
        (code, brand_id, title, product_slug, description or None),
    )
    product_id = cur.fetchone()[0]

    sku = "SKU-" + digest(asin, 32)
    barcode = compact(details.get("UPC") or details.get("EAN"), 80) or None
    # UPCs/EANs can be reused or malformed in source data; avoid unique conflicts.
    # Retain source barcode as an attribute instead.
    cur.execute(
        f"""INSERT INTO {SCHEMA}.product_variants
            (product_id, sku, variant_name, status)
            VALUES (%s, %s, %s, 'active') RETURNING variant_id""",
        (product_id, sku, "Default"),
    )
    variant_id = cur.fetchone()[0]

    cur.execute(
        f"""INSERT INTO {SCHEMA}.product_category_assignments
            (product_id, category_id, is_primary) VALUES (%s, %s, TRUE)""",
        (product_id, category_id),
    )

    for key, value in details.items():
        name = compact(key)
        content = compact(value)
        if not name or not content:
            continue
        attribute_id = get_attribute(cur, name)
        cur.execute(
            f"""INSERT INTO {SCHEMA}.product_attribute_values
                (attribute_id, product_id, value_text) VALUES (%s, %s, %s)""",
            (attribute_id, product_id, content),
        )

    images = item.get("images")
    if isinstance(images, list):
        seen = set()
        for img in images:
            if not isinstance(img, dict):
                continue
            url = compact(img.get("large") or img.get("hi_res") or img.get("thumb"))
            if not url or url in seen:
                continue
            seen.add(url)
            is_primary = len(seen) == 1
            cur.execute(
                f"""INSERT INTO {SCHEMA}.product_images
                    (product_id, image_url, alt_text, sort_order, is_primary)
                    VALUES (%s, %s, %s, %s, %s)""",
                (product_id, url, title, len(seen) - 1, is_primary),
            )

    price = parse_price(item.get("price"))
    if price is not None:
        cur.execute(
            f"""INSERT INTO {SCHEMA}.product_prices
                (variant_id, price_list_id, amount, valid_from)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)""",
            (variant_id, price_list_id, price),
        )
    return True


def process_file(conn: psycopg.Connection, path: Path, start_line: int,
                 category_id: int, price_list_id: int, batch_size: int) -> None:
    last_committed = start_line
    count_inserted = 0
    # Streaming text iterator handles >1 GB files with bounded memory.
    with path.open("r", encoding="utf-8") as stream:
        for _ in range(start_line):
            if not stream.readline():
                raise ValueError(f"Checkpoint line {start_line} exceeds length of {path.name}")
        while True:
            lines_processed = 0
            inserted = 0
            with conn.transaction():
                with conn.cursor() as cur:
                    while lines_processed < batch_size:
                        raw = stream.readline()
                        if not raw:
                            break
                        current_line = last_committed + lines_processed + 1
                        lines_processed += 1
                        if not raw.strip():
                            continue
                        try:
                            obj = json.loads(raw)
                        except json.JSONDecodeError as exc:
                            LOG.warning("Skipping malformed JSON %s:%d: %s", path.name, current_line, exc)
                            continue
                        if not isinstance(obj, dict):
                            LOG.warning("Skipping non-object JSON %s:%d", path.name, current_line)
                            continue
                        # Fail on DB errors; the batch rolls back, leaving the checkpoint unchanged.
                        if insert_product(cur, obj, category_id, price_list_id):
                            inserted += 1
            if lines_processed == 0:
                break
            last_committed += lines_processed
            count_inserted += inserted
            # Write checkpoint ONLY after the corresponding DB transaction commits.
            save_checkpoint(path.name, last_committed)
            LOG.info("%s line %d (+%d products, total +%d)",
                     path.name, last_committed, inserted, count_inserted)
    LOG.info("Completed %s (last line %d)", path.name, last_committed)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    files = sorted(DATA_DIR.glob("*.jsonl"), key=lambda p: p.name)
    if not files:
        raise FileNotFoundError(f"No .jsonl files found in {DATA_DIR}")
    saved_file, saved_line = checkpoint()
    names = [f.name for f in files]
    if saved_file is not None and saved_file not in names:
        raise ValueError(f"Checkpoint references missing file {saved_file!r}; refusing to restart")
    # Pre-populate all filename-derived categories before reading product lines.
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                categories = {
                    f.name: get_or_create_category(cur, category_from_filename(f.name))
                    for f in files
                }
                price_list_id = ensure_price_list(cur)
        batch_size = int(read_yaml(CONFIG_FILE).get("loading", {}).get("batch_size", BATCH_SIZE))
        if batch_size < 1:
            raise ValueError("loading.batch_size must be >= 1")
        for path in files:
            if saved_file and path.name < saved_file:
                continue
            line = saved_line if path.name == saved_file else 0
            LOG.info("Processing %s from line %d", path.name, line + 1)
            process_file(conn, path, line, categories[path.name], price_list_id, batch_size)
    LOG.info("All catalog files processed")


if __name__ == "__main__":
    main()
