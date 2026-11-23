#!/usr/bin/env python3
"""Concurrent PostgreSQL checkout TPS benchmark (PostgreSQL 15+, psycopg 3).

Creates all 10 order tables under db_experiment_latency_schema, including copied indexes,
checks, identity columns, and recreated foreign keys. No rows go to the original
orders schema. Uses REAL inventory_balances rows and actual customer/variant IDs.

Selected real inventory rows are temporarily given synthetic benchmark capacity.
A durable snapshot plus an in-transaction pick ledger allow exact restoration.
If a run crashes, run this script again with --recover before starting a new run.

IMPORTANT: Run only on a disposable/test copy of the database without competing
inventory writers. This script updates selected live inventory rows.
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import re
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psycopg
import yaml
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_CONFIG = ROOT / "data-generator" / "config" / "database.yaml"
DEFAULT_OUT = ROOT / "experiments"
SCHEMA = "db_experiment_latency_schema"
SOURCE_SCHEMA = "db_project_orders"
MAGIC = "warehouse-checkout-latency-v1"
LOCK_KEY_A, LOCK_KEY_B = 809331, 20261123
CLIENTS = (1, 5, 10, 20, 50, 100)
ISOLATIONS = ("READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE")
ORDER_TABLES = (
    "shopping_carts", "cart_items", "orders", "order_items",
    "order_addresses", "order_status_history", "order_allocations",
    "order_cancellations", "order_adjustments", "checkout_attempts",
)
MONEY = Decimal("0.01")


@dataclass(frozen=True)
class InventoryItem:
    balance_id: int
    warehouse_id: int
    variant_id: int
    sku: str
    name: str


@dataclass(frozen=True)
class Line:
    item: InventoryItem
    qty: int
    price: Decimal
    tax: Decimal


@dataclass
class WorkerMetrics:
    committed: int = 0
    failed: int = 0
    retries: int = 0
    serialization_failures: int = 0
    deadlocks: int = 0
    stockouts: int = 0
    latencies_ms: list[float] = field(default_factory=list)


class OutOfStock(Exception):
    pass


class SafetyError(Exception):
    pass


def load_connection_kwargs(path: Path) -> dict:
    """Supports DATABASE_URL, libpq environment, and common database.yaml layouts."""
    # Always use the requested YAML file rather than silently falling back to
    # an unrelated DATABASE_URL / libpq default database.
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(
            f"Database config not found: {path}. "
            "Expected <project-root>/data-generator/config/database.yaml; "
            "use --db-config PATH to specify a different file."
        )
    print(f"Reading PostgreSQL configuration from: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    cfg = raw
    for key in ("postgresql", "postgres", "database", "connection", "db"):
        if isinstance(cfg.get(key), dict):
            cfg = cfg[key]
            break
    kwargs = {}
    for source_key, target_key in (
        ("host", "host"), ("port", "port"), ("user", "user"),
        ("dbname", "dbname"), ("database", "dbname"), ("name", "dbname"),
        ("password", "password"), ("sslmode", "sslmode"),
    ):
        if cfg.get(source_key) is not None and target_key not in kwargs:
            kwargs[target_key] = cfg[source_key]
    password_env = cfg.get("password_env") or cfg.get("password_env_var")
    if password_env and os.environ.get(password_env):
        kwargs["password"] = os.environ[password_env]
    elif not kwargs.get("password") and os.environ.get("PGPASSWORD"):
        kwargs["password"] = os.environ["PGPASSWORD"]
    # Allow ${VARNAME} in YAML values, but never print credentials.
    for key, val in list(kwargs.items()):
        if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
            var_name = val[2:-1]
            if var_name not in os.environ:
                raise ValueError(f"Missing environment variable {var_name} referenced by {path}")
            kwargs[key] = os.environ[var_name]
    if not kwargs.get("dbname"):
        raise ValueError(
            f"No database name found in {path}. Expected database/dbname/name "
            "under the top level or a postgres/postgresql/database/connection/db section."
        )
    return kwargs


def connect(db_kwargs: dict) -> psycopg.Connection:
    kwargs = dict(db_kwargs)
    conninfo = kwargs.pop("conninfo", "")
    return psycopg.connect(conninfo, autocommit=True, connect_timeout=15, **kwargs)


def relation(schema: str, table: str):
    return sql.Identifier(schema, table)


def schema_exists(conn) -> bool:
    return bool(conn.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)",
        (SCHEMA,),
    ).fetchone()[0])


def require_owned_schema(conn) -> str:
    if not schema_exists(conn):
        return "missing"
    if conn.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.run_meta",)).fetchone()[0] is None:
        raise SafetyError(
            f"Schema {SCHEMA} already exists, but has no run_meta. "
            "Will not alter or drop a schema this script does not own."
        )
    marker, state = conn.execute(
        f"SELECT marker, state FROM {SCHEMA}.run_meta WHERE id = 1"
    ).fetchone()
    if marker != MAGIC or state not in ("ready", "seeded"):
        raise SafetyError(f"Unrecognized {SCHEMA} metadata; refusing cleanup")
    return state


def validate_connection_slots(conn, max_clients: int) -> None:
    max_connections, reserved, active = conn.execute("""
        SELECT current_setting('max_connections')::int,
               current_setting('superuser_reserved_connections')::int,
               (SELECT COUNT(*)::int FROM pg_stat_activity
                WHERE backend_type = 'client backend')
    """).fetchone()
    available = max_connections - reserved - active
    print(f"Server connection slots: max={max_connections}, reserved={reserved}, "
          f"currently used={active}, available~={available}")
    if available < max_clients:
        raise SafetyError(
            f"Need {max_clients} additional database connections, but only about "
            f"{available} ordinary slots are free. Increase PostgreSQL "
            "max_connections (for example to 150 or 175), restart it, and "
            "close other idle clients; or rerun with --clients 1,5,10,20,50."
        )


def get_customers(conn, limit: int) -> list[int]:
    rows = conn.execute("""
        SELECT customer_id FROM db_project_identity.customers
        TABLESAMPLE SYSTEM (2.0) LIMIT %s
    """, (limit,)).fetchall()
    if len(rows) < min(limit, 100):
        rows = conn.execute("""
            SELECT customer_id FROM db_project_identity.customers
            ORDER BY customer_id LIMIT %s
        """, (limit,)).fetchall()
    ids = [int(row[0]) for row in rows]
    if not ids:
        raise RuntimeError("No customer IDs found in db_project_identity.customers")
    return ids


def fetch_inventory_candidates(conn, pool_size: int, min_variant: int,
                               max_variant: int) -> list[InventoryItem]:
    """Fetch real stocked balances first; never assume continuous variant IDs."""
    query_sample = """
        SELECT b.balance_id, b.warehouse_id, b.variant_id,
               v.sku, COALESCE(NULLIF(v.variant_name, ''), v.sku)
        FROM (
            SELECT balance_id, warehouse_id, variant_id
            FROM db_project_inventory.inventory_balances
            TABLESAMPLE SYSTEM (0.8)
            WHERE quantity_on_hand > quantity_reserved
              AND variant_id BETWEEN %s AND %s
            LIMIT %s
        ) AS b
        JOIN db_project_catalog.product_variants AS v USING (variant_id)
    """
    rows = conn.execute(query_sample, (min_variant, max_variant,
                                       pool_size * 3)).fetchall()
    if len(rows) < pool_size:
        print("TABLESAMPLE returned too few balances; using indexed fallback...")
        rows = conn.execute("""
            SELECT b.balance_id, b.warehouse_id, b.variant_id,
                   v.sku, COALESCE(NULLIF(v.variant_name, ''), v.sku)
            FROM db_project_inventory.inventory_balances AS b
            JOIN db_project_catalog.product_variants AS v USING (variant_id)
            WHERE b.quantity_on_hand > b.quantity_reserved
              AND b.variant_id BETWEEN %s AND %s
            ORDER BY b.balance_id
            LIMIT %s
        """, (min_variant, max_variant, pool_size * 3)).fetchall()

    # One balance per distinct variant: avoids duplicate variant lines in a cart.
    seen_variants: set[int] = set()
    items: list[InventoryItem] = []
    for balance_id, warehouse_id, variant_id, sku, name in rows:
        if variant_id in seen_variants:
            continue
        seen_variants.add(int(variant_id))
        items.append(InventoryItem(int(balance_id), int(warehouse_id),
                                   int(variant_id), sku, name))
        if len(items) >= pool_size:
            break
    if len(items) < 2:
        raise RuntimeError("Need at least two distinct stocked inventory variants")
    random.Random(381).shuffle(items)
    return items


def create_schema_and_snapshot(conn, items: list[InventoryItem],
                               stock_floor: int) -> None:
    # One commit: durable baseline exists BEFORE ANY inventory row is changed.
    with conn.transaction():
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(SCHEMA)))
        for table in ORDER_TABLES:
            conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
                relation(SCHEMA, table), relation(SOURCE_SCHEMA, table)))

        # LIKE INCLUDING ALL does not clone foreign-key constraints. Restore them,
        # repointing references to source order tables at our cloned order tables.
        conn.execute("SET LOCAL search_path = pg_catalog, public")
        source_fks = conn.execute("""
            SELECT tbl.relname, c.conname, pg_get_constraintdef(c.oid, true)
            FROM pg_constraint AS c
            JOIN pg_class AS tbl ON tbl.oid = c.conrelid
            JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
            WHERE ns.nspname = %s AND c.contype = 'f'
              AND tbl.relname = ANY(%s)
            ORDER BY tbl.relname, c.conname
        """, (SOURCE_SCHEMA, list(ORDER_TABLES))).fetchall()
        for table, constraint_name, definition in source_fks:
            cloned_def = re.sub(
                r'(?i)(\bREFERENCES\s+)(?:"?db_project_orders"?\.)',
                r"\1db_experiment_latency_schema.", definition,
            )
            conn.execute(sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {}").format(
                relation(SCHEMA, table), sql.Identifier(constraint_name),
                sql.SQL(cloned_def)))

        conn.execute(f"""
            CREATE TABLE {SCHEMA}.run_meta (
                id int PRIMARY KEY CHECK (id = 1),
                marker text NOT NULL,
                state text NOT NULL CHECK (state IN ('ready','seeded'))
            )
        """)
        conn.execute(f"INSERT INTO {SCHEMA}.run_meta VALUES (1, %s, 'ready')",
                     (MAGIC,))
        conn.execute(f"""
            CREATE TABLE {SCHEMA}.inventory_baseline (
                balance_id bigint PRIMARY KEY,
                warehouse_id bigint NOT NULL,
                variant_id bigint NOT NULL,
                quantity_on_hand bigint NOT NULL,
                quantity_reserved bigint NOT NULL,
                original_updated_at timestamptz NOT NULL,
                seed_on_hand bigint NOT NULL
            )
        """)
        conn.execute(f"""
            CREATE TABLE {SCHEMA}.benchmark_picks (
                pick_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                order_id bigint NOT NULL REFERENCES {SCHEMA}.orders(order_id)
                    ON DELETE RESTRICT,
                balance_id bigint NOT NULL REFERENCES
                    {SCHEMA}.inventory_baseline(balance_id) ON DELETE RESTRICT,
                quantity int NOT NULL CHECK (quantity > 0)
            )
        """)
        conn.execute(f"CREATE INDEX ON {SCHEMA}.benchmark_picks (balance_id)")

        ids = [item.balance_id for item in items]
        conn.execute(f"""
            INSERT INTO {SCHEMA}.inventory_baseline
                (balance_id, warehouse_id, variant_id, quantity_on_hand,
                 quantity_reserved, original_updated_at, seed_on_hand)
            SELECT balance_id, warehouse_id, variant_id, quantity_on_hand,
                   quantity_reserved, updated_at,
                   GREATEST(quantity_on_hand, quantity_reserved + %s)
            FROM db_project_inventory.inventory_balances
            WHERE balance_id = ANY(%s)
        """, (stock_floor, ids))
        saved = conn.execute(
            f"SELECT COUNT(*) FROM {SCHEMA}.inventory_baseline"
        ).fetchone()[0]
        if saved != len(items):
            raise SafetyError("Balance IDs changed while taking the snapshot")


def write_inventory_snapshot(conn, path: Path) -> None:
    rows = conn.execute(f"""
        SELECT balance_id, variant_id, warehouse_id,
               quantity_on_hand, quantity_reserved,
               quantity_on_hand - quantity_reserved,
               seed_on_hand, original_updated_at
        FROM {SCHEMA}.inventory_baseline ORDER BY balance_id
    """).fetchall()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(("balance_id", "variant_id", "warehouse_id",
                         "original_on_hand", "original_reserved", "original_available",
                         "temporary_seed_on_hand", "original_updated_at"))
        writer.writerows(rows)
    avail = [row[5] for row in rows]
    print(f"Snapshot saved for {len(rows)} real inventory balances: "
          f"original available min={min(avail)}, max={max(avail)}")
    print(f"Inventory baseline CSV: {path}")


def assert_original_balances(conn) -> None:
    mismatches = conn.execute(f"""
        SELECT s.balance_id, b.quantity_on_hand, s.quantity_on_hand,
               b.quantity_reserved, s.quantity_reserved
        FROM {SCHEMA}.inventory_baseline AS s
        JOIN db_project_inventory.inventory_balances AS b USING (balance_id)
        WHERE b.quantity_on_hand IS DISTINCT FROM s.quantity_on_hand
           OR b.quantity_reserved IS DISTINCT FROM s.quantity_reserved
        LIMIT 5
    """).fetchall()
    if mismatches:
        raise SafetyError(f"Inventory changed outside benchmark: {mismatches!r}. "
                          "Do not reset someone else's inventory changes.")


def seed_for_scenario(conn) -> None:
    with conn.transaction():
        state = conn.execute(
            f"SELECT state FROM {SCHEMA}.run_meta WHERE id = 1 FOR UPDATE"
        ).fetchone()[0]
        if state != "ready":
            raise SafetyError(f"Cannot seed inventory from state {state}")
        # Lock every selected physical balance before checking and seeding.
        conn.execute(f"""
            SELECT b.balance_id
            FROM db_project_inventory.inventory_balances AS b
            JOIN {SCHEMA}.inventory_baseline AS s USING (balance_id)
            ORDER BY b.balance_id FOR UPDATE OF b
        """).fetchall()
        assert_original_balances(conn)
        conn.execute(f"""
            UPDATE db_project_inventory.inventory_balances AS b
            SET quantity_on_hand = s.seed_on_hand,
                updated_at = clock_timestamp()
            FROM {SCHEMA}.inventory_baseline AS s
            WHERE b.balance_id = s.balance_id
        """)
        conn.execute(f"UPDATE {SCHEMA}.run_meta SET state='seeded' WHERE id=1")


def restore_scenario(conn) -> None:
    """Atomic check, restore, and removal of all benchmark transaction rows.

    If inventory differs from our seed minus recorded purchases, ABORT restoration
    instead of silently overwriting another workload's changes. The snapshot and
    ledger remain in DB for diagnosis or manual recovery.
    """
    with conn.transaction():
        state = conn.execute(
            f"SELECT state FROM {SCHEMA}.run_meta WHERE id = 1 FOR UPDATE"
        ).fetchone()[0]
        if state == "ready":
            return
        if state != "seeded":
            raise SafetyError(f"Invalid run state: {state}")
        # Lock before validation: prevents concurrent edits between the check
        # and the compensating restore statement.
        conn.execute(f"""
            SELECT b.balance_id
            FROM db_project_inventory.inventory_balances AS b
            JOIN {SCHEMA}.inventory_baseline AS s USING (balance_id)
            ORDER BY b.balance_id FOR UPDATE OF b
        """).fetchall()
        mismatches = conn.execute(f"""
            WITH picked AS (
                SELECT balance_id, SUM(quantity)::bigint AS units
                FROM {SCHEMA}.benchmark_picks GROUP BY balance_id
            )
            SELECT s.balance_id, b.quantity_on_hand,
                   s.seed_on_hand - COALESCE(p.units, 0) AS expected_on_hand,
                   b.quantity_reserved, s.quantity_reserved AS expected_reserved
            FROM {SCHEMA}.inventory_baseline AS s
            LEFT JOIN picked AS p USING (balance_id)
            LEFT JOIN db_project_inventory.inventory_balances AS b USING (balance_id)
            WHERE b.quantity_on_hand IS DISTINCT FROM
                     (s.seed_on_hand - COALESCE(p.units, 0))
               OR b.quantity_reserved IS DISTINCT FROM s.quantity_reserved
            LIMIT 10
        """).fetchall()
        if mismatches:
            raise SafetyError(
                "Inventory no longer matches our durable pick ledger. "
                f"Refusing unsafe restoration. Examples: {mismatches!r}. "
                f"The backup remains in {SCHEMA}.inventory_baseline; "
                "inspect external changes before trying --recover."
            )
        conn.execute(f"""
            UPDATE db_project_inventory.inventory_balances AS b
            SET quantity_on_hand = s.quantity_on_hand,
                quantity_reserved = s.quantity_reserved,
                updated_at = s.original_updated_at
            FROM {SCHEMA}.inventory_baseline AS s
            WHERE b.balance_id = s.balance_id
        """)
        all_tables = ", ".join(f"{SCHEMA}.{name}" for name in
                               (*ORDER_TABLES, "benchmark_picks"))
        conn.execute(f"TRUNCATE TABLE {all_tables} RESTART IDENTITY")
        conn.execute(f"UPDATE {SCHEMA}.run_meta SET state='ready' WHERE id=1")
        assert_original_balances(conn)


def recover_and_drop(conn) -> None:
    state = require_owned_schema(conn)
    if state == "missing":
        print("No experiment schema to recover")
        return
    if state == "seeded":
        print("Recovering selected real inventory balances from durable snapshot/ledger...")
        restore_scenario(conn)
    # Refuse to drop a schema with an unrecognized marker (checked above).
    conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(SCHEMA)))
    print(f"Restoration verified; {SCHEMA} dropped (all experiment orders removed)")


def price_for_variant(variant_id: int) -> Decimal:
    # Synthetic deterministic unit-price snapshot; original price tables untouched.
    return (Decimal(499 + variant_id % 16000) / 100).quantize(MONEY)


def choose_checkout(rng: random.Random, customers: list[int],
                    items: list[InventoryItem], hot_items: list[InventoryItem],
                    items_per_order: int, hot_probability: float):
    chosen: list[InventoryItem] = []
    seen: set[int] = set()
    while len(chosen) < items_per_order:
        candidate = rng.choice(hot_items if rng.random() < hot_probability else items)
        if candidate.variant_id in seen:
            continue
        seen.add(candidate.variant_id)
        chosen.append(candidate)
    lines = []
    for item in chosen:
        qty = rng.randint(1, 2)
        price = price_for_variant(item.variant_id)
        tax = (price * qty * Decimal("0.07")).quantize(MONEY,
                                                        rounding=ROUND_HALF_UP)
        lines.append(Line(item, qty, price, tax))
    return rng.choice(customers), lines, "EXP" + uuid.uuid4().hex


def checkout_once(conn, isolation: str, customer_id: int,
                  lines: list[Line], order_number: str) -> None:
    """One atomic warehouse pick and checkout. Commit on exiting the context."""
    with conn.transaction():
        # Whitelisted SQL literals, not user-supplied SQL.
        conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}")

        balance_ids = [line.item.balance_id for line in lines]
        availability = dict(conn.execute("""
            SELECT balance_id, quantity_available
            FROM db_project_inventory.inventory_balances
            WHERE balance_id = ANY(%s)
        """, (balance_ids,)).fetchall())
        if any(availability.get(line.item.balance_id, -1) < line.qty
               for line in lines):
            raise OutOfStock("Stock no longer available at read time")

        # Acquire inventory locks in stable order to reduce avoidable deadlocks.
        for line in sorted(lines, key=lambda x: x.item.balance_id):
            updated = conn.execute("""
                UPDATE db_project_inventory.inventory_balances
                SET quantity_on_hand = quantity_on_hand - %s,
                    updated_at = clock_timestamp()
                WHERE balance_id = %s
                  AND quantity_on_hand - quantity_reserved >= %s
                RETURNING balance_id
            """, (line.qty, line.item.balance_id, line.qty)).fetchone()
            if updated is None:
                raise OutOfStock("Stock changed before guarded UPDATE")

        cart_id = conn.execute(f"""
            INSERT INTO {SCHEMA}.shopping_carts(customer_id, status)
            VALUES (%s, 'converted') RETURNING cart_id
        """, (customer_id,)).fetchone()[0]
        for line in lines:
            conn.execute(f"""
                INSERT INTO {SCHEMA}.cart_items(cart_id, variant_id, quantity)
                VALUES (%s, %s, %s)
            """, (cart_id, line.item.variant_id, line.qty))

        subtotal = sum((line.price * line.qty for line in lines), Decimal("0"))
        tax_total = sum((line.tax for line in lines), Decimal("0"))
        order_id = conn.execute(f"""
            INSERT INTO {SCHEMA}.orders
                (order_number, customer_id, cart_id, status,
                 subtotal, discount_total, tax_total, shipping_total)
            VALUES (%s, %s, %s, 'processing', %s, 0, %s, 4.99)
            RETURNING order_id
        """, (order_number, customer_id, cart_id, subtotal, tax_total)).fetchone()[0]

        for line_number, line in enumerate(lines, start=1):
            order_item_id = conn.execute(f"""
                INSERT INTO {SCHEMA}.order_items
                    (order_id, line_number, variant_id, sku_snapshot,
                     product_name_snapshot, quantity, unit_price, tax_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING order_item_id
            """, (order_id, line_number, line.item.variant_id, line.item.sku,
                  line.item.name, line.qty, line.price, line.tax)).fetchone()[0]
            conn.execute(f"""
                INSERT INTO {SCHEMA}.order_allocations
                    (order_id, order_item_id, warehouse_id,
                     allocated_quantity, status)
                VALUES (%s, %s, %s, %s, 'fulfilled')
            """, (order_id, order_item_id, line.item.warehouse_id, line.qty))
            # Durable journal: same transaction as stock decrement and order.
            conn.execute(f"""
                INSERT INTO {SCHEMA}.benchmark_picks
                    (order_id, balance_id, quantity)
                VALUES (%s, %s, %s)
            """, (order_id, line.item.balance_id, line.qty))

        conn.execute(f"""
            INSERT INTO {SCHEMA}.order_addresses
                (order_id, address_type, recipient_name, address_line1,
                 city, region, postal_code, country_code)
            VALUES (%s, 'shipping', %s, '100 Benchmark Lane',
                    'Fargo', 'ND', '58102', 'US')
        """, (order_id, f"Synthetic Customer {customer_id}"))
        conn.execute(f"""
            INSERT INTO {SCHEMA}.order_status_history
                (order_id, old_status, new_status, change_reason)
            VALUES (%s, NULL, 'processing', 'Benchmark warehouse pick')
        """, (order_id,))
        conn.execute(f"""
            INSERT INTO {SCHEMA}.checkout_attempts
                (customer_id, cart_id, idempotency_key, order_id,
                 status, completed_at)
            VALUES (%s, %s, %s, %s, 'succeeded', clock_timestamp())
        """, (customer_id, cart_id, order_number, order_id))


def run_one_checkout(conn, isolation: str, customer_id: int,
                     lines: list[Line], order_number: str,
                     rng: random.Random, max_retries: int):
    """Returns (committed, retry_count, 40001_count, 40P01_count, stockout)."""
    retries = serializations = deadlocks = 0
    for attempt in range(max_retries + 1):
        try:
            checkout_once(conn, isolation, customer_id, lines, order_number)
            return True, retries, serializations, deadlocks, False
        except OutOfStock:
            return False, retries, serializations, deadlocks, True
        except psycopg.Error as exc:
            if exc.sqlstate not in ("40001", "40P01"):
                raise  # Do not mask SQL/schema/configuration errors.
            serializations += exc.sqlstate == "40001"
            deadlocks += exc.sqlstate == "40P01"
            if attempt == max_retries:
                return False, retries, serializations, deadlocks, False
            retries += 1
            time.sleep(rng.uniform(0.0005, min(0.08, 0.001 * (2 ** min(attempt, 7)))))
    raise AssertionError("unreachable")


def worker(worker_index: int, client_count: int, isolation: str,
           db_kwargs: dict, barrier: threading.Barrier,
           start_event: threading.Event, times: dict,
           customers: list[int], items: list[InventoryItem],
           hot_items: list[InventoryItem], items_per_order: int,
           hot_probability: float, max_retries: int, seed: int) -> WorkerMetrics:
    metrics = WorkerMetrics()
    rng = random.Random(seed + client_count * 100003 + worker_index * 7919)
    try:
        conn = connect(db_kwargs)
    except BaseException:
        barrier.abort()
        raise
    try:
        # Avoid connection setup time contaminating TPS.
        barrier.wait(timeout=120)
        if not start_event.wait(timeout=120):
            raise TimeoutError("benchmark start event not received")
        measure_start, end_at = times["measure_start"], times["end_at"]
        while time.perf_counter() < end_at:
            customer_id, lines, order_number = choose_checkout(
                rng, customers, items, hot_items,
                items_per_order, hot_probability)
            start_txn = time.perf_counter()
            committed, retried, serialization, deadlocks, stockout = run_one_checkout(
                conn, isolation, customer_id, lines, order_number,
                rng, max_retries)
            done_at = time.perf_counter()
            # Count completed CHECKOUTS (not SQL statements or retry attempts).
            if measure_start <= done_at <= end_at:
                metrics.committed += int(committed)
                metrics.failed += int(not committed)
                metrics.retries += retried
                metrics.serialization_failures += serialization
                metrics.deadlocks += deadlocks
                metrics.stockouts += int(stockout)
                if committed:
                    metrics.latencies_ms.append((done_at - start_txn) * 1000)
    finally:
        conn.close()
    return metrics


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    index = (len(data) - 1) * fraction
    lo = int(index)
    hi = min(lo + 1, len(data) - 1)
    return data[lo] + (data[hi] - data[lo]) * (index - lo)


def measure_scenario(args, isolation: str, clients: int,
                     db_kwargs: dict, customers: list[int],
                     items: list[InventoryItem]) -> dict:
    hot_items = items[:min(args.hot_pool_size, len(items))]
    barrier = threading.Barrier(clients + 1)
    start_event = threading.Event()
    times: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=clients,
                            thread_name_prefix="checkout") as executor:
        futures = [executor.submit(
            worker, i, clients, isolation, db_kwargs, barrier,
            start_event, times, customers, items, hot_items,
            args.items_per_order, args.hot_probability, args.max_retries,
            args.seed,
        ) for i in range(clients)]
        try:
            barrier.wait(timeout=120)
        except threading.BrokenBarrierError as exc:
            # Cause is likely a connection error in one of the workers.
            failures = [f.exception() for f in futures if f.done() and f.exception()]
            raise RuntimeError(f"Unable to start {clients} clients: {failures}") from exc
        start = time.perf_counter()
        times["measure_start"] = start + args.warmup
        times["end_at"] = times["measure_start"] + args.duration
        start_event.set()
        results = [future.result() for future in futures]

    committed = sum(m.committed for m in results)
    failed = sum(m.failed for m in results)
    retries = sum(m.retries for m in results)
    serialization_failures = sum(m.serialization_failures for m in results)
    deadlocks = sum(m.deadlocks for m in results)
    stockouts = sum(m.stockouts for m in results)
    latencies = [ms for m in results for ms in m.latencies_ms]
    return {
        "isolation": isolation.title(),
        "clients": clients,
        "duration_s": args.duration,
        "warmup_s": args.warmup,
        "tps": round(committed / args.duration, 3),
        "committed": committed,
        "failed": failed,
        "retries": retries,
        "serialization_failures": serialization_failures,
        "deadlocks": deadlocks,
        "stockouts": stockouts,
        "avg_latency_ms": round(statistics.mean(latencies), 3) if latencies else 0,
        "p95_latency_ms": round(percentile(latencies, 0.95), 3),
    }


def plot_results(rows: list[dict], path: Path, clients: tuple[int, ...]) -> None:
    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    for iso in ISOLATIONS:
        series = sorted((r for r in rows if r["isolation"] == iso.title()),
                        key=lambda r: r["clients"])
        ax.plot([r["clients"] for r in series],
                [r["tps"] for r in series],
                marker="o", markersize=6, linewidth=2.2,
                label=iso.title())
    ax.set_xticks(list(clients))
    ax.set_xlabel("Concurrent clients (database connections)")
    ax.set_ylabel("Committed checkout transactions per second (TPS)")
    ax.set_title("PostgreSQL checkout throughput vs. concurrency\n"
                 "Read Committed / Repeatable Read / Serializable")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Isolation level")
    fig.tight_layout()
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-config", type=Path, default=DEFAULT_DB_CONFIG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--clients", type=str, default="1,5,10,20,50,100",
                        help="Comma-separated client counts")
    parser.add_argument("--duration", type=float, default=12,
                        help="Measured seconds per scenario (default 12)")
    parser.add_argument("--warmup", type=float, default=2,
                        help="Warm-up seconds per scenario (default 2)")
    parser.add_argument("--pool-size", type=int, default=800)
    parser.add_argument("--customer-pool", type=int, default=10000)
    parser.add_argument("--hot-pool-size", type=int, default=64)
    parser.add_argument("--hot-probability", type=float, default=0.75)
    parser.add_argument("--items-per-order", type=int, default=2)
    parser.add_argument("--stock-floor", type=int, default=50000,
                        help="Temporary available stock per selected balance")
    parser.add_argument("--max-retries", type=int, default=10)
    parser.add_argument("--seed", type=int, default=486)
    parser.add_argument("--min-variant", type=int, default=1)
    parser.add_argument("--max-variant", type=int, default=28673165)
    parser.add_argument("--allow-live-inventory", action="store_true",
                        help="Required acknowledgement of real inventory mutation")
    parser.add_argument("--recover", action="store_true",
                        help="Restore an interrupted run, then drop its schema")
    args = parser.parse_args()
    args.clients = tuple(int(part.strip()) for part in args.clients.split(","))
    if not args.clients or min(args.clients) < 1 or len(set(args.clients)) != len(args.clients):
        parser.error("--clients must be distinct positive integers")
    if args.duration <= 0 or args.warmup < 0:
        parser.error("--duration must be positive and --warmup nonnegative")
    if args.pool_size < 2 or args.customer_pool < 1 or args.items_per_order < 1:
        parser.error("invalid pool/checkout size")
    if args.items_per_order > args.pool_size or args.hot_pool_size < 1:
        parser.error("items-per-order must fit pool-size; hot-pool-size >= 1")
    if args.hot_probability == 1 and args.hot_pool_size < args.items_per_order:
        parser.error("100% hot selection requires hot-pool-size >= items-per-order")
    if not 0 <= args.hot_probability <= 1 or args.stock_floor < 1:
        parser.error("hot-probability must be 0..1 and stock-floor positive")
    if args.max_retries < 0 or args.max_variant < args.min_variant:
        parser.error("invalid retries or variant range")
    if not args.recover and not args.allow_live_inventory:
        parser.error("This benchmark updates LIVE inventory rows. Run on a test DB "
                     "and pass --allow-live-inventory after reading this warning.")
    return args


def main():
    args = parse_arguments()
    db_kwargs = load_connection_kwargs(args.db_config)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    admin = connect(db_kwargs)
    db_name, db_user, server_addr, server_port = admin.execute(
        "SELECT current_database(), current_user, inet_server_addr(), inet_server_port()"
    ).fetchone()
    print(f"Connected to database={db_name!r}, user={db_user!r}, "
          f"server={server_addr or '(Unix socket)'}:{server_port or '(socket)'}")
    locked = False
    owned_schema_created = False
    try:
        locked = admin.execute("SELECT pg_try_advisory_lock(%s, %s)",
                               (LOCK_KEY_A, LOCK_KEY_B)).fetchone()[0]
        if not locked:
            raise SafetyError("Another benchmark process holds the experiment lock")
        # No other database clients should write inventory until cleanup completes.
        state = require_owned_schema(admin)
        if args.recover:
            if state == "missing":
                print("No interrupted benchmark found")
            else:
                recover_and_drop(admin)
            return
        if state != "missing":
            raise SafetyError(
                f"{SCHEMA} exists (state={state}). "
                "An earlier run may have crashed. Run with --recover first."
            )
        validate_connection_slots(admin, max(args.clients))
        customers = get_customers(admin, args.customer_pool)
        items = fetch_inventory_candidates(admin, args.pool_size,
                                           args.min_variant, args.max_variant)
        if len(items) < args.items_per_order:
            raise SafetyError("Not enough DISTINCT stocked variants for checkout")
        print(f"Discovered {len(items)} real stocked variant/balance pairs "
              f"from variant IDs [{min(x.variant_id for x in items)}, "
              f"{max(x.variant_id for x in items)}], "
              f"across {len(set(x.warehouse_id for x in items))} warehouses")
        print(f"Loaded {len(customers)} real customers; "
              f"{args.items_per_order} items/order; "
              f"{args.hot_probability:.0%} of picks favor {min(args.hot_pool_size, len(items))} hot rows")
        print("WARNING: temporarily raising selected live inventory balances to "
              f"at least {args.stock_floor} available units; all values and timestamps "
              "will be restored. No other inventory writers should run.")
        create_schema_and_snapshot(admin, items, args.stock_floor)
        owned_schema_created = True
        write_inventory_snapshot(admin,
                                 args.output_dir / "2-transaction-inventory-before.csv")

        output_csv = args.output_dir / "2-transaction-number.csv"
        output_png = args.output_dir / "2-transaction-number.png"
        rows = []
        fieldnames = ["isolation", "clients", "duration_s", "warmup_s",
                      "tps", "committed", "failed", "retries",
                      "serialization_failures", "deadlocks", "stockouts",
                      "avg_latency_ms", "p95_latency_ms"]
        with output_csv.open("w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()
            for clients in args.clients:
                for iso in ISOLATIONS:
                    print(f"\n[{iso.title()}] {clients} clients: "
                          f"{args.warmup}s warmup + {args.duration}s measured")
                    seed_for_scenario(admin)
                    try:
                        result = measure_scenario(args, iso, clients, db_kwargs,
                                                  customers, items)
                    finally:
                        # Important: all worker connections exit before restoration.
                        restore_scenario(admin)
                    rows.append(result)
                    writer.writerow(result)
                    out.flush()
                    print(f"  TPS={result['tps']:.1f}, commits={result['committed']}, "
                          f"retries={result['retries']}, "
                          f"serialization_failures={result['serialization_failures']}, "
                          f"failed_checkouts={result['failed']}, "
                          f"p95={result['p95_latency_ms']:.1f}ms")
        plot_results(rows, output_png, args.clients)
        print(f"\nCompleted all scenarios. CSV: {output_csv}\nFigure: {output_png}")
    finally:
        if locked:
            try:
                if owned_schema_created:
                    recover_and_drop(admin)
            finally:
                try:
                    admin.execute("SELECT pg_advisory_unlock(%s, %s)",
                                  (LOCK_KEY_A, LOCK_KEY_B))
                finally:
                    admin.close()
        else:
            admin.close()



# Experiment 3 overrides: fixed concurrency, varying uniformly selected variants.
# All 10 cloned order tables are retained; only their data is truncated.
VARIANT_COUNTS = (1, 10, 50, 100, 500)

def ensure_experiment_ready(conn, items, stock_floor):
    state = require_owned_schema(conn)
    if state == 'missing':
        create_schema_and_snapshot(conn, items, stock_floor)
        return
    if state != 'ready':
        raise SafetyError('Interrupted experiment: run --recover first')
    with conn.transaction():
        # Existing experiment must already have no checkout records.
        count = conn.execute(f'SELECT COUNT(*) FROM {SCHEMA}.benchmark_picks').fetchone()[0]
        if count:
            raise SafetyError('Experiment ledger not empty; investigate before refreshing')
        conn.execute(f'TRUNCATE {SCHEMA}.inventory_baseline')
        ids = [x.balance_id for x in items]
        conn.execute(f'''
            INSERT INTO {SCHEMA}.inventory_baseline
                (balance_id, warehouse_id, variant_id, quantity_on_hand,
                 quantity_reserved, original_updated_at, seed_on_hand)
            SELECT balance_id, warehouse_id, variant_id, quantity_on_hand,
                   quantity_reserved, updated_at,
                   GREATEST(quantity_on_hand, quantity_reserved + %s)
            FROM db_project_inventory.inventory_balances
            WHERE balance_id = ANY(%s)
        ''', (stock_floor, ids))
        if conn.execute(f'SELECT COUNT(*) FROM {SCHEMA}.inventory_baseline').fetchone()[0] != len(items):
            raise SafetyError('Missing inventory balance during snapshot')

def recover_keep_schema(conn):
    state = require_owned_schema(conn)
    if state == 'missing':
        print('No experiment schema found')
        return
    if state == 'seeded':
        restore_scenario(conn)
    # For ready state, verify snapshot if present, then clear rows.
    with conn.transaction():
        tables = ', '.join(f'{SCHEMA}.{t}' for t in (*ORDER_TABLES, 'benchmark_picks'))
        conn.execute(f'TRUNCATE TABLE {tables} RESTART IDENTITY')
    print(f'{SCHEMA} is ready; tables retained, experiment orders cleared')

def parse_latency_args():
    parser = argparse.ArgumentParser(description='Inventory contention versus checkout latency')
    parser.add_argument('--db-config', type=Path, default=DEFAULT_DB_CONFIG)
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    parser.add_argument('--clients', type=int, default=50)
    parser.add_argument('--variants', default='1,10,50,100,500')
    parser.add_argument('--duration', type=float, default=12)
    parser.add_argument('--warmup', type=float, default=2)
    parser.add_argument('--customer-pool', type=int, default=10000)
    parser.add_argument('--stock-floor', type=int, default=100000)
    parser.add_argument('--max-retries', type=int, default=10)
    parser.add_argument('--seed', type=int, default=486)
    parser.add_argument('--min-variant', type=int, default=1)
    parser.add_argument('--max-variant', type=int, default=28673165)
    parser.add_argument('--allow-live-inventory', action='store_true')
    parser.add_argument('--recover', action='store_true')
    args = parser.parse_args()
    try:
        args.variants = tuple(int(v.strip()) for v in args.variants.split(','))
    except ValueError:
        parser.error('--variants must contain comma-separated integers')
    if not args.variants or min(args.variants) < 1 or len(set(args.variants)) != len(args.variants):
        parser.error('--variants must contain distinct positive integers')
    if args.clients < 1 or args.duration <= 0 or args.warmup < 0 or args.max_retries < 0:
        parser.error('Invalid clients, duration, warmup, or retries')
    if args.customer_pool < 1 or args.stock_floor < 1 or args.min_variant < 1 or args.max_variant < args.min_variant:
        parser.error('Invalid customer pool, stock floor, or variant ID range')
    if not args.recover and not args.allow_live_inventory:
        parser.error('Real inventory is temporarily modified. Use a test database and pass --allow-live-inventory')
    # Arguments expected by the unchanged Experiment 2 checkout worker.
    args.items_per_order = 1
    args.hot_probability = 1.0
    return args

def plot_contention(rows, path, field, ylabel, title):
    fig, ax = plt.subplots(figsize=(10, 6))
    for isolation in ISOLATIONS:
        series = sorted((r for r in rows if r['isolation'] == isolation.title()),
                        key=lambda r: r['variants'])
        ax.plot([r['variants'] for r in series],
                [r[field] for r in series], marker='o', linewidth=2.2,
                label=isolation.title())
    counts = sorted({r['variants'] for r in rows})
    ax.set_xscale('log')
    ax.set_xticks(counts)
    ax.set_xticklabels([str(x) for x in counts])
    ax.set_xlabel('Distinct contended product variants (50 concurrent clients)')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend(title='Isolation level')
    fig.tight_layout()
    fig.savefig(path, dpi=200, bbox_inches='tight')
    plt.close(fig)

def main():
    args = parse_latency_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    db_kwargs = load_connection_kwargs(args.db_config)
    admin = connect(db_kwargs)
    locked = False
    try:
        locked = admin.execute('SELECT pg_try_advisory_lock(%s,%s)',
                               (LOCK_KEY_A, LOCK_KEY_B)).fetchone()[0]
        if not locked:
            raise SafetyError('Another experiment holds the advisory lock')
        if args.recover:
            recover_keep_schema(admin)
            return
        validate_connection_slots(admin, args.clients)
        customers = get_customers(admin, args.customer_pool)
        items = fetch_inventory_candidates(admin, max(args.variants),
                                           args.min_variant, args.max_variant)
        if len(items) < max(args.variants):
            raise SafetyError(f'Only {len(items)} distinct stocked variants found; need {max(args.variants)}')
        # Stable random pool, nested subsets. All isolation levels see the same variants.
        random.Random(args.seed).shuffle(items)
        ensure_experiment_ready(admin, items, args.stock_floor)
        write_inventory_snapshot(admin, args.output_dir / '3-transaction-inventory-before.csv')
        rows = []
        fields = ['isolation', 'clients', 'variants', 'duration_s', 'warmup_s',
                  'tps', 'committed', 'failed', 'retries', 'serialization_failures',
                  'deadlocks', 'stockouts', 'avg_latency_ms', 'p95_latency_ms',
                  'conflict_events_per_100_commits']
        csv_path = args.output_dir / '3-transaction-latency.csv'
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for count in args.variants:
                selected = items[:count]
                for isolation in ISOLATIONS:
                    print(f'\n{count} variants | {isolation} | {args.clients} workers', flush=True)
                    seed_for_scenario(admin)
                    try:
                        # The original worker uses hot_items=items[:hot_pool_size].
                        # 100% hot probability means UNIFORM selection within this pool.
                        args.hot_pool_size = count
                        result = measure_scenario(args, isolation, args.clients,
                                                  db_kwargs, customers, selected)
                    finally:
                        restore_scenario(admin)
                    result['variants'] = count
                    result['conflict_events_per_100_commits'] = round(
                        100 * (result['serialization_failures'] + result['deadlocks']) /
                        max(1, result['committed']), 3)
                    rows.append(result)
                    writer.writerow(result)
                    f.flush()
                    print(f"  avg={result['avg_latency_ms']:.2f} ms, "
                          f"p95={result['p95_latency_ms']:.2f} ms, "
                          f"TPS={result['tps']:.2f}, retries={result['retries']}, "
                          f"serialization failures={result['serialization_failures']}", flush=True)
        plot_contention(rows, args.output_dir / '3-transaction-latency.png',
                        'avg_latency_ms', 'Average committed checkout latency (ms)',
                        'PostgreSQL checkout latency vs inventory contention')
        plot_contention(rows, args.output_dir / '3-transaction-throughput.png',
                        'tps', 'Committed checkout transactions per second (TPS)',
                        'PostgreSQL checkout throughput vs inventory contention')
        plot_contention(rows, args.output_dir / '3-transaction-conflicts.png',
                        'conflict_events_per_100_commits',
                        'Serialization failures + deadlocks per 100 commits',
                        'PostgreSQL checkout conflicts vs inventory contention')
        print(f'\nResults: {csv_path}\nAll selected inventory balances restored; '
              'experimental order rows cleared; schema retained.')
    finally:
        if locked:
            try:
                if require_owned_schema(admin) == 'seeded':
                    restore_scenario(admin)
            finally:
                admin.execute('SELECT pg_advisory_unlock(%s,%s)',
                              (LOCK_KEY_A, LOCK_KEY_B))
        admin.close()

if __name__ == '__main__':
    try:
        main()
    except (SafetyError, psycopg.Error, OSError, RuntimeError, ValueError) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        print('If inventory restoration failed, run this file with --recover. '
              'Do not manually delete the experiment snapshot.', file=sys.stderr)
        sys.exit(1)
