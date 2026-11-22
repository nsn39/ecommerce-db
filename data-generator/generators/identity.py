"""Generate resumable synthetic US ecommerce customers.

Run from project root: python main.py customers_load
Or directly: python path/to/customers.py
Requires: pip install 'psycopg[binary]' PyYAML Faker
"""
from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from faker import Faker
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / 'config' / 'database.yaml'
CHECKPOINT_FILE = ROOT / 'checkpoints' / 'identity.yaml'
SCHEMA = 'db_project_identity'
DEFAULT_GOAL = 2_000_000
DEFAULT_BATCH_SIZE = 2_000
LOG = logging.getLogger('customers')
FAKE = Faker('en_US')


def read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open('r', encoding='utf-8') as f:
        value = yaml.safe_load(f) or {}
    if not isinstance(value, dict):
        raise ValueError(f'Expected YAML mapping in {path}')
    return value


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
    kwargs = {
        'host': pg.get('host', 'localhost'),
        'port': int(pg.get('port', 5432)),
        'dbname': pg.get('database', pg.get('dbname')),
        'user': pg.get('user'),
        'password': password,
    }
    return psycopg.connect(**{k: v for k, v in kwargs.items() if v is not None})


def load_checkpoint() -> tuple[int, int]:
    state = read_yaml(CHECKPOINT_FILE)
    goal = state.get('goal', DEFAULT_GOAL)
    generated = state.get('generated', 0)
    if type(goal) is not int or type(generated) is not int or goal < 1 or not 0 <= generated <= goal:
        raise ValueError('identity checkpoint requires integers: goal > 0, 0 <= generated <= goal')
    return generated, goal


def save_checkpoint(generated: int, goal: int) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp = CHECKPOINT_FILE.with_suffix('.yaml.tmp')
    with temp.open('w', encoding='utf-8') as f:
        yaml.safe_dump({'generated': generated, 'goal': goal}, f, sort_keys=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, CHECKPOINT_FILE)


def customer_number(n: int) -> str:
    return f'CUST-{n:010d}'


def generate_customer(n: int, now: datetime) -> dict:
    # Per-customer seeds keep replay stable across crashes, even if Faker's global
    # random state has moved. Never use these fake credentials as real identities.
    rng = random.Random(n * 7919 + 104729)
    FAKE.seed_instance(n * 104729 + 7919)
    fake = FAKE
    first = fake.first_name()
    last = fake.last_name()
    registered = now - timedelta(days=rng.randint(0, 15 * 365), seconds=rng.randint(0, 86399))
    age = rng.randint(18, 85)
    dob = fake.date_of_birth(minimum_age=age, maximum_age=age)
    street = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zip_code = fake.postcode()
    # Unique, syntactically valid test email; reserved example domain prevents
    # accidental delivery to a real person. Faker names/addresses are synthetic.
    email = f'customer{n:010d}@example.com'
    # Faker phone values are not guaranteed unique or dialable; optional phone
    # contacts are omitted to avoid accidentally resembling real phone numbers.
    return {
        'number': customer_number(n), 'first': first, 'last': last,
        'registered': registered, 'dob': dob, 'street': street,
        'city': city, 'state': state, 'zip': zip_code, 'email': email,
        'marketing': rng.random() < 0.28,
        'email_notifications': rng.random() < 0.86,
        'status': rng.choices(['active', 'suspended', 'closed'], weights=[96, 2, 2])[0],
    }


def insert_customer(cur: psycopg.Cursor, n: int, now: datetime) -> bool:
    # ON CONFLICT also protects against a crash after commit but before checkpoint.
    # We deliberately do not update existing customers or duplicate child rows.
    d = generate_customer(n, now)
    cur.execute(
        f'''INSERT INTO {SCHEMA}.customers (customer_number, status, registered_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_number) DO NOTHING RETURNING customer_id''',
        (d['number'], d['status'], d['registered'], d['registered']),
    )
    row = cur.fetchone()
    if row is None:
        return False
    cid = row[0]
    cur.execute(
        f'''INSERT INTO {SCHEMA}.customer_profiles
            (customer_id, first_name, last_name, date_of_birth, preferred_language, updated_at)
            VALUES (%s, %s, %s, %s, 'en-US', %s)''',
        (cid, d['first'], d['last'], d['dob'], d['registered']),
    )
    cur.execute(
        f'''INSERT INTO {SCHEMA}.customer_addresses
            (customer_id, label, recipient_name, address_line1, city, region,
             postal_code, country_code, is_default_shipping, is_default_billing,
             created_at, updated_at)
            VALUES (%s, 'Home', %s, %s, %s, %s, %s, 'US', TRUE, TRUE, %s, %s)''',
        (cid, f"{d['first']} {d['last']}"[:200], d['street'][:255],
         d['city'][:120], d['state'][:120], d['zip'][:30],
         d['registered'], d['registered']),
    )
    cur.execute(
        f'''INSERT INTO {SCHEMA}.customer_contacts
            (customer_id, contact_type, contact_value, is_primary, created_at)
            VALUES (%s, 'email', %s, TRUE, %s)''',
        (cid, d['email'], d['registered']),
    )
    cur.execute(
        f'''INSERT INTO {SCHEMA}.customer_preferences
            (customer_id, preferred_currency, timezone, email_notifications,
             sms_notifications, marketing_opt_in, updated_at)
            VALUES (%s, 'USD', 'America/Chicago', %s, FALSE, %s, %s)''',
        (cid, d['email_notifications'], d['marketing'], d['registered']),
    )
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    generated, goal = load_checkpoint()
    cfg = read_yaml(CONFIG_FILE)
    batch_size = int(cfg.get('loading', {}).get('identity_batch_size', DEFAULT_BATCH_SIZE))
    if batch_size < 1:
        raise ValueError('loading.identity_batch_size must be >= 1')
    # Freeze the time horizon across restarts so the historical window is stable.
    # This is a synthetic data generator, not a record of real signup activity.
    now = datetime.now(timezone.utc)
    LOG.info('Starting at %d of %d, batch size %d', generated, goal, batch_size)
    with connect() as conn:
        # Advisory lock prevents two instances of this generator from running
        # against the same database concurrently.
        with conn.cursor() as cur:
            cur.execute('SELECT pg_try_advisory_lock(%s)', (837264910,))
            if not cur.fetchone()[0]:
                raise RuntimeError('Another customers generator is already running')
        conn.commit()
        try:
            while generated < goal:
                end = min(generated + batch_size, goal)
                inserted = 0
                with conn.transaction():
                    with conn.cursor() as cur:
                        for n in range(generated + 1, end + 1):
                            if insert_customer(cur, n, now):
                                inserted += 1
                generated = end
                save_checkpoint(generated, goal)
                LOG.info('Progress %d/%d (+%d new customers)', generated, goal, inserted)
        finally:
            with conn.cursor() as cur:
                cur.execute('SELECT pg_advisory_unlock(%s)', (837264910,))
            conn.commit()
    LOG.info('Customer generation completed: %d customer numbers processed', generated)


if __name__ == '__main__':
    main()
