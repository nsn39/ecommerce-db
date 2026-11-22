"""Deterministic, resumable synthetic historical order loader (PostgreSQL 15+).

Run: python main.py historical_orders_load
Requires: pip install 'psycopg[binary]' PyYAML

First run snapshots the date window in checkpoints/historical_orders.yaml.
Each committed batch contains whole customers; replay is guarded by deterministic order_number.
Do not run two instances concurrently or change seed/targets while resuming.
"""
from __future__ import annotations

import hashlib
import logging
import math
import os
import random
from array import array
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg import sql
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / 'config' / 'database.yaml'
CHECKPOINT = ROOT / 'checkpoints' / 'historical_orders.yaml'
SCHEMA = 'db_project_orders'
LOG = logging.getLogger('historical_orders')
DEFAULT = {'target_orders': 30_000_000, 'batch_customers': 75,
           'seed': 20261120, 'active_customer_fraction': 0.40,
           'checkout_probability': 0.85, 'extra_abandoned_cart_probability': 0.08,
           'max_items_per_order': 5}
CITIES = [
    ('New York','NY','10001'), ('Los Angeles','CA','90001'),
    ('Chicago','IL','60601'), ('Houston','TX','77001'),
    ('Phoenix','AZ','85001'), ('Philadelphia','PA','19103'),
    ('San Antonio','TX','78205'), ('San Diego','CA','92101'),
    ('Dallas','TX','75201'), ('Jacksonville','FL','32202'),
    ('Austin','TX','78701'), ('Seattle','WA','98101'),
    ('Denver','CO','80202'), ('Boston','MA','02108'),
    ('Atlanta','GA','30303'), ('Minneapolis','MN','55401'),
    ('Portland','OR','97201'), ('Miami','FL','33101'),
    ('Charlotte','NC','28202'), ('Nashville','TN','37201'),
]

def read_yaml(path):
    if not path.exists(): return {}
    with path.open(encoding='utf-8') as f: obj = yaml.safe_load(f)
    if obj is None: return {}
    if not isinstance(obj, dict): raise ValueError(f'Expected YAML mapping in {path}')
    return obj

def write_checkpoint(data):
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    temp = CHECKPOINT.with_suffix('.yaml.tmp')
    with temp.open('w', encoding='utf-8') as f:
        yaml.safe_dump(data, f, sort_keys=False)
        f.flush(); os.fsync(f.fileno())
    os.replace(temp, CHECKPOINT)

def connect():
    cfg = read_yaml(CONFIG).get('postgres', read_yaml(CONFIG))
    password = cfg.get('password')
    if cfg.get('password_env'):
        password = os.environ.get(str(cfg['password_env']))
        if password is None: raise ValueError('Postgres password environment variable is not set')
    return psycopg.connect(host=cfg.get('host','localhost'), port=int(cfg.get('port',5432)),
                           dbname=cfg.get('database',cfg.get('dbname')),
                           user=cfg.get('user'), password=password)

def stable_hash(*values):
    return int.from_bytes(hashlib.blake2b('|'.join(map(str,values)).encode(),digest_size=8).digest(),'big')

def ids(conn, query):
    result = array('q')
    with conn.cursor(name='history_source_scan') as cur:
        cur.itersize = 100000
        cur.execute(query)
        for (value,) in cur: result.append(value)
    return result

def customer_order_counts(n, target, seed, fraction):
    """Exactly target orders, only a fixed fraction of customers active.
    Deterministic heterogeneous gamma/Pareto weights, cumulative rounding.
    """
    active = round(n*fraction)
    if active > target: raise ValueError('Target smaller than active customer count')
    rng = random.Random(seed ^ 0xA193)
    # Choose active customers using a permutation of customer positions.
    stride = rng.randrange(1,n) if n > 1 else 1
    while math.gcd(stride,n) != 1: stride = (stride+1) % n or 1
    offset = rng.randrange(n)
    weights = array('d',[0.0])*n
    total = 0.0
    for i in range(n):
        if (i*stride+offset)%n < active:
            # Many light buyers, few prolific buyers; bounded heavy tail.
            weight = min(70.0, rng.paretovariate(1.65))
            weights[i] = weight
            total += weight
    remaining = target-active
    counts = array('I',[0])*n
    cumulative = 0.0
    assigned = 0
    for i,w in enumerate(weights):
        if not w: continue
        cumulative += w
        upto = min(remaining, int(remaining*cumulative/total + 1e-8))
        counts[i] = 1 + upto-assigned
        assigned = upto
    if sum(counts) != target: raise AssertionError('Order allocation not exact')
    return counts, active

def allocate_ids(cur, table, n):
    if not n: return iter(())
    cur.execute('SELECT nextval(pg_get_serial_sequence(%s,%s)) FROM generate_series(1,%s)',
                (f'{SCHEMA}.{table}', {'shopping_carts':'cart_id','cart_items':'cart_item_id',
                'orders':'order_id','order_items':'order_item_id','order_addresses':'order_address_id',
                'order_status_history':'status_event_id','order_allocations':'allocation_id',
                'order_cancellations':'cancellation_id','checkout_attempts':'checkout_attempt_id'}[table],n))
    return iter(row[0] for row in cur.fetchall())

def copy_table(cur, table, columns, rows):
    if not rows: return
    statement = sql.SQL('COPY {}.{} ({}) FROM STDIN').format(
        sql.Identifier(SCHEMA),sql.Identifier(table),
        sql.SQL(',').join(sql.Identifier(c) for c in columns))
    with cur.copy(statement) as cp:
        for row in rows: cp.write_row(row)

COLS = {
 'shopping_carts': ('cart_id','customer_id','status','currency_code','created_at','updated_at','expires_at'),
 'cart_items': ('cart_item_id','cart_id','variant_id','quantity','added_at','updated_at'),
 'orders': ('order_id','order_number','customer_id','cart_id','status','currency_code','subtotal','discount_total','tax_total','shipping_total','placed_at','updated_at','cancelled_at'),
 'order_items': ('order_item_id','order_id','line_number','variant_id','sku_snapshot','product_name_snapshot','quantity','unit_price','discount_amount','tax_amount'),
 'order_addresses': ('order_address_id','order_id','address_type','recipient_name','address_line1','address_line2','city','region','postal_code','country_code','phone_snapshot'),
 'order_status_history': ('status_event_id','order_id','old_status','new_status','changed_by_employee_id','change_reason','changed_at'),
 'order_allocations': ('allocation_id','order_id','order_item_id','warehouse_id','allocated_quantity','status','allocated_at','updated_at'),
 'order_cancellations': ('cancellation_id','order_id','order_item_id','quantity','reason_code','notes','status','requested_by_customer_id','reviewed_by_employee_id','requested_at','reviewed_at'),
 'checkout_attempts': ('checkout_attempt_id','customer_id','cart_id','idempotency_key','order_id','status','error_code','created_at','completed_at'),
}
INSERT_ORDER = ('shopping_carts','cart_items','orders','order_items','order_addresses',
                'order_status_history','order_allocations','order_cancellations','checkout_attempts')

def choose_variant(rng, variants):
    # 88% from a Zipf-like head (1% of variants), 12% long-tail uniform.
    n = len(variants)
    if rng.random() < .88:
        head = max(1,int(n*.01))
        rank = min(head-1,int(head*(rng.random()**4)))
    else:
        rank = rng.randrange(n)
    return variants[rank]

def build_customer(customer_id, count, seed, variants, warehouses, start, end, settings):
    rng = random.Random(stable_hash(seed,customer_id))
    events = []
    span = max(1,int((end-start).total_seconds())-172800)
    # Sorted timestamps within the requested year, with recent-month uplift.
    timestamps = sorted(start + timedelta(seconds=int(span*(rng.random()**0.83))) for _ in range(count))
    for ordinal, placed in enumerate(timestamps):
        # Multiple unique products per cart, with strong 1-2 item preference.
        line_count = min(settings['max_items_per_order'],1+int(rng.random()**2 * settings['max_items_per_order']))
        chosen = set()
        while len(chosen) < line_count: chosen.add(choose_variant(rng,variants))
        line_variants = sorted(chosen)
        # 950/1000 closed, 3/1000 cancelled, remainder other states, exactly
        # per every 1000 global orders (global ordinal passed separately below).
        events.append({'customer_id':customer_id,'ordinal':ordinal,'placed':placed,
                       'variants':line_variants,'warehouse':warehouses[rng.randrange(len(warehouses))],
                       'city':CITIES[rng.randrange(len(CITIES))],
                       'checkout':rng.random() < settings['checkout_probability'],
                       'abandoned':rng.random() < settings['extra_abandoned_cart_probability']})
    return events

def process_batch(conn, customer_events, first_global_order, seed, settings):
    """All rows for whole customers committed in one transaction."""
    if not customer_events: return 0
    with conn.transaction():
        with conn.cursor() as cur:
            # Protect against accidental replay after a commit but before YAML checkpoint.
            numbers = [f'HIST-{seed}-{e["customer_id"]:08d}-{e["ordinal"]:05d}' for e in customer_events]
            cur.execute(f'SELECT order_number FROM {SCHEMA}.orders WHERE order_number = ANY(%s)',(numbers,))
            found = {r[0] for r in cur.fetchall()}
            if found:
                if len(found) == len(numbers):
                    LOG.warning('Already committed batch; skipping %s orders',len(numbers))
                    return len(numbers)
                raise RuntimeError('Partially existing batch; refusing unsafe replay')
            # Get SKU/product names from real catalog (one query per batch).
            unique_variants = sorted({v for e in customer_events for v in e['variants']})
            cur.execute('''SELECT v.variant_id, v.sku, p.product_name
                           FROM db_project_catalog.product_variants v
                           JOIN db_project_catalog.products p ON p.product_id=v.product_id
                           WHERE v.variant_id = ANY(%s)''',(unique_variants,))
            metadata = {v:(sku,name) for v,sku,name in cur.fetchall()}
            if len(metadata)!=len(unique_variants): raise RuntimeError('Catalog changed during loading')
            # Estimate exact row counts, reserve IDs in one query per table.
            n_orders=len(customer_events)
            n_items=sum(len(e['variants']) for e in customer_events)
            n_status=sum(6 if (first_global_order+i)%1000 < 950 else
                         2 if (first_global_order+i)%1000 < 953 else
                         5 if (first_global_order+i)%1000 < 975 else
                         4 if (first_global_order+i)%1000 < 990 else 3
                         for i in range(n_orders))
            n_cancel=sum(950 <= (first_global_order+i)%1000 < 953 for i in range(n_orders))
            cancelled_items=sum(len(e['variants']) for i,e in enumerate(customer_events)
                                if 950 <= (first_global_order+i)%1000 < 953)
            n_attempt=sum(e['checkout'] for e in customer_events)
            n_abandoned=sum(e['abandoned'] for e in customer_events)
            sizes={'shopping_carts':n_orders+n_abandoned,'cart_items':n_items+n_abandoned,
                   'orders':n_orders,'order_items':n_items,'order_addresses':2*n_orders,
                   'order_status_history':n_status,'order_allocations':n_items-cancelled_items,
                   'order_cancellations':n_cancel,'checkout_attempts':n_attempt}
            # Allocations for cancelled orders are omitted; each other line gets one.
            sequences={table:allocate_ids(cur,table,n) for table,n in sizes.items()}
            rows={table:[] for table in INSERT_ORDER}
            for i,e in enumerate(customer_events):
                global_index=first_global_order+i
                category=global_index%1000
                status='closed' if category<950 else 'cancelled' if category<953 else (
                    'delivered' if category<975 else 'shipped' if category<990 else 'processing')
                customer=e['customer_id']; placed=e['placed']
                cart_id=next(sequences['shopping_carts']); order_id=next(sequences['orders'])
                cart_time=placed-timedelta(minutes=15+stable_hash(seed,global_index,'cart')%5000)
                rows['shopping_carts'].append((cart_id,customer,'converted','USD',cart_time,placed,cart_time+timedelta(days=30)))
                subtotal=0; discount_total=0; tax_total=0
                line_data=[]
                for j,variant in enumerate(e['variants'],1):
                    item_id=next(sequences['order_items'])
                    sku,name=metadata[variant]
                    # Synthetic prices are snapshots; not necessarily catalog price-list prices.
                    price=round(3+(stable_hash(seed,variant,'price')%25000)/100,2)
                    qty=1+(stable_hash(seed,customer,e['ordinal'],variant,'qty')%5 == 0)
                    gross=round(price*qty,2)
                    discount=round(gross*.05,2) if stable_hash(seed,global_index,j,'promo')%8==0 else 0
                    tax=round((gross-discount)*.07,2)
                    subtotal+=gross; discount_total+=discount; tax_total+=tax
                    added=cart_time+timedelta(minutes=j)
                    rows['cart_items'].append((next(sequences['cart_items']),cart_id,variant,qty,added,added))
                    rows['order_items'].append((item_id,order_id,j,variant,sku,name,qty,price,discount,tax))
                    line_data.append((item_id,qty))
                shipping=0 if subtotal>=50 else 5.99
                cancel_time=placed+timedelta(hours=1) if status=='cancelled' else None
                rows['orders'].append((order_id,numbers[i],customer,cart_id,status,'USD',round(subtotal,2),
                                       round(discount_total,2),round(tax_total,2),shipping,placed,
                                       cancel_time or placed+timedelta(days=5),cancel_time))
                city,region,zip_code=e['city']
                for kind in ('shipping','billing'):
                    rows['order_addresses'].append((next(sequences['order_addresses']),order_id,kind,
                        f'Customer {customer}',f'{100+stable_hash(customer,"street")%9800} Example Street',
                        None,city,region,zip_code,'US',None))
                if status=='cancelled':
                    transitions=[('pending_payment',placed),('cancelled',cancel_time)]
                    rows['order_cancellations'].append((next(sequences['order_cancellations']),order_id,
                        None,None,'CUSTOMER_REQUEST','Synthetic full-order cancellation','completed',
                        customer,None,placed+timedelta(minutes=20),cancel_time))
                else:
                    transitions=[('pending_payment',placed),('confirmed',placed+timedelta(minutes=4)),
                                 ('processing',placed+timedelta(hours=4))]
                    if status in ('shipped','delivered','closed'):
                        transitions.append(('shipped',placed+timedelta(days=1)))
                    if status in ('delivered','closed'):
                        transitions.append(('delivered',placed+timedelta(days=3)))
                    if status=='closed': transitions.append(('closed',placed+timedelta(days=5)))
                    for item_id,qty in line_data:
                        rows['order_allocations'].append((next(sequences['order_allocations']),order_id,item_id,
                            e['warehouse'],qty,'fulfilled' if status in ('closed','delivered','shipped') else 'reserved',
                            placed+timedelta(minutes=10),placed+timedelta(days=1)))
                # Compress recent orders' status timelines to end by yesterday.
                history_end = settings['_history_end']
                if transitions[-1][1] > history_end:
                    available = max(1.0, (history_end-placed).total_seconds())
                    original = max(1.0, (transitions[-1][1]-placed).total_seconds())
                    transitions = [(name, placed+timedelta(seconds=available*(when-placed).total_seconds()/original))
                                   for name,when in transitions]
                if status=='cancelled':
                    cancellation_row=rows['order_cancellations'][-1]
                    rows['order_cancellations'][-1]=(*cancellation_row[:-1],transitions[-1][1])
                header=rows['orders'][-1]
                rows['orders'][-1]=(*header[:-2],transitions[-1][1],
                                    transitions[-1][1] if status=='cancelled' else None)
                old=None
                for new,when in transitions:
                    rows['order_status_history'].append((next(sequences['order_status_history']),order_id,old,new,
                        None,'Synthetic historical transition',when))
                    old=new
                if e['checkout']:
                    rows['checkout_attempts'].append((next(sequences['checkout_attempts']),customer,cart_id,
                        f'HIST-CHECKOUT-{seed}-{customer}-{e["ordinal"]}',order_id,'succeeded',None,
                        placed-timedelta(minutes=1),placed))
                if e['abandoned']:
                    abandoned_cart=next(sequences['shopping_carts'])
                    abandon_time=cart_time-timedelta(days=1)
                    rows['shopping_carts'].append((abandoned_cart,customer,'abandoned','USD',
                        abandon_time,abandon_time+timedelta(hours=2),abandon_time+timedelta(days=30)))
                    rows['cart_items'].append((next(sequences['cart_items']),abandoned_cart,
                        e['variants'][0],1,abandon_time,abandon_time))
            for table in INSERT_ORDER:
                if len(rows[table])!=sizes[table]:
                    raise AssertionError(f'{table}: expected {sizes[table]}, got {len(rows[table])}')
                copy_table(cur,table,COLS[table],rows[table])
    return len(customer_events)

def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    cfg={**DEFAULT,**read_yaml(CONFIG).get('historical_orders',{})}
    if not 0<cfg['active_customer_fraction']<=1: raise ValueError('Invalid active_customer_fraction')
    if cfg['batch_customers']<1 or cfg['target_orders']<1: raise ValueError('Invalid batch/target')
    state=read_yaml(CHECKPOINT)
    if not state:
        # Yesterday inclusive: historical order placement from one year ago to yesterday.
        today=datetime.now(timezone.utc).date()
        end=datetime.combine(today-timedelta(days=1),datetime.max.time(),timezone.utc)
        start=end-timedelta(days=365)
        state={'version':1,'seed':cfg['seed'],'target_orders':cfg['target_orders'],
               'active_customer_fraction':cfg['active_customer_fraction'],
               'start_utc':start.isoformat(),'end_utc':end.isoformat(),
               'next_customer_index':0,'committed_orders':0,'completed':False}
        write_checkpoint(state)
    for key in ('seed','target_orders','active_customer_fraction'):
        if state[key]!=cfg[key]: raise ValueError(f'Cannot change {key} after checkpoint created')
    start=datetime.fromisoformat(state['start_utc']); end=datetime.fromisoformat(state['end_utc'])
    cfg['_history_end']=end
    with connect() as conn:
        customers=ids(conn,'SELECT customer_id FROM db_project_identity.customers ORDER BY customer_id')
        variants=ids(conn,'SELECT variant_id FROM db_project_catalog.product_variants ORDER BY variant_id')
        with conn.cursor() as cur:
            cur.execute('SELECT warehouse_id FROM db_project_warehouse.warehouses WHERE status=%s ORDER BY warehouse_id',('active',))
            warehouses=[r[0] for r in cur.fetchall()]
        conn.commit()
        if not customers or not variants or not warehouses: raise RuntimeError('Customers, variants and active warehouses required')
        counts,active=customer_order_counts(len(customers),cfg['target_orders'],cfg['seed'],cfg['active_customer_fraction'])
        index=state['next_customer_index']
        if index>len(customers): raise RuntimeError('Customer source changed')
        # Recover global ordinal from prefix, independent of checkpoint update timing.
        global_order=sum(counts[:index])
        if state['committed_orders']!=global_order:
            LOG.warning('Checkpoint count differs from computed prefix; relying on deterministic order numbers')
        LOG.info('Customers=%d active=%d variants=%d warehouses=%d target=%d resume_customer=%d',
                 len(customers),active,len(variants),len(warehouses),cfg['target_orders'],index)
        while index<len(customers):
            stop=min(index+cfg['batch_customers'],len(customers))
            batch=[]
            for j in range(index,stop):
                if counts[j]: batch.extend(build_customer(customers[j],counts[j],cfg['seed'],variants,
                                                           warehouses,start,end,cfg))
            written=process_batch(conn,batch,global_order,cfg['seed'],cfg)
            global_order+=written
            index=stop
            state.update(next_customer_index=index,committed_orders=global_order,
                         completed=index==len(customers))
            write_checkpoint(state)
            if index%7500 < cfg['batch_customers']:
                LOG.info('Customers %d/%d; orders %d/%d',index,len(customers),global_order,cfg['target_orders'])
        LOG.info('Finished: %d orders',global_order)

if __name__=='__main__': main()
