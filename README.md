
psql -U nishan -d ecommerce_db -f ../catalog.sql 

psql -U nishan -d ecommerce_db -f warehouse.sql

python3 experiments/1-index.py --confirm-drop --timeout-ms 300000


python experiments/2-transaction-number.py --allow-live-inventory

python experiments/2-transaction-number.py \
    --allow-live-inventory \
    --clients 1,5,10,20,50

If the program is interrupted, use:
python experiments/2-transaction-number.py --recover

python experiments/3-transaction-latency.py --allow-live-inventory

python experiments/3-transaction-latency.py \
  --clients 50 \
  --variants 1,10,50,100,500 \
  --duration 30 \
  --warmup 3 \
  --allow-live-inventory

python experiments/3-transaction-latency.py --recover