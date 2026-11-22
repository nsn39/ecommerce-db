
import argparse
from generators.catalog import main as catalog_main
from generators.identity import main as identity_main
from generators.warehouse import main as warehouse_main
from generators.inventory import main as inventory_main
from generators.historical_orders import main as orders_main
from generators.historical_payments import main as payments_main

def main():
    parser = argparse.ArgumentParser(
        description="E-commerce Data Generator"
    )

    parser.add_argument(
        "command",
        choices=["catalog_load", "identity_load", "warehouse_load", "inventory_load", "orders_load", "payments_load"],
        help="Command to execute"
    )

    args = parser.parse_args()

    if args.command == "catalog_load":
        catalog_main()

    if args.command == "identity_load":
        identity_main()

    if args.command == "warehouse_load":
        warehouse_main()
    
    if args.command == "inventory_load":
        inventory_main()

    if args.command == "orders_load":
        orders_main()

    if args.command == "payments_load":
        payments_main()


if __name__ == "__main__":
    main()
