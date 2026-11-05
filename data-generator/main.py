
import argparse
from generators.catalog import main as catalog_main


def main():
    parser = argparse.ArgumentParser(
        description="E-commerce Data Generator"
    )

    parser.add_argument(
        "command",
        choices=["catalog_load"],
        help="Command to execute"
    )

    args = parser.parse_args()

    if args.command == "catalog_load":
        catalog_main()


if __name__ == "__main__":
    main()
