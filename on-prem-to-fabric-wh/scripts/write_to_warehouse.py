"""Entry script: generate synthetic orders and write to a Fabric Warehouse."""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import argparse
import logging

from src.config import Config
from src.secrets import get_spn_secret
from src.token_provider import get_warehouse_access_token
from src.warehouse_writer import connect, write_dataframe
from src.synthetic_data import generate_orders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("write_to_warehouse")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Write synthetic orders to Fabric Warehouse.")
    p.add_argument("--rows", type=int, default=1000, help="Number of synthetic rows to generate.")
    p.add_argument("--if-exists", choices=["append", "replace", "fail"], default="append")
    p.add_argument("--table", type=str, default=None, help="Overrides Config.warehouse_table.")
    p.add_argument("--seed", type=int, default=42, help="Seed passed to generate_orders.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    try:
        logger.info("Loading configuration from environment")
        cfg = Config.from_env()

        logger.info("Fetching SPN secret from Key Vault '%s'", cfg.kv_name)
        spn_secret = get_spn_secret(cfg.kv_name, cfg.kv_secret_name)

        logger.info("Acquiring warehouse access token via SPN")
        token_struct = get_warehouse_access_token(cfg.azure_tenant_id, cfg.spn_client_id, spn_secret)

        logger.info("Generating %d synthetic rows (seed=%d)", args.rows, args.seed)
        df = generate_orders(n_rows=args.rows, seed=args.seed)

        table = args.table or cfg.warehouse_table

        logger.info("Connecting to warehouse %s/%s", cfg.warehouse_server, cfg.warehouse_database)
        conn = connect(cfg.warehouse_server, cfg.warehouse_database, token_struct)
        try:
            logger.info("Writing dataframe to [%s].[%s] (if_exists=%s)", cfg.warehouse_schema, table, args.if_exists)
            rows = write_dataframe(conn, df, cfg.warehouse_schema, table, if_exists=args.if_exists)
        finally:
            conn.close()

        print(f"SUCCESS: wrote {rows} rows to [{cfg.warehouse_schema}].[{table}]")
        sys.exit(0)
    except Exception as exc:
        logger.exception("Write to warehouse failed")
        print(f"FAILED: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
