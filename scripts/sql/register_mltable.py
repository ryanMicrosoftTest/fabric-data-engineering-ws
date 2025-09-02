"""Minimal script to register an MLTable data asset backed by an abfss path.

Edit the constants below and run:
  python scripts/sql/register_mltable.py

Prerequisites:
 - azure-ai-ml and dependencies installed
 - You are logged in (az login) OR environment creds available for DefaultAzureCredential
 - The abfss pattern matches existing files (e.g. Parquet) in ADLS Gen2
"""

from pathlib import Path
from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data

# ---- USER CONFIG ----
SUBSCRIPTION_ID = "910ebf13-1058-405d-b6cf-eda03e5288d1"
RESOURCE_GROUP = "aml-rg"
WORKSPACE = "aml-private-dev"

ASSET_NAME = "employee_table"        # Data asset name
ASSET_VERSION = None                  # None => auto versioning, or set e.g. "1"
ABFSS_PATTERN = (
    "abfss://output@healthsaadlsrheus.dfs.core.windows.net/employee/*.parquet"
)  # Full abfss pattern (can change to *.csv etc.)
BUILD_DIR = Path(".mltable_spec")     # Local folder to hold MLTable spec
# ----------------------


def ensure_spec(pattern: str, build_dir: Path) -> Path:
    build_dir.mkdir(parents=True, exist_ok=True)
    spec_file = build_dir / "MLTable"
    if not spec_file.exists():
        spec_file.write_text(
            "paths:\n"
            f"  - pattern: {pattern}\n",
            encoding="utf-8",
        )
    return spec_file.parent


def main() -> None:
    spec_dir = ensure_spec(ABFSS_PATTERN, BUILD_DIR / ASSET_NAME)
    ml_client = MLClient(
        DefaultAzureCredential(), SUBSCRIPTION_ID, RESOURCE_GROUP, WORKSPACE
    )
    data_asset = Data(
        name=ASSET_NAME,
        version=ASSET_VERSION,
        type="mltable",
        path=str(spec_dir),
        description=f"Auto-registered MLTable for pattern {ABFSS_PATTERN}",
    )
    result = ml_client.data.create_or_update(data_asset)
    print("Registered data asset:", result.id)


if __name__ == "__main__":  # pragma: no cover
    main()
