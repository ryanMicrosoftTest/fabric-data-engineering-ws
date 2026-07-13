"""Synthetic data generation for on-prem-to-Fabric warehouse POC.

This module provides functions to generate realistic synthetic data
for testing and demonstration of the Fabric Warehouse write pipeline.
"""

from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd


def generate_orders(n_rows: int = 1000, seed: Optional[int] = 42) -> pd.DataFrame:
    """Generate a synthetic orders DataFrame.

    Creates a realistic orders dataset with order ID, customer ID, timestamp,
    amount, and region. Useful for testing Fabric Warehouse write operations.

    Args:
        n_rows: Number of rows to generate. Must be >= 1.
        seed: Random seed for reproducibility. None means non-deterministic.

    Returns:
        pandas.DataFrame with columns:
            - order_id (int64): Sequential order IDs from 1 to n_rows
            - customer_id (int64): Random customer IDs in range [1000, 9999]
            - order_ts (datetime64[ns]): Random timestamps within last 90 days (UTC)
            - amount (float64): Random amounts in [5.00, 999.99], rounded to 2 decimals
            - region (object): Random region from ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]

    Raises:
        ValueError: If n_rows < 1
    """
    if n_rows < 1:
        raise ValueError(f"n_rows must be >= 1, got {n_rows}")

    rng = np.random.default_rng(seed)

    # Generate each column
    order_id = np.arange(1, n_rows + 1, dtype=np.int64)

    customer_id = rng.integers(1000, 10000, size=n_rows, dtype=np.int64)

    # Generate timestamps within last 90 days from now (UTC)
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    ninety_days_ago = now - timedelta(days=90)
    timestamps = pd.to_datetime(
        rng.uniform(
            ninety_days_ago.timestamp(),
            now.timestamp(),
            size=n_rows,
        ),
        unit="s",
        utc=False,
    )

    # Generate amounts: random in [5.00, 999.99], rounded to 2 decimals
    amount = np.round(rng.uniform(5.0, 999.99, size=n_rows), decimals=2).astype(np.float64)

    # Generate regions: random choice from the list
    regions = np.array(["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"], dtype=object)
    region = rng.choice(regions, size=n_rows)

    # Create DataFrame with explicit dtypes
    df = pd.DataFrame(
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_ts": timestamps,
            "amount": amount,
            "region": region,
        }
    )

    # Ensure explicit dtypes
    df = df.astype({
        "order_id": "int64",
        "customer_id": "int64",
        "order_ts": "datetime64[ns]",
        "amount": "float64",
        "region": "object",
    })

    return df
