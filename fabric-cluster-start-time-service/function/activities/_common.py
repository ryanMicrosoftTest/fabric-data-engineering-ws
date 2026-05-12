"""Shared helpers for activity functions."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

from shared.config import Settings
from shared.warehouse_writer import WarehouseWriter


def get_credential(settings: Settings):
    if settings.mi_client_id:
        return ManagedIdentityCredential(client_id=settings.mi_client_id)
    return DefaultAzureCredential()


def _persist_sync(
    settings: Settings,
    credential: Any,
    table: str,
    key_columns: list[str],
    rows: list[dict],
    collector_run_id: str,
) -> int:
    if not rows:
        return 0
    with WarehouseWriter(settings, credential) as wh:
        return wh.upsert_raw(table, key_columns, rows, collector_run_id)


async def persist_raw(
    settings: Settings,
    credential: Any,
    table: str,
    key_columns: list[str],
    rows: list[dict],
    collector_run_id: str,
) -> int:
    return await asyncio.to_thread(
        _persist_sync, settings, credential, table, key_columns, rows, collector_run_id
    )


def build_row(payload: dict, key_values: dict, collector_run_id: str) -> dict:
    return {
        **key_values,
        "raw_payload": json.dumps(payload, default=str),
        "collector_run_id": collector_run_id,
        "ingested_at": datetime.now(timezone.utc),
    }


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)
