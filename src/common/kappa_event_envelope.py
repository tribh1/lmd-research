from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import uuid
from typing import Any, Dict, Iterable, Optional


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def key_from_columns(row: Dict[str, Any], key_columns: Iterable[str]) -> str:
    payload = {column: row.get(column) for column in key_columns}
    return stable_hash(payload)


def make_event_id(
    *,
    batch_run_id: str,
    topic: str,
    key: str,
    row_hash: str,
) -> str:
    """
    Deterministic event id for idempotent replay.
    If the same batch run, topic, business key and row content are replayed,
    the same event id is generated.
    """
    return stable_hash(
        {
            "batch_run_id": batch_run_id,
            "topic": topic,
            "key": key,
            "row_hash": row_hash,
        }
    )


def make_batch_run_id(prefix: str, name: str, explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit
    now = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    return f"{prefix}_{name}_{now}_{uuid.uuid4().hex[:8]}"


def make_debezium_compatible_snapshot_event(
    *,
    row: Dict[str, Any],
    op: str,
    source_system: str,
    source_database: str,
    source_schema: str,
    source_table: str,
    topic: str,
    key_columns: Iterable[str],
    batch_run_id: str,
    config_version: str,
    extraction_mode: str,
) -> Dict[str, Any]:
    """
    Build a Debezium-compatible envelope for batch snapshot/backfill events.
    The downstream Kappa pipeline can read it with the same logic used for CDC.

    Debezium operation mapping:
    - r: snapshot/read event, recommended for initial load and backfill
    - c/u/d: can be used for synthetic insert/update/delete events if needed
    """
    event_ts_ms = int(dt.datetime.utcnow().timestamp() * 1000)
    key = key_from_columns(row, key_columns)
    row_hash = stable_hash(row)
    event_id = make_event_id(
        batch_run_id=batch_run_id,
        topic=topic,
        key=key,
        row_hash=row_hash,
    )

    return {
        "schema": None,
        "payload": {
            "before": None,
            "after": row,
            "op": op,
            "ts_ms": event_ts_ms,
            "source": {
                "version": "config-driven-batch-as-event",
                "connector": "batch-snapshot",
                "name": source_system,
                "db": source_database,
                "schema": source_schema,
                "table": source_table,
                "snapshot": "true",
                "batch_run_id": batch_run_id,
                "config_version": config_version,
                "extraction_mode": extraction_mode,
                "event_id": event_id,
                "row_hash": row_hash,
            },
            "transaction": None,
        },
    }
