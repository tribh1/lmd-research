from __future__ import annotations

from typing import Any, Dict, Iterable, List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.config_io import table_ident, write_iceberg
from src.common.table_registry import RuntimeConfig


def write_audit_event(spark: SparkSession, cfg: RuntimeConfig, event: Dict[str, Any]) -> None:
    row = {k: _to_primitive(v) for k, v in event.items()}
    row.setdefault("created_at", None)
    df = spark.createDataFrame([row]).withColumn("created_at", F.current_timestamp())
    write_iceberg(df, table_ident(cfg, cfg.audit_namespace, "pipeline_events"), write_mode="append")


def write_quality_results(spark: SparkSession, cfg: RuntimeConfig, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        return
    df = spark.createDataFrame(rows).withColumn("created_at", F.current_timestamp())
    write_iceberg(df, table_ident(cfg, cfg.audit_namespace, "quality_results"), write_mode="append")


def _to_primitive(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(x) for x in value)
    if isinstance(value, dict):
        return str(value)
    return str(value)
