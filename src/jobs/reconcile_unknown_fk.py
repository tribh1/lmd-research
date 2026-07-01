from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.spark_session import build_spark
from src.common.config_io import iceberg_table_exists, write_iceberg


def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def load_config(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return _expand_env_vars(yaml.safe_load(f) or {})


def enabled_jobs(config: Dict[str, Any], selected: List[str] | None = None) -> List[Dict[str, Any]]:
    selected_set = set(selected or [])
    jobs = [j for j in config.get("jobs", []) if j.get("enabled", True)]
    return [j for j in jobs if not selected_set or j["name"] in selected_set]


def ensure_column(spark, table_ident: str, column_name: str, sql_type: str) -> None:
    if column_name in spark.table(table_ident).columns:
        return
    spark.sql(f"ALTER TABLE {table_ident} ADD COLUMN {column_name} {sql_type}")


def build_updates(job: Dict[str, Any], spark) -> DataFrame:
    fact = spark.table(job["fact_table"]).alias("f")
    lookup = spark.table(job["lookup_table"]).alias("d")

    if job.get("current_filter"):
        lookup = lookup.filter(F.expr(job["current_filter"]))

    fact_unknown = fact.filter(F.col(f"f.{job['unknown_fk_column']}") == F.lit(job.get("unknown_value", -1)))

    cond = []
    for item in job["lookup_keys"]:
        cond.append(F.col(f"f.{item['fact_column']}").eqNullSafe(F.col(f"d.{item['lookup_column']}")))

    joined = fact_unknown.join(F.broadcast(lookup), cond, "inner")
    fact_cols = [F.col(f"f.{c}").alias(c) for c in spark.table(job["fact_table"]).columns]
    updates = joined.select(*fact_cols, F.col(f"d.{job['lookup_value']}").alias("__resolved_fk_value"))

    if "_meta_reconciled_at" not in updates.columns:
        updates = updates.withColumn("_meta_reconciled_at", F.current_timestamp())
    else:
        updates = updates.withColumn("_meta_reconciled_at", F.current_timestamp())

    if "_meta_reconciled_by" not in updates.columns:
        updates = updates.withColumn("_meta_reconciled_by", F.lit(job["name"]))
    else:
        updates = updates.withColumn("_meta_reconciled_by", F.lit(job["name"]))

    updates = updates.withColumn(job["unknown_fk_column"], F.col("__resolved_fk_value").cast("long")).drop("__resolved_fk_value")
    return updates


def merge_updates(job: Dict[str, Any], updates: DataFrame) -> int:
    spark = updates.sparkSession
    target = job["fact_table"]
    count = updates.count()
    if count == 0:
        return 0

    ensure_column(spark, target, "_meta_reconciled_at", "timestamp")
    ensure_column(spark, target, "_meta_reconciled_by", "string")

    view = "_reconcile_updates"
    updates.createOrReplaceTempView(view)
    keys = job["fact_key"]
    on_clause = " AND ".join([f"t.{k} <=> s.{k}" for k in keys])
    update_cols = list(dict.fromkeys(job.get("update_columns", []) + ["_meta_reconciled_at", "_meta_reconciled_by"]))
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    spark.sql(f"MERGE INTO {target} t USING {view} s ON {on_clause} WHEN MATCHED THEN UPDATE SET {set_clause}")
    spark.catalog.dropTempView(view)
    return count


def write_audit(config: Dict[str, Any], spark, metrics: List[Dict[str, Any]]) -> None:
    audit_table = config.get("runtime", {}).get("audit_table", "lakehouse.audit.reconciliation_metrics")
    if not metrics:
        return
    df = spark.createDataFrame(metrics)
    write_iceberg(df, audit_table, write_mode="append")


def main() -> None:
    ap = argparse.ArgumentParser(description="Resolve late-arriving dimension keys for fact tables with unknown SK values")
    ap.add_argument("--config", default="metadata/reconciliation_jobs.yaml")
    ap.add_argument("--jobs", default="", help="Comma-separated reconciliation job names")
    args = ap.parse_args()

    config = load_config(args.config)
    selected = [x.strip() for x in args.jobs.split(",") if x.strip()]
    spark = build_spark(config.get("runtime", {}).get("app_name", "kappa-reconcile-unknown-fk"))
    metrics: List[Dict[str, Any]] = []

    try:
        for job in enabled_jobs(config, selected):
            started = time.time()
            if not iceberg_table_exists(spark, job["fact_table"]):
                metrics.append({"job": job["name"], "status": "fact_missing", "updated_rows": 0, "elapsed_seconds": 0.0})
                continue
            if not iceberg_table_exists(spark, job["lookup_table"]):
                metrics.append({"job": job["name"], "status": "lookup_missing", "updated_rows": 0, "elapsed_seconds": 0.0})
                continue
            updates = build_updates(job, spark)
            updated_rows = merge_updates(job, updates)
            metrics.append({
                "job": job["name"],
                "status": "success",
                "updated_rows": int(updated_rows),
                "elapsed_seconds": round(time.time() - started, 3),
                "fact_table": job["fact_table"],
                "lookup_table": job["lookup_table"],
                "ts_ms": int(time.time() * 1000),
            })
        write_audit(config, spark, metrics)
        print(json.dumps(metrics, indent=2, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
