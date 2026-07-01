from __future__ import annotations

import argparse
import uuid
from pyspark.sql import functions as F
from src.common.config import load_config
from src.common.spark_session import build_spark
from src.common.governance import add_audit_columns, apply_quality_rules, apply_pii_masking
from src.common.metadata_client import MetadataClient


def table_exists(spark, ident: str) -> bool:
    try:
        spark.table(ident).limit(1).count()
        return True
    except Exception:
        return False


def create_or_append(df, ident: str, partition_col: str | None = None):
    namespace = ".".join(ident.split(".")[:-1])
    spark = df.sparkSession
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    try:
        if partition_col and partition_col in df.columns:
            df.writeTo(ident).using("iceberg").partitionedBy(F.days(F.col(partition_col))).create()
        else:
            df.writeTo(ident).using("iceberg").create()
    except Exception:
        df.writeTo(ident).append()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--table", required=True)
    args = ap.parse_args()

    cfg = load_config(args.config)
    tcfg = cfg.tables[args.table]
    batch_id = str(uuid.uuid4())
    spark = build_spark(f"work-to-silver-{args.table}")
    om = MetadataClient(cfg.openmetadata_url)

    raw_ident = f"lakehouse.raw.{args.table}"
    raw = spark.table(raw_ident)

    work = add_audit_columns(raw.drop("_layer", "_batch_id", "_ingest_ts", "_row_hash", "_source_system"), "work", batch_id)
    create_or_append(work, f"lakehouse.work.{args.table}", tcfg.get("partition_column"))

    passed, failed = apply_quality_rules(work, tcfg.get("dq_rules", []))
    masked = apply_pii_masking(passed, tcfg.get("pii_columns", {}))
    silver = add_audit_columns(masked.drop("_layer", "_batch_id", "_ingest_ts", "_row_hash", "_source_system"), "silver", batch_id)
    create_or_append(silver, f"lakehouse.silver.{args.table}", tcfg.get("partition_column"))

    if failed.count() > 0:
        failed = failed.withColumn("_quarantine_reason", F.concat_ws(",", F.col("_dq_errors")))
        create_or_append(failed, f"lakehouse.quarantine.{args.table}_failed", tcfg.get("partition_column"))

    # Quality metric by rule
    for rule in tcfg.get("dq_rules", []):
        failed_count = failed.filter(F.array_contains(F.col("_dq_errors"), rule["rule_id"])).count()
        passed_count = passed.count()
        om.emit_quality_result(f"lakehouse.silver.{args.table}", rule["rule_id"], passed_count, failed_count, batch_id)

    # Audit run summary
    metrics = spark.createDataFrame([{
        "experiment": "E2",
        "table_name": args.table,
        "batch_id": batch_id,
        "source_rows": raw.count(),
        "passed_rows": passed.count(),
        "quarantined_rows": failed.count(),
    }]).withColumn("created_at", F.current_timestamp())
    create_or_append(metrics, "lakehouse.audit.governance_metrics", None)

    om.emit_lineage(raw_ident, f"lakehouse.work.{args.table}", f"work-transform-{args.table}", batch_id)
    om.emit_lineage(f"lakehouse.work.{args.table}", f"lakehouse.silver.{args.table}", f"silver-governance-{args.table}", batch_id)
    spark.stop()

if __name__ == "__main__":
    main()
