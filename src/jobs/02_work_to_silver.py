from __future__ import annotations

import argparse
import uuid
from pyspark.sql import functions as F
from src.common.audit import record_lineage_event
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
        # Non-destructive schema evolution (Experiment 5): let Iceberg merge new
        # columns instead of rejecting the append.
        try:
            spark.sql(f"ALTER TABLE {ident} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
        except Exception:
            pass
        df.writeTo(ident).option("merge-schema", "true").append()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--table", required=True)
    # Ablation switches (thesis Table 4.3a):
    # --skip-work-layer  -> "Proposed without Work Layer" variant
    # --no-governance    -> "Baseline B: Iceberg Lakehouse only" variant
    ap.add_argument("--skip-work-layer", action="store_true",
                    help="Ablation: transform Raw directly to Silver without the Work tier")
    ap.add_argument("--no-governance", action="store_true",
                    help="Ablation: disable quality validation, quarantine, and PII masking")
    args = ap.parse_args()

    cfg = load_config(args.config)
    tcfg = cfg.tables[args.table]
    batch_id = str(uuid.uuid4())
    variant = ("iceberg_only" if args.no_governance
               else "no_work_layer" if args.skip_work_layer else "full_mdl_eg")
    spark = build_spark(f"work-to-silver-{args.table}-{variant}")
    om = MetadataClient(cfg.openmetadata_url)

    raw_ident = f"lakehouse.raw.{args.table}"
    raw = spark.table(raw_ident)
    staged = raw.drop("_layer", "_batch_id", "_ingest_ts", "_row_hash", "_source_system")

    if args.skip_work_layer:
        work = add_audit_columns(staged, "silver", batch_id)
        silver_input_ident = raw_ident
    else:
        work = add_audit_columns(staged, "work", batch_id)
        create_or_append(work, f"lakehouse.work.{args.table}", tcfg.get("partition_column"))
        silver_input_ident = f"lakehouse.work.{args.table}"

    if args.no_governance:
        passed, failed = work, None
        masked = passed
    else:
        passed, failed = apply_quality_rules(work, tcfg.get("dq_rules", []))
        masked = apply_pii_masking(passed, tcfg.get("pii_columns", {}))

    silver = add_audit_columns(masked.drop("_layer", "_batch_id", "_ingest_ts", "_row_hash", "_source_system"), "silver", batch_id)
    create_or_append(silver, f"lakehouse.silver.{args.table}", tcfg.get("partition_column"))
    # Keep the catalog schema in sync with the written table (Experiment 5).
    om.update_table_schema("silver", args.table, silver.schema)

    failed_count = 0
    if failed is not None:
        failed_count = failed.count()
        if failed_count > 0:
            failed = failed.withColumn("_quarantine_reason", F.concat_ws(",", F.col("_dq_errors")))
            create_or_append(failed, f"lakehouse.quarantine.{args.table}_failed", tcfg.get("partition_column"))

    passed_count = passed.count()
    if not args.no_governance:
        for rule in tcfg.get("dq_rules", []):
            rule_failed = failed.filter(F.array_contains(F.col("_dq_errors"), rule["rule_id"])).count() if failed_count else 0
            om.emit_quality_result(f"lakehouse.silver.{args.table}", rule["rule_id"], passed_count, rule_failed, batch_id)

    metrics = spark.createDataFrame([{
        "experiment": "E2",
        "table_name": args.table,
        "batch_id": batch_id,
        "variant": variant,
        "source_rows": raw.count(),
        "passed_rows": passed_count,
        "quarantined_rows": failed_count,
    }]).withColumn("created_at", F.current_timestamp())
    create_or_append(metrics, "lakehouse.audit.governance_metrics", None)

    if not args.skip_work_layer:
        emitted = om.emit_lineage(raw_ident, f"lakehouse.work.{args.table}", f"work-transform-{args.table}", batch_id)
        record_lineage_event(spark, raw_ident, f"lakehouse.work.{args.table}",
                             f"work-transform-{args.table}", batch_id, emitted)
    emitted = om.emit_lineage(silver_input_ident, f"lakehouse.silver.{args.table}", f"silver-governance-{args.table}", batch_id)
    record_lineage_event(spark, silver_input_ident, f"lakehouse.silver.{args.table}",
                         f"silver-governance-{args.table}", batch_id, emitted)
    spark.stop()

if __name__ == "__main__":
    main()
