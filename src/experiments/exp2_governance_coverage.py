"""Experiment 2: Governance Coverage (thesis Section 4.5, Table 4.6).

- data quality enforcement rate: injected violations (ground truth) that were
  detected and quarantined, matched per (record, rule) pair
- PII masking accuracy: PII-tagged column values correctly masked in Silver
- lineage auto-capture rate: pipeline runs (batch ids) with lineage evidence

Prerequisites: generate_data.py (with --violation-rate), load_csv_to_postgres.py,
ingest_ground_truth.py, and the 01/02 pipeline jobs.
"""
from __future__ import annotations

import argparse
from typing import Dict
from pyspark.sql import functions as F
from src.common.config import load_config
from src.common.spark_session import build_spark
from ._utils import write_json

EMAIL_RE = r"^[^@]+@[^@]+\.[^@]+$"
CARD_RE = r"^[0-9]{13,19}$"

# Maps ground-truth source_table values to pipeline table keys.
SOURCE_TO_TABLE = {
    "src_customer": "customers",
    "src_product": "products",
    "src_order": "orders",
    "src_order_item": "order_items",
    "src_payment": "payments",
}


def safe_table(spark, ident: str):
    try:
        return spark.table(ident)
    except Exception:
        return None


def run(config_path: str, out: str | None = None) -> Dict:
    cfg = load_config(config_path)
    spark = build_spark("exp2-governance-coverage")

    # ---------------------------------------------- data quality enforcement
    gt = safe_table(spark, "lakehouse.raw.exp_ground_truth_violation")
    gt_count = detected = None
    if gt is not None:
        gt_pairs = gt.select(
            F.col("source_table"), F.col("source_pk").cast("string").alias("pk"), F.col("rule_id")
        ).distinct()
        gt_count = gt_pairs.count()
        detected = 0
        for source_table, table in SOURCE_TO_TABLE.items():
            q = safe_table(spark, f"lakehouse.quarantine.{table}_failed")
            if q is None:
                continue
            pk_col = cfg.tables[table]["primary_key"][0]
            q_pairs = (q.select(F.col(pk_col).cast("string").alias("pk"),
                                F.explode("_dq_errors").alias("rule_id"))
                        .distinct()
                        .withColumn("source_table", F.lit(source_table)))
            detected += q_pairs.join(gt_pairs, ["source_table", "pk", "rule_id"], "inner").count()
    dq_rate = detected / gt_count * 100 if gt_count else None

    # ------------------------------------------------------ PII masking check
    pii_checks = []
    for table, spec in cfg.tables.items():
        if not spec.get("pii_columns"):
            continue
        df = safe_table(spark, f"lakehouse.silver.{table}")
        if df is None:
            continue
        for col, pii_spec in spec["pii_columns"].items():
            if col not in df.columns:
                continue
            method = pii_spec.get("method")
            total = df.filter(F.col(col).isNotNull()).count()
            if method == "email_mask":
                unmasked = df.filter(F.col(col).rlike(EMAIL_RE) & ~F.col(col).rlike("\\*\\*\\*")).count()
            elif method == "last4":
                unmasked = df.filter(F.col(col).rlike(CARD_RE)).count()
            elif method == "sha256":
                unmasked = df.filter(~F.col(col).rlike(r"^[0-9a-f]{64}$") & F.col(col).isNotNull()).count()
            elif method == "phone_mask":
                unmasked = df.filter(~F.col(col).startswith("***") & F.col(col).isNotNull()).count()
            elif method == "nullify":
                unmasked = df.filter(F.col(col).isNotNull()).count()
                total = df.count()
            else:
                unmasked = 0
            pii_checks.append({"table": table, "column": col, "method": method,
                               "total": total, "unmasked": unmasked})
    total_pii = sum(c["total"] for c in pii_checks)
    unmasked_pii = sum(c["unmasked"] for c in pii_checks)
    pii_accuracy = (1 - unmasked_pii / total_pii) * 100 if total_pii else None

    # ------------------------------------------------ lineage auto-capture rate
    # A pipeline run is a distinct batch_id in the run-metric audit tables; it is
    # covered when the same batch_id appears in the lineage evidence table.
    runs = None
    batch = safe_table(spark, "lakehouse.audit.batch_metrics")
    gov = safe_table(spark, "lakehouse.audit.governance_metrics")
    parts = [df.select("batch_id") for df in (batch, gov) if df is not None]
    if parts:
        runs = parts[0]
        for p in parts[1:]:
            runs = runs.unionByName(p)
        runs = runs.distinct()
    lineage_rate = None
    total_runs = covered_runs = None
    lineage = safe_table(spark, "lakehouse.audit.lineage_events")
    if runs is not None:
        total_runs = runs.count()
        if lineage is not None and total_runs:
            covered_runs = runs.join(lineage.select("batch_id").distinct(), "batch_id", "inner").count()
            lineage_rate = covered_runs / total_runs * 100

    result = {
        "experiment": "E2_GOVERNANCE_COVERAGE",
        "ground_truth_violations": gt_count,
        "detected_and_quarantined": detected,
        "data_quality_enforcement_rate_pct": round(dq_rate, 2) if dq_rate is not None else None,
        "pii_masking_accuracy_pct": round(pii_accuracy, 2) if pii_accuracy is not None else None,
        "pii_detail": pii_checks,
        "pipeline_runs": total_runs,
        "runs_with_lineage": covered_runs,
        "lineage_auto_capture_rate_pct": round(lineage_rate, 2) if lineage_rate is not None else None,
        "baseline_note": "Baseline has no inline enforcement point: DQ rate 0%, no masking guarantee, no lineage (Section 4.5.2).",
    }
    spark.stop()
    if out:
        write_json(out, result)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out")
    args = ap.parse_args()
    print(run(args.config, args.out))
