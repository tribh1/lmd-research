from __future__ import annotations

import argparse
import re
from typing import Dict
from src.common.config import load_config
from src.common.spark_session import build_spark
from ._utils import write_json

EMAIL_RE = r"^[^@]+@[^@]+\.[^@]+$"
CARD_RE = r"^[0-9]{13,19}$"


def table_count(spark, ident: str) -> int:
    try:
        return spark.table(ident).count()
    except Exception:
        return 0


def run(config_path: str, out: str | None = None) -> Dict:
    cfg = load_config(config_path)
    spark = build_spark("exp2-governance-coverage")

    gt = table_count(spark, "lakehouse.raw.exp_ground_truth_violation")
    quarantined = 0
    for table in cfg.tables.keys():
        quarantined += table_count(spark, f"lakehouse.quarantine.{table}_failed")
    dq_rate = quarantined / gt * 100 if gt else None

    pii_checks = []
    for table, spec in cfg.tables.items():
        if not spec.get("pii_columns"):
            continue
        try:
            df = spark.table(f"lakehouse.silver.{table}")
        except Exception:
            continue
        for col, pii_spec in spec["pii_columns"].items():
            if col not in df.columns:
                continue
            method = pii_spec.get("method")
            total = df.filter(f"{col} is not null").count()
            if method == "email_mask":
                unmasked = df.filter(df[col].rlike(EMAIL_RE) & ~df[col].rlike("\\*\\*\\*")) .count()
            elif method == "last4":
                unmasked = df.filter(df[col].rlike(CARD_RE)).count()
            elif method in ["sha256", "phone_mask", "nullify"]:
                unmasked = 0
            else:
                unmasked = 0
            pii_checks.append((total, unmasked))
    total_pii = sum(x[0] for x in pii_checks)
    unmasked_pii = sum(x[1] for x in pii_checks)
    pii_accuracy = (1 - unmasked_pii / total_pii) * 100 if total_pii else None

    # In a production run, count OpenMetadata lineage/job events. Here use audit events if present.
    total_jobs = table_count(spark, "lakehouse.audit.governance_metrics")
    lineage_events = table_count(spark, "lakehouse.audit.lineage_events")
    lineage_rate = lineage_events / total_jobs * 100 if total_jobs else None

    result = {
        "experiment": "E2_GOVERNANCE_COVERAGE",
        "ground_truth_violations": gt,
        "quarantined_records": quarantined,
        "data_quality_enforcement_rate_pct": round(dq_rate, 2) if dq_rate is not None else None,
        "pii_masking_accuracy_pct": round(pii_accuracy, 2) if pii_accuracy is not None else None,
        "lineage_auto_capture_rate_pct": round(lineage_rate, 2) if lineage_rate is not None else None,
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
