"""Ingest the injected-violation ground truth into the Raw tier for Experiment 2.

The generator seeds deliberate quality violations and records them in the
PostgreSQL table exp_ground_truth_violation; Experiment 2 compares quarantined
records against this ground truth (thesis Section 4.4, Table 4.4).

    spark-submit src/jobs/ingest_ground_truth.py --config metadata/pipeline_config.yaml
"""
from __future__ import annotations

import argparse

from src.common.config import load_config
from src.common.spark_session import build_spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = load_config(args.config)
    spark = build_spark("ingest-ground-truth")

    df = (spark.read.format("jdbc")
          .option("url", cfg.jdbc_url)
          .option("dbtable", "public.exp_ground_truth_violation")
          .option("user", cfg.jdbc_user)
          .option("password", cfg.jdbc_password)
          .option("driver", "org.postgresql.Driver")
          .load())
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")
    df.writeTo("lakehouse.raw.exp_ground_truth_violation").using("iceberg").createOrReplace()
    print({"ground_truth_violations": df.count()})
    spark.stop()


if __name__ == "__main__":
    main()
