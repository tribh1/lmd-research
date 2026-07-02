"""Baseline A: conventional Data Lake ingestion (thesis Section 4.4, Table 4.3a).

Reads the same PostgreSQL source tables and writes plain directory-partitioned
Parquet files to the lakehouse-baseline bucket. Deliberately omits everything
that distinguishes the proposed design: no Iceberg table format, no metadata
catalog registration, no quality validation, no PII masking, no lineage.
Throughput metrics are written as local JSON files (the baseline has no audit
tier by definition).

    spark-submit src/jobs/baseline_ingest.py --config metadata/pipeline_config.yaml \
        --table customers --scale 1gb
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.spark_session import build_spark
from src.common.fs_utils import path_size_bytes

BASELINE_ROOT = os.getenv("BASELINE_ROOT", "s3a://lakehouse-baseline")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--scale", default="unknown")
    ap.add_argument("--metrics-dir", default="results/baseline_metrics")
    args = ap.parse_args()

    cfg = load_config(args.config)
    tcfg = cfg.tables[args.table]
    spark = build_spark(f"baseline-ingest-{args.table}")

    start = time.time()
    df = (spark.read.format("jdbc")
          .option("url", cfg.jdbc_url)
          .option("dbtable", tcfg["source_table"])
          .option("user", cfg.jdbc_user)
          .option("password", cfg.jdbc_password)
          .option("driver", "org.postgresql.Driver")
          .load())
    count = df.count()

    target = f"{BASELINE_ROOT}/{args.table}"
    partition_col = tcfg.get("partition_column")
    if partition_col and partition_col in df.columns:
        df = df.withColumn("p_date", F.to_date(F.col(partition_col)))
        df.write.mode("overwrite").partitionBy("p_date").parquet(target)
    else:
        df.write.mode("overwrite").parquet(target)
    elapsed = time.time() - start

    size_bytes = path_size_bytes(spark, target)
    metrics = {
        "experiment": "E3_BASELINE",
        "configuration": "baseline_a_parquet_datalake",
        "table": args.table,
        "scale": args.scale,
        "row_count": count,
        "elapsed_sec": round(elapsed, 3),
        "rows_per_sec": round(count / elapsed, 2) if elapsed else None,
        "mb_per_sec": round(size_bytes / 1024 / 1024 / elapsed, 3) if elapsed else None,
        "written_bytes": size_bytes,
        "target": target,
    }
    os.makedirs(args.metrics_dir, exist_ok=True)
    out = os.path.join(args.metrics_dir, f"{args.table}_{args.scale}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print(metrics)
    spark.stop()


if __name__ == "__main__":
    main()
