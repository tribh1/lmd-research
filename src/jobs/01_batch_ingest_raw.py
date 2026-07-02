from __future__ import annotations

import argparse
import time
import uuid
from pyspark.sql import functions as F
from src.common.audit import record_lineage_event
from src.common.config import load_config
from src.common.spark_session import build_spark
from src.common.governance import add_audit_columns
from src.common.metadata_client import MetadataClient


def read_source(spark, cfg, table_key: str):
    tcfg = cfg.tables[table_key]
    return (spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", tcfg["source_table"])
        .option("user", cfg.jdbc_user)
        .option("password", cfg.jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load())


def write_iceberg(df, namespace: str, table_name: str, partition_col: str | None):
    df.createOrReplaceTempView("staging_df")
    spark = df.sparkSession
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace}")
    if partition_col and partition_col in df.columns:
        writer = df.writeTo(f"lakehouse.{namespace}.{table_name}").using("iceberg").partitionedBy(F.days(F.col(partition_col)))
    else:
        writer = df.writeTo(f"lakehouse.{namespace}.{table_name}").using("iceberg")
    try:
        writer.create()
    except Exception:
        # Non-destructive schema evolution (Experiment 5): merge new columns on append.
        try:
            spark.sql(f"ALTER TABLE lakehouse.{namespace}.{table_name} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
        except Exception:
            pass
        df.writeTo(f"lakehouse.{namespace}.{table_name}").option("merge-schema", "true").append()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--scale", default="unknown")
    args = ap.parse_args()

    cfg = load_config(args.config)
    tcfg = cfg.tables[args.table]
    batch_id = str(uuid.uuid4())
    spark = build_spark(f"batch-ingest-raw-{args.table}")
    om = MetadataClient(cfg.openmetadata_url)

    start = time.time()
    df = read_source(spark, cfg, args.table)
    count = df.count()
    df = add_audit_columns(df, layer="raw", batch_id=batch_id)
    write_iceberg(df, "raw", args.table, tcfg.get("partition_column"))
    elapsed = time.time() - start

    # Bytes written to the Iceberg table for this batch, from the files metadata
    # table (thesis Section 2.7 requires MB/s in addition to rows/s).
    try:
        bytes_written = (spark.table(f"lakehouse.raw.{args.table}.files")
                         .agg(F.sum("file_size_in_bytes")).collect()[0][0]) or 0
    except Exception:
        bytes_written = 0

    metrics = spark.createDataFrame([{
        "experiment": "E3",
        "table_name": args.table,
        "scale": args.scale,
        "batch_id": batch_id,
        "row_count": count,
        "elapsed_sec": elapsed,
        "rows_per_sec": count / elapsed if elapsed else None,
        "bytes_written": int(bytes_written),
        "mb_per_sec": bytes_written / 1024 / 1024 / elapsed if elapsed else None,
        "created_at": None,
    }]).withColumn("created_at", F.current_timestamp())
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.audit")
    try:
        metrics.writeTo("lakehouse.audit.batch_metrics").using("iceberg").create()
    except Exception:
        metrics.writeTo("lakehouse.audit.batch_metrics").append()

    emitted = om.emit_lineage(tcfg["source_table"], f"lakehouse.raw.{args.table}", f"batch-ingest-{args.table}", batch_id)
    record_lineage_event(spark, tcfg["source_table"], f"lakehouse.raw.{args.table}",
                         f"batch-ingest-{args.table}", batch_id, emitted)
    print({"table": args.table, "rows": count, "elapsed_sec": elapsed,
           "rows_per_sec": count / elapsed if elapsed else None,
           "mb_per_sec": bytes_written / 1024 / 1024 / elapsed if elapsed else None})
    spark.stop()


if __name__ == "__main__":
    main()
