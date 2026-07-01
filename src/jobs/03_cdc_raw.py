from __future__ import annotations

import argparse
import uuid
from pyspark.sql import functions as F
from src.common.config import load_config
from src.common.spark_session import build_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--topic", required=True, help="Example: pgsource.public.src_customer")
    ap.add_argument("--target", required=True, help="Example: customers_cdc")
    ap.add_argument("--checkpoint", default="s3a://lakehouse-audit/checkpoints/cdc")
    args = ap.parse_args()

    cfg = load_config(args.config)
    spark = build_spark(f"cdc-raw-{args.target}")
    batch_id = str(uuid.uuid4())

    raw_kafka = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load())

    parsed = raw_kafka.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("debezium_json"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("_ingest_ts"),
        F.lit(batch_id).alias("_batch_id")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")
    query = (parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{args.checkpoint}/{args.target}")
        .toTable(f"lakehouse.raw.{args.target}"))
    query.awaitTermination()

if __name__ == "__main__":
    main()
