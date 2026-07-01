from __future__ import annotations

import argparse
import uuid
from pyspark.sql import functions as F
from src.common.config import load_config
from src.common.spark_session import build_spark

EVENT_SCHEMA = "event_id STRING, event_time TIMESTAMP, customer_id LONG, session_id STRING, event_type STRING, channel STRING, page STRING, amount DOUBLE"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--topic", default="app-events")
    ap.add_argument("--checkpoint", default="s3a://lakehouse-audit/checkpoints/app-events")
    args = ap.parse_args()
    cfg = load_config(args.config)
    spark = build_spark("stream-app-events")
    batch_id = str(uuid.uuid4())

    kafka = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load())

    events = kafka.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        F.col("timestamp").alias("kafka_ts")
    ).select("e.*", "kafka_ts") \
     .withColumn("_ingest_ts", F.current_timestamp()) \
     .withColumn("_event_latency_ms", (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("kafka_ts"))) * 1000) \
     .withColumn("_batch_id", F.lit(batch_id))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")
    query = (events.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .toTable("lakehouse.raw.app_events"))
    query.awaitTermination()

if __name__ == "__main__":
    main()
