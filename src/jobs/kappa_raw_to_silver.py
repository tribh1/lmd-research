from __future__ import annotations

"""Dedicated Raw -> Work -> Silver processor.

This job supports the production-layered execution style. It reads already-ingested Raw Iceberg
records and applies the same metadata-driven transformation semantics used by the unified streaming job.

Typical use cases:
- replay Raw without re-consuming Kafka;
- backfill governance rules after metadata change;
- scale ingestion and Silver governance independently;
- retry Silver merge without duplicating Raw ingestion.
"""

import argparse
import json
from pathlib import Path
from typing import List

from pyspark.sql import functions as F

from src.common.kappa_layer_processor import KappaLayerProcessor
from src.common.kappa_openmetadata import OpenMetadataEmitter
from src.common.kappa_registry import KappaFlow, load_kappa_registry
from src.common.kappa_transform import ident
from src.common.spark_session import build_spark


def parse_csv(value: str | None) -> List[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def load_raw_window(spark, flow: KappaFlow, catalog: str, from_ts: str | None, to_ts: str | None, limit: int | None):
    raw_ident = ident(catalog, flow.raw_table)
    df = spark.table(raw_ident)
    if from_ts and "_meta_ingest_ts" in df.columns:
        df = df.filter(F.col("_meta_ingest_ts") >= F.lit(from_ts).cast("timestamp"))
    if to_ts and "_meta_ingest_ts" in df.columns:
        df = df.filter(F.col("_meta_ingest_ts") < F.lit(to_ts).cast("timestamp"))
    if limit and limit > 0:
        df = df.limit(limit)
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Run Raw -> Work -> Silver processor once")
    ap.add_argument("--config", default="metadata/kappa_flows.yaml")
    ap.add_argument("--flows", default="")
    ap.add_argument("--from-ts", default=None, help="Optional _meta_ingest_ts lower bound")
    ap.add_argument("--to-ts", default=None, help="Optional _meta_ingest_ts upper bound")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--batch-id", type=int, default=0)
    ap.add_argument("--results", default="results/raw_to_silver_results.json")
    args = ap.parse_args()

    registry = load_kappa_registry(args.config)
    spark = build_spark(f"{registry.runtime.app_name}-raw-to-silver")
    try:
        try:
            emitter = OpenMetadataEmitter.from_file()
        except Exception:
            emitter = None
        processor = KappaLayerProcessor(spark, registry, emitter)
        results = []
        for flow in registry.enabled_flows(parse_csv(args.flows)):
            raw_df = load_raw_window(
                spark,
                flow,
                registry.runtime.catalog,
                args.from_ts,
                args.to_ts,
                args.limit if args.limit > 0 else None,
            )
            metrics = processor.process_raw_to_silver(raw_df, flow, args.batch_id)
            metrics["execution_style"] = "raw_to_silver_once"
            results.append(metrics)
            print(json.dumps(metrics, ensure_ascii=False))

        out = Path(args.results)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
