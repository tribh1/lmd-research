from __future__ import annotations

"""Dedicated Work -> Silver/Quarantine processor.

This job enforces the v7 layered execution rule: governance, DQ, PII masking,
quarantine routing, SCD merge, and fact upsert are executed only after Work has
already been materialized.
"""

import argparse
import json
from pathlib import Path
from typing import List

from pyspark.sql import functions as F

from src.common.kappa_layer_processor import KappaLayerProcessor
from src.common.kappa_openmetadata import OpenMetadataEmitter
from src.common.kappa_registry import load_kappa_registry
from src.common.kappa_transform import ident
from src.common.spark_session import build_spark


def parse_csv(value: str | None) -> List[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def load_work_window(spark, table_ident: str, from_ts: str | None, to_ts: str | None, limit: int | None):
    df = spark.table(table_ident)
    # Prefer Work materialization timestamp when available; fall back to ingest timestamp inherited from Raw.
    ts_col = "_meta_work_ts" if "_meta_work_ts" in df.columns else "_meta_ingest_ts"
    if from_ts and ts_col in df.columns:
        df = df.filter(F.col(ts_col) >= F.lit(from_ts).cast("timestamp"))
    if to_ts and ts_col in df.columns:
        df = df.filter(F.col(ts_col) < F.lit(to_ts).cast("timestamp"))
    if limit and limit > 0:
        df = df.limit(limit)
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Run Work -> Silver/Quarantine processor once")
    ap.add_argument("--config", default="metadata/kappa_flows.yaml")
    ap.add_argument("--flows", default="")
    ap.add_argument("--from-ts", default=None, help="Optional Work timestamp lower bound")
    ap.add_argument("--to-ts", default=None, help="Optional Work timestamp upper bound")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--batch-id", type=int, default=0)
    ap.add_argument("--results", default="results/work_to_silver_results.json")
    args = ap.parse_args()

    registry = load_kappa_registry(args.config)
    spark = build_spark(f"{registry.runtime.app_name}-work-to-silver")
    try:
        try:
            emitter = OpenMetadataEmitter.from_file()
        except Exception:
            emitter = None
        processor = KappaLayerProcessor(spark, registry, emitter)
        results = []
        for flow in registry.enabled_flows(parse_csv(args.flows)):
            work_ident = ident(registry.runtime.catalog, flow.work_table)
            work_df = load_work_window(
                spark,
                work_ident,
                args.from_ts,
                args.to_ts,
                args.limit if args.limit > 0 else None,
            )
            metrics = processor.process_work_to_silver(work_df, flow, args.batch_id)
            metrics["execution_style"] = "strict_layered"
            results.append(metrics)
            print(json.dumps(metrics, ensure_ascii=False))

        out = Path(args.results)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
