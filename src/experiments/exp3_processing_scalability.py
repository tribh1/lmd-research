"""Experiment 3: Processing Performance and Scalability (thesis Table 4.7).

- batch throughput per scale in rows/s and MB/s, from the audit batch_metrics
  table (proposed) and results/baseline_metrics JSON files (baseline)
- streaming end-to-end latency p50/p95/p99 over the most recent five-minute
  window of events in lakehouse.raw.app_events
"""
from __future__ import annotations

import argparse
import glob
import json
import os
from typing import Dict
from pyspark.sql import functions as F
from src.common.spark_session import build_spark
from ._utils import write_json, pct

WINDOW_MINUTES = 5


def run(config_path: str, out: str | None = None,
        baseline_metrics_dir: str = "results/baseline_metrics") -> Dict:
    spark = build_spark("exp3-processing-scalability")
    result = {"experiment": "E3_PROCESSING_PERFORMANCE_SCALABILITY",
              "batch_proposed": {}, "batch_baseline": {}, "streaming": {}}

    try:
        batch = spark.table("lakehouse.audit.batch_metrics").collect()
        for row in batch:
            scale = row["scale"] or "unknown"
            entry = {"table": row["table_name"],
                     "rows_per_sec": row["rows_per_sec"],
                     "elapsed_sec": row["elapsed_sec"]}
            d = row.asDict()
            if "mb_per_sec" in d:
                entry["mb_per_sec"] = d["mb_per_sec"]
            result["batch_proposed"].setdefault(scale, []).append(entry)
    except Exception:
        result["batch_note"] = "No batch metrics table found. Run 01_batch_ingest_raw.py first."

    for path in glob.glob(os.path.join(baseline_metrics_dir, "*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                m = json.load(f)
            result["batch_baseline"].setdefault(m.get("scale", "unknown"), []).append({
                "table": m["table"], "rows_per_sec": m["rows_per_sec"],
                "mb_per_sec": m["mb_per_sec"], "elapsed_sec": m["elapsed_sec"]})
        except Exception:
            continue

    try:
        events = spark.table("lakehouse.raw.app_events")
        max_ts = events.agg(F.max("_ingest_ts")).collect()[0][0]
        window = events.filter(
            F.col("_ingest_ts") >= F.lit(max_ts) - F.expr(f"INTERVAL {WINDOW_MINUTES} MINUTES"))
        latencies = [float(r["_event_latency_ms"])
                     for r in window.select("_event_latency_ms").collect()
                     if r["_event_latency_ms"] is not None]
        result["streaming"] = {
            "window_minutes": WINDOW_MINUTES,
            "p50_ms": pct(latencies, 0.50),
            "p95_ms": pct(latencies, 0.95),
            "p99_ms": pct(latencies, 0.99),
            "sample_count": len(latencies),
        }
    except Exception:
        result["streaming_note"] = ("No streaming events table found. Run scripts/produce_events.py "
                                    "and 04_stream_events.py first.")

    spark.stop()
    if out:
        write_json(out, result)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out")
    ap.add_argument("--baseline-metrics-dir", default="results/baseline_metrics")
    args = ap.parse_args()
    print(run(args.config, args.out, args.baseline_metrics_dir))
