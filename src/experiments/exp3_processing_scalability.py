from __future__ import annotations

import argparse
from typing import Dict
from src.common.spark_session import build_spark
from ._utils import write_json, pct


def run(config_path: str, out: str | None = None) -> Dict:
    spark = build_spark("exp3-processing-scalability")
    result = {"experiment": "E3_PROCESSING_PERFORMANCE_SCALABILITY", "batch": {}, "streaming": {}}
    try:
        batch = spark.table("lakehouse.audit.batch_metrics").collect()
        for row in batch:
            scale = row["scale"] or "unknown"
            result["batch"].setdefault(scale, [])
            result["batch"][scale].append({"table": row["table_name"], "rows_per_sec": row["rows_per_sec"], "elapsed_sec": row["elapsed_sec"]})
    except Exception:
        result["batch_note"] = "No batch metrics table found. Run 01_batch_ingest_raw.py first."

    try:
        latencies = [float(r["_event_latency_ms"]) for r in spark.table("lakehouse.raw.app_events").select("_event_latency_ms").collect()]
        result["streaming"] = {"p50_ms": pct(latencies, 0.50), "p95_ms": pct(latencies, 0.95), "p99_ms": pct(latencies, 0.99), "sample_count": len(latencies)}
    except Exception:
        result["streaming_note"] = "No streaming events table found. Run 04_stream_events.py first."

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
