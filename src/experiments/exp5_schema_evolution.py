from __future__ import annotations

import argparse
import time
from typing import Dict
import psycopg2
from src.common.config import load_config
from src.common.spark_session import build_spark
from ._utils import write_json


def run(config_path: str, out: str | None = None) -> Dict:
    cfg = load_config(config_path)
    spark = build_spark("exp5-schema-evolution")
    start = time.time()
    # 1. Apply schema change at source.
    conn = psycopg2.connect(cfg.jdbc_url.replace("jdbc:", ""), user=cfg.jdbc_user, password=cfg.jdbc_password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("ALTER TABLE src_customer ADD COLUMN IF NOT EXISTS loyalty_tier TEXT DEFAULT 'STANDARD'")
    cur.close(); conn.close()
    source_changed_at = time.time()

    # 2. In a real run, execute batch/CDC pipeline immediately after change.
    # This script measures visibility after the pipeline has been re-run.
    deadline = time.time() + 600
    available_count = 0
    total_probe = 0
    silver_visible_at = None
    while time.time() < deadline:
        total_probe += 1
        try:
            cols = spark.table("lakehouse.silver.customers").columns
            spark.sql("SELECT count(*) FROM lakehouse.mart.sales_dashboard").collect()
            available_count += 1
            if "loyalty_tier" in cols:
                silver_visible_at = time.time()
                break
        except Exception:
            pass
        time.sleep(5)
    availability = available_count / total_probe * 100 if total_probe else 0
    result = {
        "experiment": "E5_SCHEMA_EVOLUTION",
        "source_change_to_probe_start_sec": round(source_changed_at - start, 3),
        "schema_evolution_latency_total_sec": round((silver_visible_at or time.time()) - start, 3) if silver_visible_at else None,
        "pipeline_availability_pct": round(availability, 2),
        "note": "Run 01_batch_ingest_raw.py and 02_work_to_silver.py for customers after ALTER TABLE to propagate the new column."
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
