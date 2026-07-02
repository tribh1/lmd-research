"""Experiment 5: Schema Evolution (thesis Section 4.5, Table 4.9).

Measures, for a column added at the PostgreSQL source:
- schema evolution latency, decomposed per layer (raw, work, silver) and to
  OpenMetadata catalog visibility
- pipeline availability during evolution: proportion of continuous monitoring
  queries (silver + mart) completing successfully while the change propagates

The experiment applies the ALTER TABLE itself, re-runs the customers pipeline
in a background thread (01 -> 02 -> 05), and probes visibility concurrently.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import threading
import time
from typing import Dict, Optional
import psycopg2

from src.common.config import load_config
from src.common.spark_session import build_spark
from src.common.metadata_client import MetadataClient
from ._utils import write_json

NEW_COLUMN = "loyalty_tier"
PROBE_INTERVAL_SEC = 2
DEADLINE_SEC = 900

PIPELINE_JOBS = [
    ["src/jobs/01_batch_ingest_raw.py", "--table", "customers"],
    ["src/jobs/02_work_to_silver.py", "--table", "customers"],
    ["src/jobs/05_gold_mart.py"],
]


def run_pipeline(config_path: str, log: Dict) -> None:
    spark_submit = os.getenv("SPARK_SUBMIT", "spark-submit")
    for job in PIPELINE_JOBS:
        cmd = [spark_submit, job[0], "--config", config_path] + job[1:]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        log[job[0]] = proc.returncode
        if proc.returncode != 0:
            log[f"{job[0]}_stderr"] = proc.stderr[-2000:]
            return


def column_visible(spark, ident: str) -> bool:
    try:
        spark.catalog.refreshTable(ident)
    except Exception:
        pass
    try:
        return NEW_COLUMN in spark.table(ident).columns
    except Exception:
        return False


def om_column_visible(om: MetadataClient) -> bool:
    t = om.get_table("lakehouse.silver.customers", fields="columns")
    return bool(t and any(c["name"] == NEW_COLUMN for c in t.get("columns", [])))


def run(config_path: str, out: str | None = None) -> Dict:
    cfg = load_config(config_path)
    spark = build_spark("exp5-schema-evolution")
    om = MetadataClient(cfg.openmetadata_url)

    # 1. Apply the schema change at the source.
    dsn = cfg.jdbc_url.replace("jdbc:", "")
    conn = psycopg2.connect(dsn, user=cfg.jdbc_user, password=cfg.jdbc_password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE src_customer ADD COLUMN IF NOT EXISTS {NEW_COLUMN} TEXT DEFAULT 'STANDARD'")
    cur.close(); conn.close()
    t0 = time.time()

    # 2. Re-run the pipeline in the background while probing visibility.
    pipeline_log: Dict = {}
    worker = threading.Thread(target=run_pipeline, args=(config_path, pipeline_log), daemon=True)
    worker.start()

    visible_at: Dict[str, Optional[float]] = {"raw": None, "work": None, "silver": None, "openmetadata": None}
    probes_total = probes_ok = 0
    deadline = t0 + DEADLINE_SEC
    while time.time() < deadline:
        # Availability probe: monitoring queries against serving tables must keep
        # succeeding during the evolution (zero-downtime claim).
        probes_total += 1
        try:
            spark.sql("SELECT count(*) FROM lakehouse.silver.customers").collect()
            spark.sql("SELECT count(*) FROM lakehouse.mart.sales_dashboard").collect()
            probes_ok += 1
        except Exception:
            pass

        for layer, ident in [("raw", "lakehouse.raw.customers"),
                             ("work", "lakehouse.work.customers"),
                             ("silver", "lakehouse.silver.customers")]:
            if visible_at[layer] is None and column_visible(spark, ident):
                visible_at[layer] = time.time()
        if visible_at["openmetadata"] is None and om.available() and om_column_visible(om):
            visible_at["openmetadata"] = time.time()

        if all(visible_at[k] is not None for k in ("raw", "work", "silver")) and \
                (visible_at["openmetadata"] is not None or not om.available()) and \
                not worker.is_alive():
            break
        time.sleep(PROBE_INTERVAL_SEC)
    worker.join(timeout=5)

    latencies = {f"{layer}_latency_sec": round(ts - t0, 2) if ts else None
                 for layer, ts in visible_at.items()}
    total = max((ts for ts in visible_at.values() if ts), default=None)
    result = {
        "experiment": "E5_SCHEMA_EVOLUTION",
        "new_column": NEW_COLUMN,
        **latencies,
        "schema_evolution_latency_total_sec": round(total - t0, 2) if total else None,
        "pipeline_availability_pct": round(probes_ok / probes_total * 100, 2) if probes_total else None,
        "availability_probes": {"total": probes_total, "succeeded": probes_ok},
        "pipeline_exit_codes": pipeline_log,
        "baseline_note": ("Baseline requires manual re-definition of Parquet external tables and "
                          "pipeline shutdown during schema modification (Section 4.5.5)."),
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
