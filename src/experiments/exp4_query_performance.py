"""Experiment 4: Query Performance (thesis Section 4.5, Table 4.8).

Runs the three analytical query classes against both configurations on the same
Trino engine:
- proposed: Iceberg catalog (silver/gold tiers, pre-aggregated KPIs)
- baseline: hive catalog over plain partitioned Parquet (KPIs computed on the fly)

Each query executes `--repeats` times (default 3); the median execution time and
the physical input bytes (data scanned) are reported, and the scan reduction
ratio is derived per query class. Baseline tables must be registered first via
scripts/register_baseline_trino.py.
"""
from __future__ import annotations

import argparse
import os
import time
from typing import Dict, Optional
import requests
import trino

from ._utils import write_json, median

PROPOSED_QUERIES = {
    "single_layer_silver_scan": """
        SELECT channel, count(*) AS order_count, sum(total_amount) AS revenue
        FROM iceberg.silver.orders
        WHERE order_date >= current_date - INTERVAL '90' DAY
        GROUP BY channel
    """,
    "cross_layer_gold_silver_join": """
        SELECT d.business_date, d.channel, d.gross_revenue, count(c.customer_id) AS customer_count
        FROM iceberg.gold.daily_revenue_kpi d
        LEFT JOIN iceberg.silver.customers c ON c.province IS NOT NULL
        WHERE d.business_date >= current_date - INTERVAL '90' DAY
        GROUP BY d.business_date, d.channel, d.gross_revenue
    """,
    "gold_aggregation": """
        SELECT category, sum(qty_sold) AS qty_sold, sum(net_sales) AS net_sales
        FROM iceberg.gold.product_sales_kpi
        GROUP BY category
    """,
}

# Logically equivalent workload for the baseline Data Lake: no silver/gold tiers
# exist, so conformed scans hit the raw Parquet and KPIs are computed on the fly.
BASELINE_QUERIES = {
    "single_layer_silver_scan": """
        SELECT channel, count(*) AS order_count, sum(total_amount) AS revenue
        FROM hive.baseline.orders
        WHERE p_date >= current_date - INTERVAL '90' DAY
        GROUP BY channel
    """,
    "cross_layer_gold_silver_join": """
        WITH daily_revenue AS (
            SELECT date(o.order_date) AS business_date, o.channel,
                   sum(o.total_amount) AS gross_revenue
            FROM hive.baseline.orders o
            LEFT JOIN hive.baseline.payments p ON p.order_id = o.order_id
            WHERE o.p_date >= current_date - INTERVAL '90' DAY
            GROUP BY date(o.order_date), o.channel
        )
        SELECT d.business_date, d.channel, d.gross_revenue, count(c.customer_id) AS customer_count
        FROM daily_revenue d
        LEFT JOIN hive.baseline.customers c ON c.province IS NOT NULL
        GROUP BY d.business_date, d.channel, d.gross_revenue
    """,
    "gold_aggregation": """
        SELECT pr.category, sum(i.quantity) AS qty_sold, sum(i.line_amount) AS net_sales
        FROM hive.baseline.order_items i
        LEFT JOIN hive.baseline.products pr ON pr.product_id = i.product_id
        GROUP BY pr.category
    """,
}


def connect(catalog: str, schema: str):
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8088")),
        user=os.getenv("TRINO_USER", "experiment"),
        catalog=catalog,
        schema=schema,
    )


def _scanned_bytes_from_rest(query_id: str) -> Optional[int]:
    """Fallback: read queryStats.physicalInputDataSize from the Trino REST API."""
    try:
        host = os.getenv("TRINO_HOST", "localhost")
        port = os.getenv("TRINO_PORT", "8088")
        user = os.getenv("TRINO_USER", "experiment")
        r = requests.get(f"http://{host}:{port}/v1/query/{query_id}",
                         headers={"X-Trino-User": user}, timeout=10)
        if not r.ok:
            return None
        size = r.json().get("queryStats", {}).get("physicalInputDataSize", "")
        units = {"B": 1, "kB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
        for unit, factor in sorted(units.items(), key=lambda x: -len(x[0])):
            if size.endswith(unit):
                return int(float(size[: -len(unit)]) * factor)
    except Exception:
        pass
    return None


def run_query(cur, sql: str, repeats: int) -> Dict:
    timings, scanned = [], []
    rows_returned = 0
    for _ in range(repeats):
        start = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        timings.append((time.time() - start) * 1000)
        rows_returned = len(rows)
        stats = cur.stats or {}
        b = stats.get("physicalInputBytes")
        if b in (None, 0):
            qid = stats.get("queryId") or getattr(cur, "query_id", None)
            b = _scanned_bytes_from_rest(qid) if qid else None
        if b is not None:
            scanned.append(b)
    return {
        "median_exec_ms": round(median(timings), 2),
        "data_scanned_mb": round(median(scanned) / 1024 / 1024, 3) if scanned else None,
        "rows_returned": rows_returned,
    }


def run(config_path: str, out: str | None = None, repeats: int = 3) -> Dict:
    result = {"experiment": "E4_QUERY_PERFORMANCE", "repeats": repeats,
              "proposed": {}, "baseline": {}, "scan_reduction_pct": {}}

    conn = connect("iceberg", "silver")
    cur = conn.cursor()
    for name, sql in PROPOSED_QUERIES.items():
        result["proposed"][name] = run_query(cur, sql, repeats)
    cur.close(); conn.close()

    try:
        conn = connect("hive", "baseline")
        cur = conn.cursor()
        for name, sql in BASELINE_QUERIES.items():
            result["baseline"][name] = run_query(cur, sql, repeats)
        cur.close(); conn.close()
    except Exception as ex:
        result["baseline_error"] = (f"Baseline query failed ({ex}). Run baseline_ingest.py and "
                                    "scripts/register_baseline_trino.py first.")

    for name in PROPOSED_QUERIES:
        p = result["proposed"].get(name, {}).get("data_scanned_mb")
        b = result["baseline"].get(name, {}).get("data_scanned_mb")
        if p is not None and b:
            result["scan_reduction_pct"][name] = round((b - p) / b * 100, 2)

    if out:
        write_json(out, result)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out")
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()
    print(run(args.config, args.out, args.repeats))
