from __future__ import annotations

import argparse
import os
import re
import time
from typing import Dict, List
import trino
from ._utils import write_json, median

QUERIES = {
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


def connect():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8088")),
        user=os.getenv("TRINO_USER", "experiment"),
        catalog="iceberg",
        schema="silver",
    )


def run_query(cur, sql: str, repeats: int = 3) -> Dict:
    timings = []
    row_counts = []
    for _ in range(repeats):
        start = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        timings.append((time.time() - start) * 1000)
        row_counts.append(len(rows))
    return {"median_exec_ms": median(timings), "rows_returned": row_counts[-1] if row_counts else 0}


def run(config_path: str, out: str | None = None) -> Dict:
    conn = connect()
    cur = conn.cursor()
    result = {"experiment": "E4_QUERY_PERFORMANCE", "queries": {}}
    for name, sql in QUERIES.items():
        result["queries"][name] = run_query(cur, sql)
    cur.close()
    conn.close()
    result["scan_reduction_note"] = "Use Trino UI or EXPLAIN ANALYZE JSON to fill data_scanned_MB for proposed and baseline, then compute (baseline-proposed)/baseline*100."
    if out:
        write_json(out, result)
    return result

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out")
    args = ap.parse_args()
    print(run(args.config, args.out))
