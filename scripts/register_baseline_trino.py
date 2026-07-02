"""Register baseline Parquet directories as Trino external tables (hive catalog).

The baseline Data Lake has no automated catalog; this manual registration step
is itself part of the baseline definition (thesis Section 4.4: "ingestion writes
directly to layer directories and analytical access is provided by the same
Trino engine for fairness").

    python scripts/register_baseline_trino.py --config metadata/pipeline_config.yaml
"""
from __future__ import annotations

import argparse
import os
import yaml
import trino

TYPE_MAP = {
    "BIGINT": "bigint",
    "INT": "integer",
    "TEXT": "varchar",
    "TIMESTAMP": "timestamp",
    "BOOLEAN": "boolean",
    "DECIMAL": "decimal(18,2)",
    "DATE": "date",
}

BASELINE_LOCATION = os.getenv("BASELINE_S3_LOCATION", "s3a://lakehouse-baseline")


def connect():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8088")),
        user=os.getenv("TRINO_USER", "experiment"),
        catalog="hive",
        schema="baseline",
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    conn = connect()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS hive.baseline")
    cur.fetchall()

    for table, spec in cfg["tables"].items():
        cols = spec.get("columns", [])
        if not cols:
            print(f"skip {table}: no columns declared in config")
            continue
        col_ddl = ", ".join(f'"{c["name"]}" {TYPE_MAP[c["dataType"]]}' for c in cols)
        partitioned = bool(spec.get("partition_column"))
        if partitioned:
            col_ddl += ', "p_date" date'
        cur.execute(f"DROP TABLE IF EXISTS hive.baseline.{table}")
        cur.fetchall()
        props = [f"external_location = '{BASELINE_LOCATION}/{table}'", "format = 'PARQUET'"]
        if partitioned:
            props.append("partitioned_by = ARRAY['p_date']")
        cur.execute(f"CREATE TABLE hive.baseline.{table} ({col_ddl}) WITH ({', '.join(props)})")
        cur.fetchall()
        if partitioned:
            cur.execute(f"CALL hive.system.sync_partition_metadata('baseline', '{table}', 'FULL')")
            cur.fetchall()
        print(f"registered hive.baseline.{table}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
