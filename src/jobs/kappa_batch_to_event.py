from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras
import yaml
from confluent_kafka import Producer

from src.common.kappa_event_envelope import (
    canonical_json,
    key_from_columns,
    make_batch_run_id,
    make_debezium_compatible_snapshot_event,
)


def load_yaml(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def enabled_jobs(config: Dict[str, Any], selected: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    selected_set = {x.strip() for x in selected or [] if x.strip()}
    jobs = [x for x in config.get("batch_sources", []) if x.get("enabled", True)]
    if selected_set:
        jobs = [x for x in jobs if x["name"] in selected_set]
    return jobs


def resolve_connection(config: Dict[str, Any], name: str) -> Dict[str, Any]:
    conn = config.get("connections", {}).get(name)
    if not conn:
        raise ValueError(f"Connection not found: {name}")
    return conn


def make_query(job: Dict[str, Any], from_ts: Optional[str], to_ts: Optional[str]) -> tuple[str, Optional[Dict[str, Any]]]:
    source = job["source"]
    if source.get("query_template"):
        if not from_ts or not to_ts:
            raise ValueError(f"Job {job['name']} requires --from-ts and --to-ts")
        return source["query_template"], {"from_ts": from_ts, "to_ts": to_ts}
    if source.get("query"):
        return source["query"], None
    table = source["table"]
    schema = source.get("schema", "public")
    return f"SELECT * FROM {schema}.{table}", None


def publish_job(
    *,
    config: Dict[str, Any],
    job: Dict[str, Any],
    batch_run_id: Optional[str],
    from_ts: Optional[str],
    to_ts: Optional[str],
    dry_run: bool,
) -> Dict[str, Any]:
    runtime = config.get("runtime", {})
    source = job["source"]
    event = job["event"]
    extraction = job.get("extraction", {})

    source_conn = resolve_connection(config, source["connection"])
    kafka_conn = resolve_connection(config, event["connection"])

    dsn = os.path.expandvars(source_conn["dsn"])
    topic = event["topic"]
    key_columns = event.get("key_columns", [])
    op = event.get("op", runtime.get("default_snapshot_op", "r"))
    source_database = source.get("database", "unknown")
    source_schema = source.get("schema", "public")
    source_table = event.get("source_table") or source.get("table")
    batch_size = int(extraction.get("batch_size", runtime.get("default_batch_size", 5000)))
    run_id = make_batch_run_id(
        runtime.get("batch_run_id_prefix", "batch_snapshot"),
        job["name"],
        batch_run_id,
    )

    query, params = make_query(job, from_ts, to_ts)

    producer = None
    if not dry_run:
        producer = Producer({"bootstrap.servers": kafka_conn["bootstrap_servers"]})

    sent = 0
    started = time.time()

    with psycopg2.connect(dsn) as conn:
        with conn.cursor(name=f"cur_{job['name']}", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.itersize = batch_size
            cur.execute(query, params)

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                for raw_row in rows:
                    row = dict(raw_row)
                    key = key_from_columns(row, key_columns)
                    envelope = make_debezium_compatible_snapshot_event(
                        row=row,
                        op=op,
                        source_system=runtime.get("source_system", "postgres"),
                        source_database=source_database,
                        source_schema=source_schema,
                        source_table=source_table,
                        topic=topic,
                        key_columns=key_columns,
                        batch_run_id=run_id,
                        config_version=str(runtime.get("config_version", config.get("metadata_version", "unknown"))),
                        extraction_mode=job.get("mode", "snapshot_to_kafka"),
                    )

                    if dry_run:
                        if sent < 3:
                            print(canonical_json({"topic": topic, "key": key, "value": envelope}))
                    else:
                        producer.produce(
                            topic,
                            key=key.encode("utf-8"),
                            value=canonical_json(envelope).encode("utf-8"),
                        )
                        if sent % 1000 == 0:
                            producer.poll(0)
                    sent += 1

                if producer:
                    producer.flush(10)

    if producer:
        producer.flush(30)

    return {
        "job": job["name"],
        "flow_name": job.get("flow_name"),
        "topic": topic,
        "batch_run_id": run_id,
        "rows_published": sent,
        "elapsed_seconds": round(time.time() - started, 3),
        "dry_run": dry_run,
    }


def parse_csv(value: str | None) -> List[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish batch snapshot/backfill data as Debezium-compatible Kafka events")
    ap.add_argument("--config", default="metadata/kappa_batch_sources.yaml")
    ap.add_argument("--jobs", default="", help="Comma-separated batch source names")
    ap.add_argument("--batch-run-id", default=None)
    ap.add_argument("--from-ts", default=None)
    ap.add_argument("--to-ts", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    config = load_yaml(args.config)
    jobs = enabled_jobs(config, parse_csv(args.jobs))
    if not jobs:
        raise SystemExit("No enabled batch jobs selected")

    results = []
    for job in jobs:
        result = publish_job(
            config=config,
            job=job,
            batch_run_id=args.batch_run_id,
            from_ts=args.from_ts,
            to_ts=args.to_ts,
            dry_run=args.dry_run,
        )
        results.append(result)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    print(json.dumps({"summary": results}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
