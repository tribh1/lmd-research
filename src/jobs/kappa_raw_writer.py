from __future__ import annotations

"""Dedicated Raw Writer entrypoint.

Use this when you want to separate Kafka ingestion from downstream governance processing:
Kafka/Debezium/batch-as-event -> Raw Iceberg only.
"""

import argparse

from src.jobs.kappa_config_pipeline import KappaConfigPipeline, parse_csv
from src.common.kappa_registry import load_kappa_registry


def main() -> None:
    ap = argparse.ArgumentParser(description="Run Kappa Raw Writer only")
    ap.add_argument("--config", default="metadata/kappa_flows.yaml")
    ap.add_argument("--flows", default="")
    ap.add_argument("--once", action="store_true", help="Process currently available Kafka offsets and stop")
    args = ap.parse_args()

    registry = load_kappa_registry(args.config)
    pipeline = KappaConfigPipeline(registry)
    try:
        pipeline.start_flows(registry.enabled_flows(parse_csv(args.flows)), raw_only=True, once=args.once)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
