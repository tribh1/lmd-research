from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List

# Allow running this script directly from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common.table_registry import export_registry_summary, load_registry  # noqa: E402


DEFAULT_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.postgresql:postgresql:42.7.3",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    ]
)


def csv(values: List[str]) -> str:
    return ",".join(values)


def build_command(config: str, stage: str, tables: List[str], models: List[str], spark_submit: str, packages: str) -> List[str]:
    cmd = [
        spark_submit,
        "--packages",
        packages,
        "src/jobs/config_driven_etl.py",
        "--config",
        config,
        "--stage",
        stage,
    ]
    if tables:
        cmd.extend(["--tables", csv(tables)])
    if models:
        cmd.extend(["--models", csv(models)])
    return cmd


def main() -> None:
    ap = argparse.ArgumentParser(description="Metadata-aware runner that submits generic Spark jobs based on YAML registry")
    ap.add_argument("--config", default="metadata/config_driven_tables.yaml")
    ap.add_argument("--tables", default="", help="Comma-separated tables. Empty = all enabled tables from config.")
    ap.add_argument("--models", default="", help="Comma-separated models. Empty = all enabled models from config.")
    ap.add_argument("--stage", default="all", help="raw|work|silver|all|models|summary")
    ap.add_argument("--spark-submit", default="spark-submit")
    ap.add_argument("--packages", default=DEFAULT_PACKAGES)
    ap.add_argument("--print-only", action="store_true", help="Only print generated spark-submit command")
    args = ap.parse_args()

    cfg = load_registry(args.config)
    tables = [x.strip() for x in args.tables.split(",") if x.strip()]
    models = [x.strip() for x in args.models.split(",") if x.strip()]

    if args.stage == "summary":
        print(json.dumps(export_registry_summary(cfg), indent=2, ensure_ascii=False))
        return

    cmd = build_command(args.config, args.stage, tables, models, args.spark_submit, args.packages)
    if args.print_only:
        print(" ".join(cmd))
        return

    print("[metadata-runner] submitting:", " ".join(cmd))
    completed = subprocess.run(cmd, check=False)
    sys.exit(completed.returncode)


if __name__ == "__main__":
    main()
