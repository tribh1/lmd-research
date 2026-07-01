from __future__ import annotations

import argparse
import json
import time
import uuid
from typing import Iterable, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.spark_session import build_spark
from src.common.table_registry import RuntimeConfig, TableSpec, ModelSpec, export_registry_summary, load_registry
from src.common.config_audit import write_audit_event, write_quality_results
from src.common.config_io import read_source, table_ident, write_iceberg
from src.common.config_masking import apply_pii_masking
from src.common.config_quality import apply_quality_rules, summarize_rule_results
from src.common.config_transform import (
    add_audit_columns,
    apply_silver_transformations,
    apply_work_transformations,
    drop_audit_columns,
)
from src.common.metadata_provider import MetadataEmitter


TECHNICAL_COLUMNS = {
    "_table_name", "_layer", "_source_system", "_batch_id", "_ingest_ts", "_row_hash", "_dq_errors", "_dq_passed", "_quarantine_reason"
}


class ConfigDrivenPipeline:
    def __init__(self, cfg: RuntimeConfig, app_name: str = "config-driven-lakehouse-pipeline"):
        self.cfg = cfg
        self.spark = build_spark(app_name)
        self.metadata = MetadataEmitter(cfg)
        self.source_system = cfg.pipeline.get("default_source_system", "unknown")

    def close(self) -> None:
        self.spark.stop()

    def run_tables(self, tables: Iterable[TableSpec], stages: List[str]) -> None:
        for table in tables:
            batch_id = str(uuid.uuid4())
            try:
                if "raw" in stages:
                    self.run_raw(table, batch_id)
                if "work" in stages:
                    self.run_work(table, batch_id)
                if "silver" in stages:
                    self.run_silver(table, batch_id)
            except Exception as exc:
                write_audit_event(
                    self.spark,
                    self.cfg,
                    {
                        "batch_id": batch_id,
                        "table_name": table.name,
                        "stage": ",".join(stages),
                        "status": "FAILED",
                        "error_message": str(exc)[:2000],
                    },
                )
                if self.cfg.pipeline.get("on_error", "continue") == "fail_fast":
                    raise
                print(f"[WARN] table={table.name} failed: {exc}")

    def run_raw(self, table: TableSpec, batch_id: str) -> None:
        start = time.time()
        src = read_source(self.spark, self.cfg, table)
        row_count = src.count()
        raw_df = add_audit_columns(src, layer="raw", table_name=table.name, batch_id=batch_id, source_system=self.source_system)
        target = table_ident(self.cfg, "raw", table.target_table)
        write_iceberg(
            raw_df,
            target,
            partition_by=table.partition_by,
            write_mode=table.write_mode if table.write_mode in {"append", "overwrite"} else "append",
            primary_key=table.primary_key,
        )
        elapsed = time.time() - start
        self.metadata.register_table(self.cfg, table, "raw")
        self.metadata.emit_lineage(table.full_source_name, target, f"raw-ingest-{table.name}", batch_id)
        write_audit_event(
            self.spark,
            self.cfg,
            {
                "batch_id": batch_id,
                "table_name": table.name,
                "stage": "raw",
                "status": "SUCCESS",
                "source": table.full_source_name,
                "target": target,
                "row_count": row_count,
                "elapsed_sec": elapsed,
                "rows_per_sec": row_count / elapsed if elapsed else None,
            },
        )
        print(json.dumps({"table": table.name, "stage": "raw", "rows": row_count, "elapsed_sec": round(elapsed, 3)}))

    def run_work(self, table: TableSpec, batch_id: str) -> None:
        start = time.time()
        raw_ident = table_ident(self.cfg, "raw", table.target_table)
        work_ident = table_ident(self.cfg, "work", table.target_table)
        raw_df = self.spark.table(raw_ident)
        base = drop_audit_columns(raw_df)
        work_df = apply_work_transformations(base, table)
        work_df = add_audit_columns(work_df, layer="work", table_name=table.name, batch_id=batch_id, source_system=self.source_system)
        row_count = work_df.count()
        write_iceberg(
            work_df,
            work_ident,
            partition_by=table.partition_by,
            write_mode=table.write_mode,
            primary_key=table.primary_key,
        )
        elapsed = time.time() - start
        self.metadata.register_table(self.cfg, table, "work")
        self.metadata.emit_lineage(raw_ident, work_ident, f"work-transform-{table.name}", batch_id)
        write_audit_event(
            self.spark,
            self.cfg,
            {
                "batch_id": batch_id,
                "table_name": table.name,
                "stage": "work",
                "status": "SUCCESS",
                "source": raw_ident,
                "target": work_ident,
                "row_count": row_count,
                "elapsed_sec": elapsed,
                "rows_per_sec": row_count / elapsed if elapsed else None,
            },
        )
        print(json.dumps({"table": table.name, "stage": "work", "rows": row_count, "elapsed_sec": round(elapsed, 3)}))

    def run_silver(self, table: TableSpec, batch_id: str) -> None:
        start = time.time()
        work_ident = table_ident(self.cfg, "work", table.target_table)
        silver_ident = table_ident(self.cfg, "silver", table.target_table)
        quarantine_ident = table_ident(self.cfg, self.cfg.quarantine_namespace, f"{table.target_table}_failed")

        work_df = self.spark.table(work_ident)
        base = drop_audit_columns(work_df)
        base = apply_silver_transformations(base, table)
        base = add_audit_columns(base, layer="silver_precheck", table_name=table.name, batch_id=batch_id, source_system=self.source_system)

        rules = table.governance.get("dq_rules", []) or []
        passed, failed = apply_quality_rules(base, rules)
        quality_rows = summarize_rule_results(passed, failed, rules)
        for row in quality_rows:
            row.update({"batch_id": batch_id, "table_name": table.name, "target": silver_ident})
            self.metadata.emit_quality_result(silver_ident, row)
        if self.cfg.pipeline.get("write_audit_metrics", True):
            write_quality_results(self.spark, self.cfg, quality_rows)

        masked = apply_pii_masking(passed, table.governance.get("pii_columns", {}) or {})
        # Replace silver_precheck with final silver audit values after masking.
        silver_df = add_audit_columns(
            drop_audit_columns(masked),
            layer="silver",
            table_name=table.name,
            batch_id=batch_id,
            source_system=self.source_system,
        )
        silver_count = silver_df.count()
        failed_count = failed.count()
        write_iceberg(
            silver_df,
            silver_ident,
            partition_by=table.partition_by,
            write_mode=table.write_mode,
            primary_key=table.primary_key,
        )

        if failed_count > 0:
            failed_df = failed.withColumn("_quarantine_reason", F.concat_ws(",", F.col("_dq_errors")))
            write_iceberg(
                failed_df,
                quarantine_ident,
                partition_by=table.partition_by,
                write_mode="append",
                primary_key=None,
            )

        elapsed = time.time() - start
        self.metadata.register_table(self.cfg, table, "silver")
        self.metadata.emit_lineage(work_ident, silver_ident, f"silver-governance-{table.name}", batch_id)
        write_audit_event(
            self.spark,
            self.cfg,
            {
                "batch_id": batch_id,
                "table_name": table.name,
                "stage": "silver",
                "status": "SUCCESS",
                "source": work_ident,
                "target": silver_ident,
                "row_count": silver_count,
                "quarantined_rows": failed_count,
                "dq_rule_count": len(rules),
                "pii_column_count": len(table.governance.get("pii_columns", {}) or {}),
                "elapsed_sec": elapsed,
                "rows_per_sec": silver_count / elapsed if elapsed else None,
            },
        )
        print(json.dumps({"table": table.name, "stage": "silver", "rows": silver_count, "quarantined": failed_count, "elapsed_sec": round(elapsed, 3)}))

    def run_models(self, models: Iterable[ModelSpec]) -> None:
        for model in models:
            batch_id = str(uuid.uuid4())
            start = time.time()
            try:
                df = self.spark.sql(model.sql)
                target = table_ident(self.cfg, model.layer, model.name)
                count = df.count()
                write_iceberg(df, target, partition_by=model.partition_by, write_mode=model.write_mode)
                for upstream in model.upstream:
                    self.metadata.emit_lineage(f"{self.cfg.catalog}.{upstream}", target, f"model-{model.name}", batch_id)
                self.metadata.register_model(self.cfg, model)
                elapsed = time.time() - start
                write_audit_event(
                    self.spark,
                    self.cfg,
                    {
                        "batch_id": batch_id,
                        "table_name": model.name,
                        "stage": model.layer,
                        "status": "SUCCESS",
                        "source": ",".join(model.upstream),
                        "target": target,
                        "row_count": count,
                        "elapsed_sec": elapsed,
                        "rows_per_sec": count / elapsed if elapsed else None,
                    },
                )
                print(json.dumps({"model": model.name, "layer": model.layer, "rows": count, "elapsed_sec": round(elapsed, 3)}))
            except Exception as exc:
                write_audit_event(
                    self.spark,
                    self.cfg,
                    {
                        "batch_id": batch_id,
                        "table_name": model.name,
                        "stage": model.layer,
                        "status": "FAILED",
                        "error_message": str(exc)[:2000],
                    },
                )
                if self.cfg.pipeline.get("on_error", "continue") == "fail_fast":
                    raise
                print(f"[WARN] model={model.name} failed: {exc}")


def parse_csv(value: str | None) -> List[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def resolve_stages(stage_arg: str, cfg: RuntimeConfig) -> List[str]:
    if stage_arg == "all":
        return list(cfg.pipeline.get("default_stage_sequence", ["raw", "work", "silver"]))
    return parse_csv(stage_arg)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run metadata-configured Lakehouse pipelines")
    ap.add_argument("--config", default="metadata/config_driven_tables.yaml")
    ap.add_argument("--tables", default="", help="Comma-separated table names; empty means all enabled tables")
    ap.add_argument("--models", default="", help="Comma-separated model names; empty means all enabled models")
    ap.add_argument("--stage", default="all", help="raw|work|silver|raw,work,silver|all|models|summary")
    ap.add_argument("--dry-run", action="store_true", help="Validate and print registry summary without running Spark jobs")
    args = ap.parse_args()

    cfg = load_registry(args.config)
    selected_tables = parse_csv(args.tables)
    selected_models = parse_csv(args.models)

    if args.dry_run or args.stage == "summary":
        print(json.dumps(export_registry_summary(cfg), indent=2, ensure_ascii=False))
        return

    pipeline = ConfigDrivenPipeline(cfg)
    try:
        if args.stage != "models":
            stages = resolve_stages(args.stage, cfg)
            pipeline.run_tables(cfg.enabled_tables(selected_tables), stages)
        if args.stage in {"all", "models"}:
            pipeline.run_models(cfg.enabled_models(selected_models))
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
