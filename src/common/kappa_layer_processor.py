from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config_io import write_iceberg
from src.common.config_masking import apply_pii_masking
from src.common.kappa_merge import write_silver_configured
from src.common.kappa_openmetadata import OpenMetadataEmitter
from src.common.kappa_quality import apply_kappa_dq
from src.common.kappa_registry import KappaFlow, KappaRegistry
from src.common.kappa_transform import (
    add_scd_columns,
    add_surrogate_key,
    apply_business_logic,
    apply_standardization,
    ident,
    resolve_foreign_keys,
    select_final_columns,
)


def safe_count(df: DataFrame) -> int:
    try:
        return int(df.count())
    except Exception:
        return -1


class KappaLayerProcessor:
    """
    Reusable layer processor used by strictly separated physical jobs.

    v7 design rule:
    - Kafka -> Raw is one job.
    - Raw -> Work is one job.
    - Work -> Silver/Quarantine is one job.
    - Silver -> Gold and Gold -> Mart are separate jobs.

    The class is shared to avoid duplicated transformation semantics while still enforcing
    separate physical jobs for each layer transition.
    """

    def __init__(
        self,
        spark: SparkSession,
        registry: KappaRegistry,
        metadata_emitter: Optional[OpenMetadataEmitter] = None,
    ):
        self.spark = spark
        self.registry = registry
        self.metadata_emitter = metadata_emitter

    def table_idents(self, flow: KappaFlow) -> Dict[str, str]:
        catalog = self.registry.runtime.catalog
        return {
            "raw": ident(catalog, flow.raw_table),
            "work": ident(catalog, flow.work_table),
            "silver": ident(catalog, flow.silver_table),
            "quarantine": ident(catalog, flow.quarantine_table),
            "audit": f"{catalog}.{self.registry.runtime.audit_namespace}.kappa_batch_metrics",
        }

    def write_raw(self, raw_df: DataFrame, flow: KappaFlow, batch_id: int) -> DataFrame:
        ids = self.table_idents(flow)
        out = (
            raw_df
            .withColumn("_meta_micro_batch_id", F.lit(int(batch_id)))
            .withColumn("_meta_layer", F.lit("raw"))
        )
        write_iceberg(out, ids["raw"], partition_by=flow.partition_by, write_mode="append")

        if self.metadata_emitter:
            self.metadata_emitter.register_flow_assets_and_lineage(flow, self.registry)

        return out

    def raw_to_work(self, raw_df: DataFrame, flow: KappaFlow) -> DataFrame:
        work_df = apply_standardization(raw_df, flow, self.registry)
        work_df = apply_business_logic(work_df, flow)
        work_df = add_surrogate_key(work_df, flow)
        work_df = add_scd_columns(work_df, flow)
        if flow.foreign_keys:
            work_df = resolve_foreign_keys(work_df, flow)
        return (
            work_df
            .withColumn("_meta_layer", F.lit("work"))
            .withColumn("_meta_work_ts", F.current_timestamp())
        )

    def work_to_silver(self, work_df: DataFrame, flow: KappaFlow) -> Tuple[DataFrame, DataFrame, List[Dict[str, Any]]]:
        rules = (flow.data_quality or {}).get("rules", []) or []
        passed, failed, dq_metrics = apply_kappa_dq(work_df, rules)

        silver_df = apply_pii_masking(passed, flow.pii_policy or {})
        silver_df = silver_df.withColumn("_meta_layer", F.lit("silver"))
        silver_df = select_final_columns(silver_df, flow)
        return silver_df, failed, dq_metrics


    def process_raw_to_work(
        self,
        raw_df: DataFrame,
        flow: KappaFlow,
        batch_id: int,
        *,
        write_work: bool = True,
        emit_metrics: bool = True,
    ) -> Dict[str, Any]:
        """Process one layer transition only: Raw -> Work."""
        # Deprecated compatibility method. v7 uses process_raw_to_work() and
        # process_work_to_silver() in separate jobs.
        if raw_df.rdd.isEmpty():
            return {
                "flow_name": flow.name,
                "batch_id": int(batch_id),
                "source_layer": "raw",
                "target_layer": "work",
                "input_rows": 0,
                "work_rows": 0,
            }

        ids = self.table_idents(flow)
        work_df = self.raw_to_work(raw_df, flow)

        if write_work:
            write_iceberg(work_df, ids["work"], partition_by=flow.partition_by, write_mode="append")

        metrics = {
            "flow_name": flow.name,
            "batch_id": int(batch_id),
            "source_layer": "raw",
            "target_layer": "work",
            "target_table": ids["work"],
            "input_rows": safe_count(raw_df),
            "work_rows": safe_count(work_df),
        }

        if emit_metrics:
            self.emit_simple_metrics(
                ids["audit"],
                ids["work"],
                flow,
                batch_id,
                metrics,
                execution_step="raw_to_work",
            )

        return metrics

    def process_work_to_silver(
        self,
        work_df: DataFrame,
        flow: KappaFlow,
        batch_id: int,
        *,
        write_quarantine: bool = True,
        write_silver: bool = True,
        emit_metrics: bool = True,
    ) -> Dict[str, Any]:
        """Process one layer transition only: Work -> Silver/Quarantine."""
        if work_df.rdd.isEmpty():
            return {
                "flow_name": flow.name,
                "batch_id": int(batch_id),
                "source_layer": "work",
                "target_layer": "silver",
                "input_rows": 0,
                "silver_rows": 0,
                "quarantine_rows": 0,
                "dq_rule_count": 0,
            }

        ids = self.table_idents(flow)
        silver_df, failed, dq_metrics = self.work_to_silver(work_df, flow)

        quarantine_rows = safe_count(failed)
        if write_quarantine and quarantine_rows > 0:
            failed = failed.withColumn("_meta_layer", F.lit("quarantine"))
            write_iceberg(failed, ids["quarantine"], partition_by=flow.partition_by, write_mode="append")

        if write_silver:
            write_silver_configured(silver_df, ids["silver"], flow, partition_by=flow.partition_by)

        metrics = {
            "flow_name": flow.name,
            "batch_id": int(batch_id),
            "source_layer": "work",
            "target_layer": "silver",
            "target_table": ids["silver"],
            "input_rows": safe_count(work_df),
            "silver_rows": safe_count(silver_df),
            "quarantine_rows": quarantine_rows,
            "dq_rule_count": len(dq_metrics),
        }

        if emit_metrics:
            self.emit_audit_metrics(ids["audit"], ids["silver"], flow, batch_id, metrics, dq_metrics)

        return metrics

    def emit_simple_metrics(
        self,
        audit_ident: str,
        target_table: str,
        flow: KappaFlow,
        batch_id: int,
        metrics: Dict[str, Any],
        *,
        execution_step: str,
    ) -> None:
        audit_rows = []
        for key, value in metrics.items():
            if key.endswith("_rows") or key in {"input_rows", "work_rows"}:
                audit_rows.append(
                    {
                        "flow_name": flow.name,
                        "micro_batch_id": int(batch_id),
                        "target_table": target_table,
                        "metric_name": f"{execution_step}_{key}",
                        "metric_value": float(value),
                    }
                )
        if audit_rows:
            audit_df = self.spark.createDataFrame(audit_rows)
            write_iceberg(audit_df, audit_ident, write_mode="append")

        if self.metadata_emitter:
            self.metadata_emitter.emit_batch_metrics(
                target_table,
                flow,
                batch_id,
                {
                    **{k: int(v) for k, v in metrics.items() if isinstance(v, int)},
                    "execution_step": execution_step,
                    "execution_style": "strict_layered",
                },
                self.registry,
            )

    def process_raw_to_silver(
        self,
        raw_df: DataFrame,
        flow: KappaFlow,
        batch_id: int,
        *,
        write_work: bool = True,
        write_quarantine: bool = True,
        write_silver: bool = True,
        emit_metrics: bool = True,
    ) -> Dict[str, Any]:
        if raw_df.rdd.isEmpty():
            return {
                "flow_name": flow.name,
                "batch_id": int(batch_id),
                "input_rows": 0,
                "work_rows": 0,
                "silver_rows": 0,
                "quarantine_rows": 0,
                "dq_rule_count": 0,
            }

        ids = self.table_idents(flow)
        work_df = self.raw_to_work(raw_df, flow)

        if write_work:
            write_iceberg(work_df, ids["work"], partition_by=flow.partition_by, write_mode="append")

        silver_df, failed, dq_metrics = self.work_to_silver(work_df, flow)

        quarantine_rows = safe_count(failed)
        if write_quarantine and quarantine_rows > 0:
            failed = failed.withColumn("_meta_layer", F.lit("quarantine"))
            write_iceberg(failed, ids["quarantine"], partition_by=flow.partition_by, write_mode="append")

        if write_silver:
            write_silver_configured(silver_df, ids["silver"], flow, partition_by=flow.partition_by)

        metrics = {
            "flow_name": flow.name,
            "batch_id": int(batch_id),
            "target_table": ids["silver"],
            "input_rows": safe_count(raw_df),
            "work_rows": safe_count(work_df),
            "silver_rows": safe_count(silver_df),
            "quarantine_rows": quarantine_rows,
            "dq_rule_count": len(dq_metrics),
        }

        if emit_metrics:
            self.emit_audit_metrics(ids["audit"], ids["silver"], flow, batch_id, metrics, dq_metrics)

        return metrics

    def emit_audit_metrics(
        self,
        audit_ident: str,
        target_table: str,
        flow: KappaFlow,
        batch_id: int,
        metrics: Dict[str, Any],
        dq_metrics: List[Dict[str, Any]],
    ) -> None:
        audit_rows = [
            {
                "flow_name": flow.name,
                "micro_batch_id": int(batch_id),
                "target_table": target_table,
                "metric_name": "input_rows",
                "metric_value": float(metrics.get("input_rows", 0)),
            },
            {
                "flow_name": flow.name,
                "micro_batch_id": int(batch_id),
                "target_table": target_table,
                "metric_name": "silver_rows",
                "metric_value": float(metrics.get("silver_rows", 0)),
            },
            {
                "flow_name": flow.name,
                "micro_batch_id": int(batch_id),
                "target_table": target_table,
                "metric_name": "quarantine_rows",
                "metric_value": float(metrics.get("quarantine_rows", 0)),
            },
        ]

        for m in dq_metrics:
            audit_rows.append(
                {
                    "flow_name": flow.name,
                    "micro_batch_id": int(batch_id),
                    "target_table": target_table,
                    "metric_name": f"dq_failed_{m['rule_id']}",
                    "metric_value": float(m.get("failed_rows", 0)),
                }
            )

        audit_df = self.spark.createDataFrame(audit_rows)
        write_iceberg(audit_df, audit_ident, write_mode="append")

        if self.metadata_emitter:
            self.metadata_emitter.emit_quality_results(target_table, flow, batch_id, dq_metrics, self.registry)
            self.metadata_emitter.emit_batch_metrics(
                target_table,
                flow,
                batch_id,
                {
                    "input_rows": int(metrics.get("input_rows", 0)),
                    "silver_rows": int(metrics.get("silver_rows", 0)),
                    "quarantine_rows": int(metrics.get("quarantine_rows", 0)),
                    "dq_rule_count": len(dq_metrics),
                    "execution_style": "layered_or_unified",
                },
                self.registry,
            )
