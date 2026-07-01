from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
import yaml

from src.common.kappa_registry import KappaFlow, KappaModel, KappaRegistry


@dataclass(frozen=True)
class OpenMetadataConfig:
    enabled: bool
    base_url: str
    auth_token: Optional[str]
    database_service_name: str
    database_service_type: str
    messaging_service_name: str
    messaging_service_type: str
    pipeline_service_name: str
    pipeline_service_type: str
    owner: str
    domain: str
    database_name: str
    include_embedded_metadata_columns: bool
    local_fallback_dir: str
    tag_mapping: Dict[str, str]


def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def load_openmetadata_config(path: str | Path = "metadata/openmetadata_config.yaml") -> OpenMetadataConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = _expand_env_vars(yaml.safe_load(f) or {})

    svc = raw.get("service", {}) or {}
    defaults = raw.get("defaults", {}) or {}

    return OpenMetadataConfig(
        enabled=bool(raw.get("enabled", True)),
        base_url=(raw.get("base_url") or "http://openmetadata:8585/api/v1").rstrip("/"),
        auth_token=raw.get("auth_token") or None,
        database_service_name=svc.get("database_service_name", "lakehouse_trino"),
        database_service_type=svc.get("database_service_type", "Trino"),
        messaging_service_name=svc.get("messaging_service_name", "lakehouse_kafka"),
        messaging_service_type=svc.get("messaging_service_type", "Kafka"),
        pipeline_service_name=svc.get("pipeline_service_name", "lakehouse_airflow"),
        pipeline_service_type=svc.get("pipeline_service_type", "Airflow"),
        owner=defaults.get("owner", "data-platform-team"),
        domain=defaults.get("domain", "enterprise-sales"),
        database_name=defaults.get("database_name", "lakehouse"),
        include_embedded_metadata_columns=bool(defaults.get("include_embedded_metadata_columns", True)),
        local_fallback_dir=defaults.get("local_fallback_dir", "results/openmetadata_events"),
        tag_mapping=raw.get("tag_mapping", {}) or {},
    )


def split_table_ref(table_ref: str, default_catalog: str = "lakehouse") -> tuple[str, str, str]:
    parts = table_ref.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return default_catalog, parts[0], parts[1]
    if len(parts) == 1:
        return default_catalog, "default", parts[0]
    raise ValueError(f"Unsupported table reference: {table_ref}")


def table_fqn(cfg: OpenMetadataConfig, table_ref: str, default_catalog: str = "lakehouse") -> str:
    catalog, schema, table = split_table_ref(table_ref, default_catalog)
    return f"{cfg.database_service_name}.{catalog}.{schema}.{table}"


def topic_fqn(cfg: OpenMetadataConfig, topic_name: str) -> str:
    return f"{cfg.messaging_service_name}.{topic_name}"


class OpenMetadataEmitter:
    """Defensive OpenMetadata emitter for the Kappa prototype.

    The class emits table/topic/pipeline lineage, embedded metadata contracts,
    quality results and operational metrics. If OpenMetadata is not available,
    every payload is saved locally so the experiment is still reproducible.
    """

    def __init__(self, cfg: OpenMetadataConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        if cfg.auth_token:
            self.session.headers.update({"Authorization": f"Bearer {cfg.auth_token}"})
        Path(cfg.local_fallback_dir).mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_file(cls, path: str | Path = "metadata/openmetadata_config.yaml") -> "OpenMetadataEmitter":
        return cls(load_openmetadata_config(path))

    def _write_fallback(self, kind: str, payload: Dict[str, Any]) -> None:
        ts = int(time.time() * 1000)
        safe_kind = kind.replace("/", "_").replace(".", "_")
        path = Path(self.cfg.local_fallback_dir) / f"{ts}_{safe_kind}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)

    def _request(self, method: str, path: str, payload: Dict[str, Any], kind: str) -> bool:
        if not self.cfg.enabled:
            self._write_fallback(kind, payload)
            return False
        try:
            response = self.session.request(method, f"{self.cfg.base_url}{path}", json=payload, timeout=10)
            if 200 <= response.status_code < 300:
                return True
            self._write_fallback(f"{kind}_http_{response.status_code}", {"payload": payload, "response": response.text[:2000]})
            return False
        except Exception as exc:
            self._write_fallback(f"{kind}_offline", {"payload": payload, "error": repr(exc)})
            return False

    def _put(self, path: str, payload: Dict[str, Any], kind: str) -> bool:
        return self._request("PUT", path, payload, kind)

    def _post(self, path: str, payload: Dict[str, Any], kind: str) -> bool:
        return self._request("POST", path, payload, kind)

    def _tags(self, values: Iterable[str]) -> List[Dict[str, Any]]:
        tags = []
        for value in values or []:
            tag_fqn = self.cfg.tag_mapping.get(value, value)
            tags.append({"tagFQN": tag_fqn, "source": "Tag", "labelType": "Manual", "state": "Confirmed"})
        return tags

    def embedded_metadata_columns(self, registry: KappaRegistry) -> List[Dict[str, Any]]:
        if not self.cfg.include_embedded_metadata_columns:
            return []
        include = registry.embedded_metadata.get("include", []) or []
        prefix = registry.embedded_metadata.get("prefix", "_meta")
        mapping = {
            "event_id": "Stable event identifier derived from Kafka topic/partition/offset or synthetic batch event id.",
            "source_system": "Original source system name.",
            "source_database": "Original source database.",
            "source_schema": "Original source schema.",
            "source_table": "Original source table.",
            "source_operation": "CDC operation: c/u/d/r.",
            "source_ts_ms": "Source commit/event timestamp in milliseconds.",
            "kafka_topic": "Kafka topic consumed by the Kappa pipeline.",
            "kafka_partition": "Kafka partition.",
            "kafka_offset": "Kafka offset.",
            "ingest_ts": "Lakehouse ingestion timestamp.",
            "config_version": "Metadata configuration version used to process the record.",
            "pipeline_name": "Kappa flow name.",
            "layer": "Medallion layer where the record is stored.",
            "record_hash": "Hash of business payload used for idempotent replay and SCD2 change detection.",
            "schema_hash": "Hash of the schema contract.",
            "dq_errors": "Array of data quality rule identifiers that failed.",
            "pii_tags": "PII classification tags embedded at record level.",
            "lineage": "JSON lineage context from topic to Raw/Work/Silver/Gold.",
        }
        cols = []
        for item in include:
            name = f"{prefix}_{item}"
            cols.append({
                "name": name,
                "dataType": "STRING",
                "dataTypeDisplay": "string",
                "description": mapping.get(item, f"Embedded metadata field {name}."),
                "tags": self._tags(["embedded-metadata"]),
            })
        # Operational fields added by the implementation but not always present in the YAML list.
        for name, desc in {
            "_meta_micro_batch_id": "Spark Structured Streaming micro-batch id.",
            "_meta_is_deleted": "Soft-delete flag derived from CDC delete operation.",
            "_meta_deleted_at": "Timestamp when the record was marked as deleted.",
            "_meta_closed_by_event_id": "Event id that closed an SCD2 version.",
            "_meta_closed_at": "Timestamp when an SCD2 version was closed.",
            "_meta_reconciled_at": "Timestamp when late-arriving foreign key reconciliation updated the record.",
            "_meta_reconciled_by": "Job name that reconciled unknown foreign keys.",
        }.items():
            cols.append({"name": name, "dataType": "STRING", "dataTypeDisplay": "string", "description": desc, "tags": self._tags(["embedded-metadata"])})
        return cols

    def flow_columns(self, flow: KappaFlow, registry: KappaRegistry) -> List[Dict[str, Any]]:
        cols = []
        pii_cols = set((flow.pii_policy or {}).keys())
        for c in flow.columns:
            tags = list(c.get("classification", []) or [])
            if c["name"] in pii_cols and "PII" not in tags:
                tags.append("PII")
            cols.append({
                "name": c["name"],
                "dataType": str(c.get("type", "string")).upper(),
                "dataTypeDisplay": str(c.get("type", "string")),
                "description": c.get("description") or f"Configured column from path {c.get('path', c['name'])}.",
                "constraint": "NOT_NULL" if c.get("nullable") is False else None,
                "tags": self._tags(tags),
                "customProperties": {
                    "sourcePath": c.get("path"),
                    "classification": ",".join(c.get("classification", []) or []),
                },
            })
        return cols + self.embedded_metadata_columns(registry)

    def register_topic(self, topic_name: str, registry: KappaRegistry) -> bool:
        payload = {
            "name": topic_name,
            "displayName": topic_name,
            "service": self.cfg.messaging_service_name,
            "messageSchema": {"schemaType": "JSON", "schemaText": "Debezium-compatible CDC envelope"},
            "description": "Kafka CDC topic consumed by the metadata-driven Kappa pipeline.",
            "tags": self._tags(["kappa"]),
            "customProperties": {"configVersion": registry.runtime.config_version},
        }
        return self._put("/topics", payload, f"topic_{topic_name}")

    def register_table(self, table_ref: str, flow: KappaFlow, registry: KappaRegistry, layer: str) -> bool:
        catalog, schema, table = split_table_ref(table_ref, registry.runtime.catalog)
        payload = {
            "name": table,
            "displayName": f"{schema}.{table}",
            "fullyQualifiedName": table_fqn(self.cfg, table_ref, registry.runtime.catalog),
            "databaseSchema": f"{self.cfg.database_service_name}.{catalog}.{schema}",
            "tableType": "Regular",
            "description": f"{layer.upper()} table generated by Kappa flow {flow.name}. {flow.description or ''}",
            "owners": [{"name": flow.owner or self.cfg.owner, "type": "user"}],
            "domain": flow.domain or self.cfg.domain,
            "columns": self.flow_columns(flow, registry),
            "tags": self._tags(list(flow.tags or []) + [flow.entity_type, "kappa"]),
            "customProperties": {
                "flowName": flow.name,
                "layer": layer,
                "entityType": flow.entity_type,
                "sourceTopics": ",".join(flow.topic_list),
                "naturalKey": ",".join(flow.natural_key),
                "sequenceColumn": flow.sequence_column or "",
                "scdType": str((flow.surrogate_key or {}).get("scd_type", flow.scd_type or "none")),
                "surrogateKey": json.dumps(flow.surrogate_key or {}, ensure_ascii=False),
                "foreignKeys": json.dumps(flow.foreign_keys or [], ensure_ascii=False),
                "piiPolicy": json.dumps(flow.pii_policy or {}, ensure_ascii=False),
                "dqRules": json.dumps((flow.data_quality or {}).get("rules", []), ensure_ascii=False),
                "configVersion": registry.runtime.config_version,
                "embeddedMetadataContract": json.dumps(registry.embedded_metadata or {}, ensure_ascii=False),
            },
        }
        return self._put("/tables", payload, f"table_{table_ref}")

    def register_model(self, model: KappaModel, registry: KappaRegistry) -> bool:
        table_ref = f"{registry.runtime.catalog}.{model.layer}.{model.name}"
        catalog, schema, table = split_table_ref(table_ref, registry.runtime.catalog)
        payload = {
            "name": table,
            "displayName": f"{schema}.{table}",
            "fullyQualifiedName": table_fqn(self.cfg, table_ref, registry.runtime.catalog),
            "databaseSchema": f"{self.cfg.database_service_name}.{catalog}.{schema}",
            "tableType": "Regular",
            "description": f"Configured Gold/Mart SQL model. Upstream: {', '.join(model.upstream)}",
            "columns": [],
            "tags": self._tags(["kappa"]),
            "customProperties": {
                "modelName": model.name,
                "layer": model.layer,
                "upstream": ",".join(model.upstream),
                "writeMode": model.write_mode,
                "sql": model.sql,
                "configVersion": registry.runtime.config_version,
            },
        }
        return self._put("/tables", payload, f"model_{model.name}")

    def add_lineage(self, from_fqn: str, to_fqn: str, pipeline_name: str, description: str = "") -> bool:
        payload = {
            "edge": {
                "fromEntity": {"fullyQualifiedName": from_fqn},
                "toEntity": {"fullyQualifiedName": to_fqn},
                "lineageDetails": {
                    "pipeline": pipeline_name,
                    "description": description,
                    "source": "metadata-driven-kappa-prototype",
                    "createdAt": int(time.time() * 1000),
                },
            }
        }
        return self._post("/lineage/addLineage", payload, f"lineage_{pipeline_name}")

    def emit_quality_results(self, table_ref: str, flow: KappaFlow, batch_id: int, dq_metrics: List[Dict[str, Any]], registry: KappaRegistry) -> None:
        for metric in dq_metrics or []:
            payload = {
                "table": table_fqn(self.cfg, table_ref, registry.runtime.catalog),
                "testCaseName": metric.get("rule_id"),
                "testCaseResult": {
                    "timestamp": int(time.time() * 1000),
                    "testCaseStatus": "Success" if int(metric.get("failed_rows", 0)) == 0 else "Failed",
                    "result": json.dumps(metric, ensure_ascii=False),
                    "sampleData": "",
                },
                "customProperties": {
                    "flowName": flow.name,
                    "microBatchId": str(batch_id),
                    "severity": metric.get("severity", "critical"),
                },
            }
            self._post("/dataQuality/testCases/testCaseResult", payload, f"dq_{flow.name}_{metric.get('rule_id')}")

    def emit_batch_metrics(self, table_ref: str, flow: KappaFlow, batch_id: int, metrics: Dict[str, Any], registry: KappaRegistry) -> None:
        payload = {
            "table": table_fqn(self.cfg, table_ref, registry.runtime.catalog),
            "flowName": flow.name,
            "microBatchId": batch_id,
            "metrics": metrics,
            "timestamp": int(time.time() * 1000),
            "configVersion": registry.runtime.config_version,
        }
        self._post("/events/kappaBatchMetrics", payload, f"batch_metrics_{flow.name}_{batch_id}")

    def register_flow_assets_and_lineage(self, flow: KappaFlow, registry: KappaRegistry) -> None:
        # Topic assets
        for topic in flow.topic_list:
            self.register_topic(topic, registry)

        # Table assets
        layer_tables = [
            ("raw", flow.raw_table),
            ("work", flow.work_table),
            ("silver", flow.silver_table),
            ("quarantine", flow.quarantine_table),
        ]
        for layer, table_ref in layer_tables:
            self.register_table(table_ref, flow, registry, layer)

        # Lineage: topic -> raw -> work -> silver, and work -> quarantine.
        for topic in flow.topic_list:
            self.add_lineage(
                topic_fqn(self.cfg, topic),
                table_fqn(self.cfg, flow.raw_table, registry.runtime.catalog),
                flow.name,
                "Kafka topic consumed into Raw Iceberg table.",
            )
        self.add_lineage(table_fqn(self.cfg, flow.raw_table, registry.runtime.catalog), table_fqn(self.cfg, flow.work_table, registry.runtime.catalog), flow.name, "Raw to Work standardization and business derivation.")
        self.add_lineage(table_fqn(self.cfg, flow.work_table, registry.runtime.catalog), table_fqn(self.cfg, flow.silver_table, registry.runtime.catalog), flow.name, "Work to Silver DQ, masking, SCD/SK/FK merge.")
        self.add_lineage(table_fqn(self.cfg, flow.work_table, registry.runtime.catalog), table_fqn(self.cfg, flow.quarantine_table, registry.runtime.catalog), flow.name, "Records failing critical DQ rules are routed to Quarantine.")

    def register_all(self, registry: KappaRegistry) -> None:
        for flow in registry.enabled_flows():
            self.register_flow_assets_and_lineage(flow, registry)
        for model in registry.enabled_models():
            self.register_model(model, registry)
            for upstream in model.upstream:
                source_ref = upstream if upstream.count(".") >= 2 else f"{registry.runtime.catalog}.{upstream}"
                self.add_lineage(
                    table_fqn(self.cfg, source_ref, registry.runtime.catalog),
                    table_fqn(self.cfg, f"{registry.runtime.catalog}.{model.layer}.{model.name}", registry.runtime.catalog),
                    model.name,
                    "Configured SQL model lineage.",
                )
