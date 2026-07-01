from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import os
import yaml


class KappaRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class KappaRuntime:
    app_name: str
    catalog: str
    checkpoint_base: str
    default_trigger: str
    config_version: str
    source_system: str
    audit_namespace: str = "audit"
    quarantine_namespace: str = "quarantine"


@dataclass(frozen=True)
class KappaConnection:
    name: str
    type: str
    options: Dict[str, Any]


@dataclass(frozen=True)
class KappaFlow:
    name: str
    enabled: bool
    entity_type: str
    source: Dict[str, Any]
    target: Dict[str, Any]
    natural_key: List[str]
    sequence_column: Optional[str]
    schema_contract: Dict[str, Any]
    standardization: Dict[str, Any]
    business_logic: Dict[str, Any]
    data_quality: Dict[str, Any]
    pii_policy: Dict[str, Any]
    surrogate_key: Dict[str, Any]
    foreign_keys: List[Dict[str, Any]] = field(default_factory=list)
    scd_type: Any = None
    owner: Optional[str] = None
    domain: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    @property
    def columns(self) -> List[Dict[str, Any]]:
        return self.schema_contract.get("columns", []) or []

    @property
    def topic_list(self) -> List[str]:
        return self.source.get("topics", []) or []

    @property
    def raw_table(self) -> str:
        return self.target["raw_table"]

    @property
    def work_table(self) -> str:
        return self.target["work_table"]

    @property
    def silver_table(self) -> str:
        return self.target["silver_table"]

    @property
    def quarantine_table(self) -> str:
        return self.target.get("quarantine_table", f"quarantine.{self.name}_failed")

    @property
    def partition_by(self) -> Optional[str]:
        return self.target.get("partition_by")

    @property
    def write_mode(self) -> str:
        return self.target.get("write_mode", "merge")


@dataclass(frozen=True)
class KappaModel:
    name: str
    enabled: bool
    layer: str
    sql: str
    upstream: List[str] = field(default_factory=list)
    write_mode: str = "overwrite"


@dataclass(frozen=True)
class KappaRegistry:
    metadata_version: str
    runtime: KappaRuntime
    connections: Dict[str, KappaConnection]
    embedded_metadata: Dict[str, Any]
    rule_sets: Dict[str, List[Dict[str, Any]]]
    flows: Dict[str, KappaFlow]
    models: Dict[str, KappaModel]

    def enabled_flows(self, names: Optional[Iterable[str]] = None) -> List[KappaFlow]:
        selected = set(names or [])
        values = [f for f in self.flows.values() if f.enabled]
        return [f for f in values if not selected or f.name in selected]

    def enabled_models(self, names: Optional[Iterable[str]] = None) -> List[KappaModel]:
        selected = set(names or [])
        values = [m for m in self.models.values() if m.enabled]
        return [m for m in values if not selected or m.name in selected]


def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def load_kappa_registry(path: str | Path) -> KappaRegistry:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    raw = _expand_env_vars(raw)

    rt = raw.get("runtime", {}) or {}
    runtime = KappaRuntime(
        app_name=rt.get("app_name", "kappa-config-driven-lakehouse"),
        catalog=rt.get("catalog", "lakehouse"),
        checkpoint_base=rt.get("checkpoint_base", "s3a://warehouse/checkpoints/kappa"),
        default_trigger=rt.get("default_trigger", "30 seconds"),
        config_version=str(rt.get("config_version", raw.get("metadata_version", "unknown"))),
        source_system=rt.get("source_system", "unknown"),
        audit_namespace=rt.get("audit_namespace", "audit"),
        quarantine_namespace=rt.get("quarantine_namespace", "quarantine"),
    )

    connections = {
        name: KappaConnection(name=name, type=spec.get("type", "unknown"), options={k: v for k, v in spec.items() if k != "type"})
        for name, spec in (raw.get("connections", {}) or {}).items()
    }

    flows = {}
    for item in raw.get("flows", []) or []:
        flow = KappaFlow(
            name=item["name"],
            enabled=bool(item.get("enabled", True)),
            entity_type=item.get("entity_type", "table"),
            source=item.get("source", {}) or {},
            target=item.get("target", {}) or {},
            natural_key=item.get("natural_key", []) or [],
            sequence_column=item.get("sequence_column"),
            schema_contract=item.get("schema_contract", {}) or {},
            standardization=item.get("standardization", {}) or {},
            business_logic=item.get("business_logic", {}) or {},
            data_quality=item.get("data_quality", {}) or {},
            pii_policy=item.get("pii_policy", {}) or {},
            surrogate_key=item.get("surrogate_key", {}) or {},
            foreign_keys=item.get("foreign_keys", []) or [],
            scd_type=item.get("scd_type"),
            owner=item.get("owner"),
            domain=item.get("domain"),
            description=item.get("description"),
            tags=item.get("tags", []) or [],
        )
        flows[flow.name] = flow

    models = {}
    for item in raw.get("models", []) or []:
        model = KappaModel(
            name=item["name"],
            enabled=bool(item.get("enabled", True)),
            layer=item.get("layer", "gold"),
            sql=item["sql"],
            upstream=item.get("upstream", []) or [],
            write_mode=item.get("write_mode", "overwrite"),
        )
        models[model.name] = model

    registry = KappaRegistry(
        metadata_version=str(raw.get("metadata_version", "1.0")),
        runtime=runtime,
        connections=connections,
        embedded_metadata=raw.get("embedded_metadata", {}) or {},
        rule_sets=raw.get("rule_sets", {}) or {},
        flows=flows,
        models=models,
    )
    validate_kappa_registry(registry)
    return registry


def validate_kappa_registry(registry: KappaRegistry) -> None:
    if not registry.connections:
        raise KappaRegistryError("At least one Kafka connection must be configured")
    for flow in registry.flows.values():
        conn = flow.source.get("connection")
        if conn not in registry.connections:
            raise KappaRegistryError(f"Flow {flow.name}: missing connection {conn}")
        for field in ["raw_table", "work_table", "silver_table"]:
            if field not in flow.target:
                raise KappaRegistryError(f"Flow {flow.name}: target.{field} is required")
        cols = [c["name"] for c in flow.columns]
        missing_key = [k for k in flow.natural_key if k not in cols]
        if missing_key:
            raise KappaRegistryError(f"Flow {flow.name}: natural key not found in schema {missing_key}")


def kappa_registry_summary(registry: KappaRegistry) -> List[Dict[str, Any]]:
    return [
        {
            "name": f.name,
            "entity_type": f.entity_type,
            "topics": f.topic_list,
            "raw": f.raw_table,
            "work": f.work_table,
            "silver": f.silver_table,
            "natural_key": f.natural_key,
            "surrogate_key": f.surrogate_key.get("column"),
            "dq_rules": len(f.data_quality.get("rules", []) or []),
            "pii_columns": list((f.pii_policy or {}).keys()),
            "foreign_keys": [x.get("name") for x in f.foreign_keys],
        }
        for f in registry.flows.values()
    ]
