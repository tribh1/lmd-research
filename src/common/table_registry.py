from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import os
import yaml


class RegistryError(ValueError):
    """Raised when the metadata registry is invalid."""


@dataclass(frozen=True)
class ConnectionSpec:
    name: str
    type: str
    options: Dict[str, Any]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str = "string"
    nullable: bool = True
    description: Optional[str] = None
    classification: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class TableSpec:
    name: str
    enabled: bool
    source: Dict[str, Any]
    target: Dict[str, Any]
    primary_key: List[str]
    watermark_column: Optional[str]
    schema_contract: Dict[str, Any]
    transformations: Dict[str, Any]
    governance: Dict[str, Any]
    domain: Optional[str] = None
    owner: Optional[str] = None
    description: Optional[str] = None
    glossary_terms: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    @property
    def target_table(self) -> str:
        return self.target.get("table_name", self.name)

    @property
    def partition_by(self) -> Optional[str]:
        return self.target.get("partition_by")

    @property
    def write_mode(self) -> str:
        return self.target.get("write_mode", "append")

    @property
    def columns(self) -> List[ColumnSpec]:
        cols = self.schema_contract.get("columns", []) or []
        return [ColumnSpec(**c) for c in cols]

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    @property
    def full_source_name(self) -> str:
        if self.source.get("type") == "jdbc":
            return self.source.get("table") or self.source.get("query", self.name)
        return self.source.get("topic") or self.source.get("path") or self.name


@dataclass(frozen=True)
class ModelSpec:
    name: str
    enabled: bool
    layer: str
    sql: str
    upstream: List[str] = field(default_factory=list)
    write_mode: str = "overwrite"
    partition_by: Optional[str] = None


@dataclass(frozen=True)
class RuntimeConfig:
    metadata_version: str
    environment: Dict[str, Any]
    pipeline: Dict[str, Any]
    connections: Dict[str, ConnectionSpec]
    tables: Dict[str, TableSpec]
    models: Dict[str, ModelSpec]

    @property
    def catalog(self) -> str:
        return self.environment.get("lakehouse", {}).get("catalog", "lakehouse")

    @property
    def quarantine_namespace(self) -> str:
        return self.environment.get("lakehouse", {}).get("quarantine_namespace", "quarantine")

    @property
    def audit_namespace(self) -> str:
        return self.environment.get("lakehouse", {}).get("audit_namespace", "audit")

    @property
    def openmetadata_url(self) -> str:
        return os.getenv(
            "OPENMETADATA_URL",
            self.environment.get("openmetadata", {}).get("url", "http://openmetadata:8585/api"),
        )

    @property
    def openmetadata_enabled(self) -> bool:
        raw = os.getenv("OPENMETADATA_ENABLED")
        if raw is not None:
            return raw.lower() in {"1", "true", "yes", "y"}
        return bool(self.environment.get("openmetadata", {}).get("enabled", False))

    def enabled_tables(self, names: Optional[Iterable[str]] = None) -> List[TableSpec]:
        selected = set(names or [])
        tables = [t for t in self.tables.values() if t.enabled]
        if selected:
            tables = [t for t in tables if t.name in selected]
        return tables

    def enabled_models(self, names: Optional[Iterable[str]] = None) -> List[ModelSpec]:
        selected = set(names or [])
        models = [m for m in self.models.values() if m.enabled]
        if selected:
            models = [m for m in models if m.name in selected]
        return models


def _expand_env_vars(obj: Any) -> Any:
    """Allow ${ENV_VAR} placeholders inside YAML values."""
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def load_registry(path: str | Path) -> RuntimeConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    raw = _expand_env_vars(raw)

    env = raw.get("environment", {})
    connection_items = env.get("connections", {})
    connections = {
        name: ConnectionSpec(name=name, type=spec.get("type", "unknown"), options={k: v for k, v in spec.items() if k != "type"})
        for name, spec in connection_items.items()
    }

    table_items = raw.get("tables", []) or []
    tables: Dict[str, TableSpec] = {}
    for item in table_items:
        if "name" not in item:
            raise RegistryError("Every table entry must have a name")
        spec = TableSpec(
            name=item["name"],
            enabled=bool(item.get("enabled", True)),
            source=item.get("source", {}),
            target=item.get("target", {}),
            primary_key=item.get("primary_key", []),
            watermark_column=item.get("watermark_column"),
            schema_contract=item.get("schema_contract", {}),
            transformations=item.get("transformations", {}),
            governance=item.get("governance", {}),
            domain=item.get("domain"),
            owner=item.get("owner"),
            description=item.get("description"),
            glossary_terms=item.get("glossary_terms", []) or [],
            tags=item.get("tags", []) or [],
        )
        tables[spec.name] = spec

    model_items = raw.get("models", []) or []
    models: Dict[str, ModelSpec] = {}
    for item in model_items:
        if "name" not in item or "sql" not in item:
            raise RegistryError("Every model entry must have name and sql")
        spec = ModelSpec(
            name=item["name"],
            enabled=bool(item.get("enabled", True)),
            layer=item.get("layer", "gold"),
            sql=item["sql"],
            upstream=item.get("upstream", []) or [],
            write_mode=item.get("write_mode", "overwrite"),
            partition_by=item.get("partition_by"),
        )
        models[spec.name] = spec

    cfg = RuntimeConfig(
        metadata_version=str(raw.get("metadata_version", "1.0")),
        environment=env,
        pipeline=raw.get("pipeline", {}) or {},
        connections=connections,
        tables=tables,
        models=models,
    )
    validate_registry(cfg)
    return cfg


def validate_registry(cfg: RuntimeConfig) -> None:
    if not cfg.connections:
        raise RegistryError("At least one connection must be configured")
    for table in cfg.tables.values():
        source = table.source
        if source.get("type") == "jdbc":
            conn_name = source.get("connection")
            if conn_name not in cfg.connections:
                raise RegistryError(f"Table {table.name}: missing JDBC connection {conn_name}")
            if not source.get("table") and not source.get("query"):
                raise RegistryError(f"Table {table.name}: JDBC source requires table or query")
        pk_missing = [c for c in table.primary_key if c not in table.column_names]
        if table.schema_contract.get("columns") and pk_missing:
            raise RegistryError(f"Table {table.name}: primary key columns not found in schema: {pk_missing}")
        if table.watermark_column and table.column_names and table.watermark_column not in table.column_names:
            raise RegistryError(f"Table {table.name}: watermark column not found in schema")


def export_registry_summary(cfg: RuntimeConfig) -> List[Dict[str, Any]]:
    return [
        {
            "name": t.name,
            "enabled": t.enabled,
            "source": t.full_source_name,
            "target": f"{cfg.catalog}.silver.{t.target_table}",
            "pk": t.primary_key,
            "watermark": t.watermark_column,
            "dq_rules": len(t.governance.get("dq_rules", []) or []),
            "pii_columns": list((t.governance.get("pii_columns", {}) or {}).keys()),
        }
        for t in cfg.tables.values()
    ]
