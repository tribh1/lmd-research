from __future__ import annotations

import time
from typing import Any, Dict, Optional
import requests
from src.common.table_registry import RuntimeConfig, TableSpec, ModelSpec


class MetadataEmitter:
    """Emit runtime metadata to OpenMetadata when enabled.

    The class is intentionally defensive: pipeline execution must not fail when
    the local experiment environment has not started OpenMetadata yet. In that
    case, metadata operations return False and the audit table still records the
    run locally.
    """

    def __init__(self, cfg: RuntimeConfig, token: Optional[str] = None):
        self.enabled = cfg.openmetadata_enabled
        self.base_url = cfg.openmetadata_url.rstrip("/")
        self.session = requests.Session()
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _post(self, path: str, payload: Dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        try:
            response = self.session.post(f"{self.base_url}{path}", json=payload, timeout=5)
            return 200 <= response.status_code < 300
        except Exception:
            return False

    def register_table(self, cfg: RuntimeConfig, table: TableSpec, layer: str) -> bool:
        payload = {
            "name": table.target_table,
            "displayName": f"{layer}.{table.target_table}",
            "description": table.description,
            "tableType": "Regular",
            "fullyQualifiedName": f"{cfg.catalog}.{layer}.{table.target_table}",
            "owner": table.owner,
            "domain": table.domain,
            "tags": table.tags,
            "glossaryTerms": table.glossary_terms,
            "columns": [
                {
                    "name": c.name,
                    "dataTypeDisplay": c.type,
                    "description": c.description,
                    "tags": c.classification,
                    "constraint": "NOT_NULL" if not c.nullable else None,
                }
                for c in table.columns
            ],
        }
        return self._post("/v1/tables", payload)

    def emit_lineage(self, from_entity: str, to_entity: str, job_name: str, batch_id: str) -> bool:
        payload = {
            "edge": {"fromEntity": from_entity, "toEntity": to_entity},
            "job": job_name,
            "batchId": batch_id,
            "eventTime": int(time.time() * 1000),
        }
        return self._post("/v1/lineage", payload)

    def emit_quality_result(self, table_fqn: str, result: Dict[str, Any]) -> bool:
        payload = {"table": table_fqn, **result}
        return self._post("/v1/dataQuality/testCases/run", payload)

    def register_model(self, cfg: RuntimeConfig, model: ModelSpec) -> bool:
        payload = {
            "name": model.name,
            "displayName": f"{model.layer}.{model.name}",
            "fullyQualifiedName": f"{cfg.catalog}.{model.layer}.{model.name}",
            "tableType": "View" if model.layer == "mart" else "Regular",
            "description": f"Configured SQL model. Upstream: {', '.join(model.upstream)}",
            "customProperties": {"sql": model.sql, "upstream": ",".join(model.upstream)},
        }
        return self._post("/v1/tables", payload)
