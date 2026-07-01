from __future__ import annotations

import json
import time
from typing import Dict, Iterable, List, Optional
import requests


class MetadataClient:
    """Small OpenMetadata client with graceful local fallback.

    In the prototype, OpenMetadata is the authoritative catalog. This class keeps
    the jobs decoupled from OpenMetadata availability during local development.
    """
    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _post(self, path: str, payload: Dict) -> bool:
        try:
            r = self.session.post(f"{self.base_url}{path}", json=payload, timeout=5)
            return 200 <= r.status_code < 300
        except Exception:
            return False

    def emit_lineage(self, from_entity: str, to_entity: str, job_name: str, batch_id: str) -> bool:
        payload = {
            "edge": {"fromEntity": from_entity, "toEntity": to_entity},
            "job": job_name,
            "batchId": batch_id,
            "ts": int(time.time() * 1000),
        }
        # Replace with OpenMetadata addLineage endpoint in the target deployment.
        return self._post("/v1/lineage", payload)

    def emit_quality_result(self, table: str, rule_id: str, passed: int, failed: int, batch_id: str) -> bool:
        payload = {"table": table, "ruleId": rule_id, "passed": passed, "failed": failed, "batchId": batch_id}
        return self._post("/v1/dataQuality/testCases/run", payload)

    def register_asset_snapshot(self, asset: Dict) -> bool:
        return self._post("/v1/tables", asset)
