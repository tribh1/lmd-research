from __future__ import annotations

import base64
import os
import time
from typing import Dict, List, Optional
import requests


class MetadataClient:
    """OpenMetadata REST client used by the pipeline jobs and experiments.

    OpenMetadata is the authoritative catalog of the prototype (thesis Section 3.3).
    All methods degrade gracefully: when the server is unreachable the call returns
    False/None so local smoke runs (CI without the full stack) still succeed. Jobs
    additionally persist lineage/quality evidence into Iceberg audit tables, so the
    experiments never depend solely on OpenMetadata availability.

    Spark table identifiers ("lakehouse.raw.customers") are mapped to OpenMetadata
    fully qualified names ("<service>.<database>.<schema>.<table>"). PostgreSQL
    source tables ("public.src_customer") are registered under the "source" schema.
    """

    def __init__(self, base_url: str, token: Optional[str] = None,
                 service_name: str = "mdl-eg", database_name: str = "lakehouse"):
        self.base_url = base_url.rstrip("/")
        self.service_name = service_name
        self.database_name = database_name
        self.session = requests.Session()
        self._available: Optional[bool] = None
        token = token or os.getenv("OPENMETADATA_JWT_TOKEN")
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            self._basic_login()

    # ------------------------------------------------------------------ auth
    def _basic_login(self) -> None:
        email = os.getenv("OPENMETADATA_ADMIN_EMAIL", "admin@open-metadata.org")
        password = os.getenv("OPENMETADATA_ADMIN_PASSWORD", "admin")
        try:
            r = self.session.post(
                f"{self.base_url}/v1/users/login",
                json={"email": email, "password": base64.b64encode(password.encode()).decode()},
                timeout=10,
            )
            if r.ok:
                self.session.headers.update({"Authorization": f"Bearer {r.json()['accessToken']}"})
                self._available = True
        except Exception:
            self._available = False

    def available(self) -> bool:
        if self._available is not None:
            return self._available
        try:
            r = self.session.get(f"{self.base_url}/v1/system/version", timeout=5)
            self._available = r.ok
        except Exception:
            self._available = False
        return self._available

    # ------------------------------------------------------------------ http
    def _put(self, path: str, payload: Dict) -> Optional[Dict]:
        try:
            r = self.session.put(f"{self.base_url}{path}", json=payload, timeout=15)
            if 200 <= r.status_code < 300:
                return r.json()
            print(f"[metadata] PUT {path} -> {r.status_code}: {r.text[:300]}")
        except Exception as ex:
            print(f"[metadata] PUT {path} failed: {ex}")
        return None

    def _post(self, path: str, payload: Dict, ok_conflict: bool = True) -> Optional[Dict]:
        try:
            r = self.session.post(f"{self.base_url}{path}", json=payload, timeout=15)
            if 200 <= r.status_code < 300:
                return r.json()
            if ok_conflict and r.status_code == 409:
                return {}
            print(f"[metadata] POST {path} -> {r.status_code}: {r.text[:300]}")
        except Exception as ex:
            print(f"[metadata] POST {path} failed: {ex}")
        return None

    def _get(self, path: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            r = self.session.get(f"{self.base_url}{path}", params=params, timeout=15)
            if r.ok:
                return r.json()
        except Exception as ex:
            print(f"[metadata] GET {path} failed: {ex}")
        return None

    # ------------------------------------------------------------------ fqn
    def to_fqn(self, ident: str) -> str:
        """Map a Spark identifier or source table name to an OpenMetadata FQN."""
        parts = ident.split(".")
        if len(parts) == 3 and parts[0] == self.database_name:
            _, schema, table = parts
            return f"{self.service_name}.{self.database_name}.{schema}.{table}"
        # PostgreSQL source table, e.g. "public.src_customer"
        return f"{self.service_name}.{self.database_name}.source.{parts[-1]}"

    # ------------------------------------------------------ entity bootstrap
    def ensure_service(self) -> Optional[Dict]:
        return self._put("/v1/services/databaseServices", {
            "name": self.service_name,
            "serviceType": "CustomDatabase",
            "connection": {"config": {"type": "CustomDatabase",
                                      "sourcePythonClass": "",
                                      "connectionOptions": {}}},
            "description": "Metadata-driven Lakehouse with Embedded Governance (MDL-EG) prototype",
        })

    def ensure_database(self) -> Optional[Dict]:
        return self._put("/v1/databases", {
            "name": self.database_name,
            "service": self.service_name,
            "description": "Iceberg lakehouse catalog (five-tier Medallion design)",
        })

    def ensure_schema(self, schema: str, description: str = "") -> Optional[Dict]:
        return self._put("/v1/databaseSchemas", {
            "name": schema,
            "database": f"{self.service_name}.{self.database_name}",
            "description": description,
        })

    def ensure_table(self, schema: str, table: str, columns: List[Dict],
                     description: str = "", owner: Optional[Dict] = None,
                     tags: Optional[List[Dict]] = None) -> Optional[Dict]:
        payload = {
            "name": table,
            "databaseSchema": f"{self.service_name}.{self.database_name}.{schema}",
            "columns": columns,
            "description": description,
        }
        if owner:
            payload["owner"] = owner
        if tags:
            payload["tags"] = tags
        return self._put("/v1/tables", payload)

    def ensure_team(self, name: str, description: str = "") -> Optional[Dict]:
        return self._put("/v1/teams", {"name": name, "teamType": "Group", "description": description})

    def ensure_classification(self, name: str, description: str = "") -> Optional[Dict]:
        return self._put("/v1/classifications", {"name": name, "description": description or name})

    def ensure_tag(self, classification: str, name: str, description: str = "") -> Optional[Dict]:
        return self._put("/v1/tags", {"classification": classification, "name": name,
                                      "description": description or name})

    def ensure_glossary(self, name: str, description: str = "") -> Optional[Dict]:
        return self._put("/v1/glossaries", {"name": name, "description": description or name})

    def ensure_glossary_term(self, glossary: str, term: str, description: str = "") -> Optional[Dict]:
        return self._put("/v1/glossaryTerms", {"glossary": glossary, "name": term,
                                               "description": description or term})

    # ---------------------------------------------------------------- lookup
    def get_table(self, ident_or_fqn: str, fields: str = "") -> Optional[Dict]:
        fqn = ident_or_fqn if ident_or_fqn.startswith(self.service_name + ".") else self.to_fqn(ident_or_fqn)
        params = {"fields": fields} if fields else None
        return self._get(f"/v1/tables/name/{fqn}", params)

    def list_tables(self, fields: str = "owner,tags", limit: int = 500) -> List[Dict]:
        out: List[Dict] = []
        after = None
        while True:
            params = {"fields": fields, "limit": min(limit, 100),
                      "database": f"{self.service_name}.{self.database_name}"}
            if after:
                params["after"] = after
            page = self._get("/v1/tables", params)
            if not page:
                break
            out.extend(page.get("data", []))
            after = page.get("paging", {}).get("after")
            if not after or len(out) >= limit:
                break
        return out

    def search(self, query: str, index: str = "table_search_index") -> Optional[Dict]:
        return self._get("/v1/search/query", {"q": query, "index": index, "from": 0, "size": 10})

    def get_lineage(self, ident_or_fqn: str, upstream_depth: int = 10,
                    downstream_depth: int = 0) -> Optional[Dict]:
        fqn = ident_or_fqn if ident_or_fqn.startswith(self.service_name + ".") else self.to_fqn(ident_or_fqn)
        return self._get(f"/v1/lineage/table/name/{fqn}",
                         {"upstreamDepth": upstream_depth, "downstreamDepth": downstream_depth})

    def update_table_schema(self, schema: str, table: str, spark_schema) -> bool:
        """Re-register a table's columns from a Spark schema after a pipeline write.

        Keeps the catalog in sync with schema evolution (Experiment 5): existing
        description, owner, and tags are preserved; column tags are carried over
        by column name.
        """
        type_map = {"long": "BIGINT", "integer": "INT", "string": "TEXT",
                    "timestamp": "TIMESTAMP", "boolean": "BOOLEAN", "double": "DOUBLE",
                    "date": "DATE", "float": "FLOAT"}
        existing = self.get_table(f"{self.service_name}.{self.database_name}.{schema}.{table}",
                                  fields="owner,tags,columns")
        old_col_tags = {c["name"]: c.get("tags") for c in (existing or {}).get("columns", [])}
        columns = []
        for field in spark_schema.fields:
            if field.name.startswith("_"):
                continue
            tname = field.dataType.typeName()
            om_type = "DECIMAL" if tname.startswith("decimal") else type_map.get(tname, "TEXT")
            col = {"name": field.name, "dataType": om_type}
            if old_col_tags.get(field.name):
                col["tags"] = old_col_tags[field.name]
            columns.append(col)
        payload = {
            "name": table,
            "databaseSchema": f"{self.service_name}.{self.database_name}.{schema}",
            "columns": columns,
        }
        if existing:
            if existing.get("description"):
                payload["description"] = existing["description"]
            if existing.get("owner"):
                payload["owner"] = {"id": existing["owner"]["id"], "type": existing["owner"]["type"]}
            if existing.get("tags"):
                payload["tags"] = existing["tags"]
        return self._put("/v1/tables", payload) is not None

    # --------------------------------------------------------------- lineage
    def emit_lineage(self, from_entity: str, to_entity: str, job_name: str, batch_id: str) -> bool:
        src = self.get_table(from_entity)
        dst = self.get_table(to_entity)
        if not src or not dst:
            return False
        payload = {
            "edge": {
                "fromEntity": {"id": src["id"], "type": "table"},
                "toEntity": {"id": dst["id"], "type": "table"},
                "lineageDetails": {"description": f"{job_name} (batch {batch_id})"},
            }
        }
        return self._put("/v1/lineage", payload) is not None

    # ----------------------------------------------------------- data quality
    def emit_quality_result(self, table_ident: str, rule_id: str,
                            passed: int, failed: int, batch_id: str) -> bool:
        fqn = self.to_fqn(table_ident)
        if not self.get_table(fqn):
            return False
        suite = self._post("/v1/dataQuality/testSuites/executable", {
            "name": f"{fqn}.testSuite",
            "executableEntityReference": fqn,
        })
        if suite is None:
            return False
        case = self._post("/v1/dataQuality/testCases", {
            "name": rule_id,
            "testDefinition": "tableCustomSQLQuery",
            "testSuite": f"{fqn}.testSuite",
            "entityLink": f"<#E::table::{fqn}>",
            "parameterValues": [{"name": "sqlExpression",
                                 "value": f"-- inline governance rule {rule_id}"}],
        })
        if case is None:
            return False
        result = self._put(f"/v1/dataQuality/testCases/{fqn}.{rule_id}/testCaseResult", {
            "timestamp": int(time.time() * 1000),
            "testCaseStatus": "Success" if failed == 0 else "Failed",
            "result": f"passed={passed} failed={failed} batch={batch_id}",
        })
        return result is not None
