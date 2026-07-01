from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict
import yaml


@dataclass
class LakehouseConfig:
    raw: str
    work: str
    silver: str
    gold: str
    mart: str
    audit: str
    quarantine: str
    jdbc_url: str
    jdbc_user: str
    jdbc_password: str
    kafka_bootstrap: str
    openmetadata_url: str
    tables: Dict[str, Any]
    lineage: Dict[str, Any]


def load_config(path: str) -> LakehouseConfig:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    env = cfg["environment"]
    wh = env["warehouse"]
    jdbc = env["jdbc"]
    kafka = env["kafka"]
    om = env.get("openmetadata", {})
    return LakehouseConfig(
        raw=wh["raw"], work=wh["work"], silver=wh["silver"], gold=wh["gold"], mart=wh["mart"], audit=wh["audit"], quarantine=wh["quarantine"],
        jdbc_url=os.getenv("SOURCE_JDBC_URL", jdbc["source_url"]),
        jdbc_user=os.getenv("SOURCE_JDBC_USER", jdbc["user"]),
        jdbc_password=os.getenv("SOURCE_JDBC_PASSWORD", jdbc["password"]),
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", kafka["bootstrap_servers"]),
        openmetadata_url=os.getenv("OPENMETADATA_URL", om.get("url", "http://openmetadata:8585/api")),
        tables=cfg["tables"], lineage=cfg.get("lineage", {})
    )
