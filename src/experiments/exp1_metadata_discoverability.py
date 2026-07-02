"""Experiment 1: Metadata Discoverability (thesis Section 4.5, Table 4.5).

Measures against the running OpenMetadata catalog:
- metadata coverage rate: % of registered assets with description + owner + >=1 tag
- search latency: median catalog response time across ten keyword queries (ms)
- lineage depth: max transformation hops from mart.sales_dashboard back to source

Requires `openmetadata_bootstrap` to have registered the catalog. When
OpenMetadata is unreachable (CI smoke test), falls back to a local YAML scan and
labels the result mode accordingly — offline numbers must NOT be reported in
Table 4.5.
"""
from __future__ import annotations

import argparse
import os
import time
from collections import defaultdict, deque
from typing import Dict, List
import yaml

from src.common.metadata_client import MetadataClient
from ._utils import write_json, median

SEARCH_QUERIES = ["customer", "order", "payment", "pii", "kpi",
                  "sales", "product", "telephone", "gold", "revenue"]


def coverage_from_openmetadata(om: MetadataClient) -> Dict:
    tables = om.list_tables(fields="owner,tags")
    total = len(tables)
    covered = 0
    for t in tables:
        has_description = bool(t.get("description"))
        has_owner = bool(t.get("owner"))
        has_tag = bool(t.get("tags"))
        if has_description and has_owner and has_tag:
            covered += 1
    return {"total_assets": total, "covered_assets": covered,
            "coverage_pct": round(covered / total * 100, 2) if total else 0.0}


def search_latency_openmetadata(om: MetadataClient, queries: List[str]) -> Dict:
    timings = []
    hits = {}
    for q in queries:
        start = time.time()
        res = om.search(q)
        timings.append((time.time() - start) * 1000)
        if res:
            hits[q] = res.get("hits", {}).get("total", {}).get("value", 0)
    return {"median_ms": round(median(timings), 2), "per_query_hits": hits}


def lineage_depth_openmetadata(om: MetadataClient, target: str = "lakehouse.mart.sales_dashboard") -> int:
    res = om.get_lineage(target, upstream_depth=10)
    if not res:
        return 0
    # Build upstream adjacency from returned edges and BFS from the target entity.
    entity_id = res.get("entity", {}).get("id")
    upstream = defaultdict(list)
    for e in res.get("upstreamEdges", []) or []:
        upstream[e["toEntity"]].append(e["fromEntity"])
    q = deque([(entity_id, 0)])
    seen = set()
    max_depth = 0
    while q:
        node, depth = q.popleft()
        if node in seen:
            continue
        seen.add(node)
        max_depth = max(max_depth, depth)
        for up in upstream.get(node, []):
            q.append((up, depth + 1))
    return max_depth


# --------------------------------------------------------------- offline mode
def coverage_from_yaml(cfg: Dict) -> float:
    total = covered = 0
    for spec in cfg["tables"].values():
        total += 1
        if spec.get("description") and (spec.get("pii_columns") or spec.get("dq_rules") or spec.get("glossary_terms")):
            covered += 1
    return covered / total * 100 if total else 0


def lineage_depth_yaml(cfg: Dict, target: str = "mart.sales_dashboard") -> int:
    graph = defaultdict(list)
    for node, spec in cfg.get("lineage", {}).items():
        for up in spec.get("upstream", []):
            graph[node].append(up)
    q = deque([(target, 0)])
    seen = set()
    max_depth = 0
    while q:
        node, depth = q.popleft()
        if node in seen:
            continue
        seen.add(node)
        max_depth = max(max_depth, depth)
        for up in graph.get(node, []):
            q.append((up, depth + 1))
    return max_depth


def run(config_path: str, out: str | None = None) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    om = MetadataClient(os.getenv("OPENMETADATA_URL", cfg["environment"]["openmetadata"]["url"]))

    if om.available():
        coverage = coverage_from_openmetadata(om)
        search = search_latency_openmetadata(om, SEARCH_QUERIES)
        result = {
            "experiment": "E1_METADATA_DISCOVERABILITY",
            "mode": "openmetadata",
            "metadata_coverage_rate_pct": coverage["coverage_pct"],
            "coverage_detail": coverage,
            "search_latency_median_ms": search["median_ms"],
            "search_detail": search["per_query_hits"],
            "lineage_depth_hops": lineage_depth_openmetadata(om),
        }
    else:
        result = {
            "experiment": "E1_METADATA_DISCOVERABILITY",
            "mode": "offline_yaml_fallback",
            "warning": "OpenMetadata unreachable — values below are a YAML smoke check, not thesis results.",
            "metadata_coverage_rate_pct": round(coverage_from_yaml(cfg), 2),
            "search_latency_median_ms": None,
            "lineage_depth_hops": lineage_depth_yaml(cfg),
        }
    result["baseline_note"] = ("Baseline (conventional Data Lake) has no catalog: coverage 0%, "
                               "no search endpoint, lineage depth 0 by construction (Section 4.4).")
    if out:
        write_json(out, result)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out")
    args = ap.parse_args()
    print(run(args.config, args.out))
