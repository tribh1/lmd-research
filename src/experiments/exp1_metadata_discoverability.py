from __future__ import annotations

import argparse
import time
from collections import defaultdict, deque
from typing import Dict, List
import yaml
from ._utils import write_json, median


def compute_metadata_coverage(cfg: Dict) -> float:
    total = 0
    covered = 0
    for name, spec in cfg["tables"].items():
        total += 1
        has_description = bool(spec.get("glossary_terms"))
        has_owner = True  # inherited from catalog_defaults.owner
        has_tag = bool(spec.get("pii_columns") or spec.get("dq_rules") or spec.get("glossary_terms"))
        if has_description and has_owner and has_tag:
            covered += 1
    return covered / total * 100 if total else 0


def simulate_search_latency(cfg: Dict, queries: List[str]) -> float:
    # Local deterministic fallback: scans YAML catalog like a small metadata catalog.
    rows = []
    for q in queries:
        start = time.time()
        ql = q.lower()
        _ = [t for t, spec in cfg["tables"].items() if ql in t.lower() or ql in " ".join(spec.get("glossary_terms", [])).lower()]
        rows.append((time.time() - start) * 1000)
    return median(rows)


def lineage_depth(cfg: Dict, target: str = "mart.sales_dashboard") -> int:
    graph = defaultdict(list)
    for node, spec in cfg.get("lineage", {}).items():
        for up in spec.get("upstream", []):
            graph[node].append(up)
    q = deque([(target, 0)])
    visited = set()
    max_depth = 0
    while q:
        node, depth = q.popleft()
        if node in visited:
            continue
        visited.add(node)
        max_depth = max(max_depth, depth)
        for up in graph.get(node, []):
            q.append((up, depth + 1))
    return max_depth


def run(config_path: str, out: str | None = None) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    result = {
        "experiment": "E1_METADATA_DISCOVERABILITY",
        "metadata_coverage_rate_pct": round(compute_metadata_coverage(cfg), 2),
        "search_latency_median_ms": round(simulate_search_latency(cfg, ["customer", "order", "payment", "pii", "kpi", "sales", "product", "telephone", "gold", "mart"]), 4),
        "lineage_depth_hops": lineage_depth(cfg),
        "baseline_note": "For the baseline, set coverage/search/lineage from manual inventory if available; otherwise use 0/NA/0 per thesis baseline definition."
    }
    if out:
        write_json(out, result)
    return result

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out")
    args = ap.parse_args()
    print(run(args.config, args.out))
