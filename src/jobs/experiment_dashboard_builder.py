from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_json(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def flatten_metrics(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            flatten_metrics(f"{prefix}.{k}" if prefix else str(k), v, out)
    elif isinstance(obj, list):
        out[f"{prefix}.count"] = len(obj)
        # Aggregate common fields in list of dicts.
        if obj and all(isinstance(x, dict) for x in obj):
            for key in ["rows", "failed_rows", "elapsed_seconds", "row_count", "lineage_edge_count"]:
                values = [x.get(key) for x in obj if isinstance(x.get(key), (int, float))]
                if values:
                    out[f"{prefix}.{key}.sum"] = sum(values)
                    out[f"{prefix}.{key}.max"] = max(values)
    else:
        out[prefix] = obj


def collect_openmetadata_fallback_metrics(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"openmetadata_fallback_events": 0}
    files = list(p.glob("*.json"))
    kinds: Dict[str, int] = {}
    for file in files:
        name = file.name
        # file format: timestamp_kind.json
        kind = name.split("_", 1)[1].rsplit(".json", 1)[0] if "_" in name else "unknown"
        kinds[kind] = kinds.get(kind, 0) + 1
    return {"openmetadata_fallback_events": len(files), "openmetadata_fallback_by_kind": kinds}


def build_markdown(summary: Dict[str, Any], sections: List[Dict[str, Any]]) -> str:
    lines = [
        "# Lakehouse Kappa Experiment Dashboard",
        "",
        "This report is generated from experiment, Gold/Mart, reconciliation and OpenMetadata fallback artifacts.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for k in sorted(summary):
        v = summary[k]
        if isinstance(v, (dict, list)):
            continue
        lines.append(f"| `{k}` | {v} |")

    lines.extend(["", "## OpenMetadata fallback events", ""])
    by_kind = summary.get("openmetadata_fallback_by_kind", {}) or {}
    if by_kind:
        lines.extend(["| Kind | Count |", "|---|---:|"])
        for k, v in sorted(by_kind.items()):
            lines.append(f"| `{k}` | {v} |")
    else:
        lines.append("No fallback OpenMetadata event files found.")

    lines.extend(["", "## Configured Dashboard Sections", ""])
    for section in sections:
        lines.append(f"### {section.get('name', 'Section')}")
        lines.append("")
        for metric in section.get("metrics", []) or []:
            matched = {k: v for k, v in summary.items() if metric in k}
            if not matched:
                lines.append(f"- `{metric}`: not found in current artifacts")
            else:
                for k, v in matched.items():
                    if not isinstance(v, (dict, list)):
                        lines.append(f"- `{k}`: {v}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build markdown/json dashboard from experiment artifacts")
    ap.add_argument("--config", default="metadata/dashboard_metrics.yaml")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    rt = cfg.get("runtime", {}) or {}
    output_dir = Path(rt.get("output_dir", "results/dashboard"))
    output_dir.mkdir(parents=True, exist_ok=True)

    artifacts = {
        "experiment": load_json(rt.get("experiment_results", "results/experiment_results.json")),
        "gold": load_json(rt.get("gold_model_results", "results/gold_models/gold_model_results.json")),
        "reconciliation": load_json(rt.get("reconciliation_results", "results/reconciliation_results.json")),
    }

    summary: Dict[str, Any] = {}
    for name, obj in artifacts.items():
        flatten_metrics(name, obj, summary)
    summary.update(collect_openmetadata_fallback_metrics(rt.get("openmetadata_events_dir", "results/openmetadata_events")))

    json_path = output_dir / "dashboard_summary.json"
    md_path = output_dir / "dashboard.md"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)

    md = build_markdown(summary, cfg.get("sections", []) or [])
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(json.dumps({"status": "success", "json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
