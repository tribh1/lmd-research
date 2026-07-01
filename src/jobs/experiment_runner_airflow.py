from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List


def run_step(name: str, command: List[str], cwd: str | None = None, continue_on_error: bool = False) -> Dict[str, Any]:
    started = time.time()
    proc = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    result = {
        "name": name,
        "command": command,
        "returncode": proc.returncode,
        "elapsed_seconds": round(time.time() - started, 3),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }
    if proc.returncode != 0 and not continue_on_error:
        raise RuntimeError(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Run thesis experiment workflow for Airflow or command line")
    ap.add_argument("--output", default="results/experiment_results.json")
    ap.add_argument("--continue-on-error", action="store_true")
    args = ap.parse_args()

    steps = [
        ("metadata_summary", ["python", "-m", "src.jobs.kappa_config_pipeline", "--mode", "summary"]),
        ("sync_openmetadata", ["python", "-m", "src.jobs.openmetadata_sync", "--print-summary"]),
        ("reconcile_unknown_fk", ["python", "-m", "src.jobs.reconcile_unknown_fk"]),
        ("run_gold_models", ["python", "-m", "src.jobs.gold_model_runner"]),
        ("run_exp1_metadata", ["python", "-m", "src.experiments.exp1_metadata_discoverability"]),
        ("run_exp2_governance", ["python", "-m", "src.experiments.exp2_governance_coverage"]),
        ("run_exp3_processing", ["python", "-m", "src.experiments.exp3_processing_scalability"]),
        ("run_exp4_query", ["python", "-m", "src.experiments.exp4_query_performance"]),
        ("run_exp5_schema", ["python", "-m", "src.experiments.exp5_schema_evolution"]),
        ("build_dashboard", ["python", "-m", "src.jobs.experiment_dashboard_builder"]),
    ]

    results = []
    for name, cmd in steps:
        try:
            results.append(run_step(name, cmd, continue_on_error=args.continue_on_error))
        except Exception as exc:
            results.append({"name": name, "status": "failed", "error": repr(exc)})
            if not args.continue_on_error:
                break

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump({"generated_at_ms": int(time.time() * 1000), "steps": results}, f, indent=2, ensure_ascii=False)
    print(json.dumps({"output": args.output, "steps": len(results)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
