#!/usr/bin/env bash
set -euo pipefail

OUTPUT=${OUTPUT:-results/experiment_results.json}
CONTINUE_ON_ERROR=${CONTINUE_ON_ERROR:-true}
ARGS=(--output "$OUTPUT")
if [[ "$CONTINUE_ON_ERROR" == "true" ]]; then
  ARGS+=(--continue-on-error)
fi
python -m src.jobs.experiment_runner_airflow "${ARGS[@]}"
