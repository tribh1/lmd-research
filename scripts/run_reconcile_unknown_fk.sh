#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/reconciliation_jobs.yaml}
JOBS=${JOBS:-}
ARGS=(--config "$CONFIG")
if [[ -n "$JOBS" ]]; then
  ARGS+=(--jobs "$JOBS")
fi
python -m src.jobs.reconcile_unknown_fk "${ARGS[@]}"
