#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/kappa_flows.yaml}
FLOWS=${FLOWS:-}
FROM_TS=${FROM_TS:-}
TO_TS=${TO_TS:-}
LIMIT=${LIMIT:-0}
BATCH_ID=${BATCH_ID:-0}
RESULTS=${RESULTS:-results/work_to_silver_results.json}

ARGS=(--config "$CONFIG" --flows "$FLOWS" --limit "$LIMIT" --batch-id "$BATCH_ID" --results "$RESULTS")

if [[ -n "$FROM_TS" ]]; then
  ARGS+=(--from-ts "$FROM_TS")
fi

if [[ -n "$TO_TS" ]]; then
  ARGS+=(--to-ts "$TO_TS")
fi

python -m src.jobs.kappa_work_to_silver "${ARGS[@]}"
