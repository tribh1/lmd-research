#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/kappa_batch_sources.yaml}
JOBS=${JOBS:-}
BATCH_RUN_ID=${BATCH_RUN_ID:-}
FROM_TS=${FROM_TS:-}
TO_TS=${TO_TS:-}
DRY_RUN=${DRY_RUN:-false}

ARGS=(--config "$CONFIG")

if [[ -n "$JOBS" ]]; then
  ARGS+=(--jobs "$JOBS")
fi
if [[ -n "$BATCH_RUN_ID" ]]; then
  ARGS+=(--batch-run-id "$BATCH_RUN_ID")
fi
if [[ -n "$FROM_TS" ]]; then
  ARGS+=(--from-ts "$FROM_TS")
fi
if [[ -n "$TO_TS" ]]; then
  ARGS+=(--to-ts "$TO_TS")
fi
if [[ "$DRY_RUN" == "true" ]]; then
  ARGS+=(--dry-run)
fi

python -m src.jobs.kappa_batch_to_event "${ARGS[@]}"
