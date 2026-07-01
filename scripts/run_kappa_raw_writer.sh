#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/kappa_flows.yaml}
FLOWS=${FLOWS:-}
ONCE=${ONCE:-false}

ARGS=(--config "$CONFIG" --flows "$FLOWS")
if [[ "$ONCE" == "true" ]]; then
  ARGS+=(--once)
fi

python -m src.jobs.kappa_raw_writer "${ARGS[@]}"
