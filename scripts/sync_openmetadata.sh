#!/usr/bin/env bash
set -euo pipefail

KAPPA_CONFIG=${KAPPA_CONFIG:-metadata/kappa_flows.yaml}
OPENMETADATA_CONFIG=${OPENMETADATA_CONFIG:-metadata/openmetadata_config.yaml}
PRINT_SUMMARY=${PRINT_SUMMARY:-false}

ARGS=(--kappa-config "$KAPPA_CONFIG" --openmetadata-config "$OPENMETADATA_CONFIG")
if [[ "$PRINT_SUMMARY" == "true" ]]; then
  ARGS+=(--print-summary)
fi

python -m src.jobs.openmetadata_sync "${ARGS[@]}"
