#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/kappa_flows.yaml}
FLOWS=${FLOWS:-}
MODE=${MODE:-stream-raw-only}
MODELS=${MODELS:-}

python -m src.jobs.kappa_config_pipeline \
  --config "$CONFIG" \
  --flows "$FLOWS" \
  --models "$MODELS" \
  --mode "$MODE"
