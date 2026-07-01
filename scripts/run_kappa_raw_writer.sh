#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/kappa_flows.yaml}
FLOWS=${FLOWS:-}

python -m src.jobs.kappa_raw_writer \
  --config "$CONFIG" \
  --flows "$FLOWS"
