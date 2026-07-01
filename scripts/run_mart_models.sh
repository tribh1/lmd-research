#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/gold_models.yaml}
MODELS=${MODELS:-}
LAYERS=${LAYERS:-mart}
MODE=${MODE:-run}

python -m src.jobs.gold_model_runner \
  --config "$CONFIG" \
  --models "$MODELS" \
  --layers "$LAYERS" \
  --mode "$MODE"
