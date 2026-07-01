#!/usr/bin/env bash
set -euo pipefail
CONFIG=${CONFIG:-metadata/dashboard_metrics.yaml}
python -m src.jobs.experiment_dashboard_builder --config "$CONFIG"
