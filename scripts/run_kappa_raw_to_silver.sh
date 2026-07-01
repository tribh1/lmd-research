#!/usr/bin/env bash
set -euo pipefail

echo "DEPRECATED: v7 uses separate jobs. Running Raw -> Work then Work -> Silver."
./scripts/run_kappa_raw_to_work.sh
./scripts/run_kappa_work_to_silver.sh
