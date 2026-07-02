#!/usr/bin/env bash
# Ablation configurations (thesis Table 4.3a). Each variant should be executed
# against a RESET lakehouse (fresh buckets/warehouse) so timings do not mix:
#   docker compose down -v && docker compose up -d   # between variants
#
#   VARIANT=baseline_a|iceberg_only|full|no_work_layer SCALE=1gb bash scripts/run_ablation.sh
set -euo pipefail
CONF=${CONF:-metadata/pipeline_config.yaml}
SCALE=${SCALE:-small}
VARIANT=${VARIANT:-full}
TABLES="customers products orders order_items payments"

case "$VARIANT" in
  baseline_a)
    for t in $TABLES; do
      spark-submit src/jobs/baseline_ingest.py --config "$CONF" --table "$t" --scale "$SCALE"
    done
    ;;
  iceberg_only)
    for t in $TABLES; do
      spark-submit src/jobs/01_batch_ingest_raw.py --config "$CONF" --table "$t" --scale "$SCALE"
      spark-submit src/jobs/02_work_to_silver.py --config "$CONF" --table "$t" --no-governance
    done
    ;;
  no_work_layer)
    for t in $TABLES; do
      spark-submit src/jobs/01_batch_ingest_raw.py --config "$CONF" --table "$t" --scale "$SCALE"
      spark-submit src/jobs/02_work_to_silver.py --config "$CONF" --table "$t" --skip-work-layer
    done
    ;;
  full)
    for t in $TABLES; do
      spark-submit src/jobs/01_batch_ingest_raw.py --config "$CONF" --table "$t" --scale "$SCALE"
      spark-submit src/jobs/02_work_to_silver.py --config "$CONF" --table "$t"
    done
    ;;
  *)
    echo "Unknown VARIANT=$VARIANT (expected baseline_a|iceberg_only|full|no_work_layer)"; exit 1;;
esac
echo "Ablation variant '$VARIANT' completed at scale '$SCALE'."
