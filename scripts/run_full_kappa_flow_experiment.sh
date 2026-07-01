#!/usr/bin/env bash
set -euo pipefail

SCALE=${SCALE:-small}
SOURCE_DSN=${SOURCE_DSN:-postgresql://lakehouse:lakehouse@postgres-source:5432/source_db}
BATCH_RUN_ID=${BATCH_RUN_ID:-github_actions_full_flow}
KAPPA_CONFIG=${KAPPA_CONFIG:-metadata/kappa_flows.yaml}
BATCH_CONFIG=${BATCH_CONFIG:-metadata/kappa_batch_sources.yaml}
GOLD_CONFIG=${GOLD_CONFIG:-metadata/gold_models.yaml}
FLOW_RESULTS_DIR=${FLOW_RESULTS_DIR:-results/full_kappa_flow}

mkdir -p "$FLOW_RESULTS_DIR"

python scripts/generate_data.py --scale "$SCALE" --out "data/generated/$SCALE"
python scripts/load_csv_to_postgres.py \
  --input "data/generated/$SCALE" \
  --dsn "$SOURCE_DSN"

CONFIG="$KAPPA_CONFIG" MODE=summary ./scripts/run_kappa_config.sh \
  > "$FLOW_RESULTS_DIR/00_metadata_summary.json"

CONFIG="$BATCH_CONFIG" BATCH_RUN_ID="$BATCH_RUN_ID" ./scripts/run_kappa_batch_publish.sh \
  | tee "$FLOW_RESULTS_DIR/01_source_to_kafka.log"

CONFIG="$KAPPA_CONFIG" ONCE=true ./scripts/run_kappa_raw_writer.sh \
  | tee "$FLOW_RESULTS_DIR/02_kafka_to_raw.log"

CONFIG="$KAPPA_CONFIG" RESULTS="$FLOW_RESULTS_DIR/03_raw_to_work.json" ./scripts/run_kappa_raw_to_work.sh
CONFIG="$KAPPA_CONFIG" RESULTS="$FLOW_RESULTS_DIR/04_work_to_silver.json" ./scripts/run_kappa_work_to_silver.sh

CONFIG="$GOLD_CONFIG" LAYERS=gold MODE=run ./scripts/run_gold_models.sh \
  | tee "$FLOW_RESULTS_DIR/05_silver_to_gold.log"

CONFIG="$GOLD_CONFIG" LAYERS=mart MODE=run ./scripts/run_mart_models.sh \
  | tee "$FLOW_RESULTS_DIR/06_gold_to_mart.log"

python -m src.experiments.run_all \
  --config metadata/pipeline_config.yaml \
  --out "$FLOW_RESULTS_DIR/07_experiment_results.json"
