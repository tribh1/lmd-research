#!/usr/bin/env bash
# End-to-end experiment run (thesis Section 4.5). Assumes `docker compose up -d`
# is healthy (including the openmetadata service) and spark-submit is available.
set -euo pipefail
CONF=${CONF:-metadata/pipeline_config.yaml}
SCALE=${SCALE:-small}
PG_DSN=${PG_DSN:-postgresql://lakehouse:lakehouse@localhost:5432/source_db}

# 0. Record host environment (Table 4.3).
python scripts/collect_host_env.py --out results/host_environment.json

# 1. Register the catalog in OpenMetadata (control-plane bootstrap).
python -m src.jobs.openmetadata_bootstrap --config "$CONF"

# 2. Generate and load the synthetic dataset (Table 4.4).
python scripts/generate_data.py --scale "$SCALE" --out "data/generated/$SCALE"
python scripts/load_csv_to_postgres.py --input "data/generated/$SCALE" --dsn "$PG_DSN"

# 3. Proposed pipeline: raw -> work -> silver (embedded governance) -> gold/mart.
spark-submit src/jobs/ingest_ground_truth.py --config "$CONF"
for table in customers products orders order_items payments; do
  spark-submit src/jobs/01_batch_ingest_raw.py --config "$CONF" --table "$table" --scale "$SCALE"
  spark-submit src/jobs/02_work_to_silver.py --config "$CONF" --table "$table"
done
spark-submit src/jobs/05_gold_mart.py --config "$CONF"

# 4. Baseline A: plain partitioned Parquet Data Lake on the same substrate.
for table in customers products orders order_items payments; do
  spark-submit src/jobs/baseline_ingest.py --config "$CONF" --table "$table" --scale "$SCALE"
done
python scripts/register_baseline_trino.py --config "$CONF"

# 5. Verify actual on-storage volume per layer for this scale.
spark-submit src/jobs/measure_layer_sizes.py --config "$CONF" --scale "$SCALE"

# 6. Streaming (Experiment 3): start the consumer, then drive the 5-minute load.
#    Run these in two terminals before invoking exp3, e.g.:
#      spark-submit src/jobs/04_stream_events.py --config $CONF
#      python scripts/produce_events.py --bootstrap localhost:9092 --rate 5000 --duration 300

# 7. Experiments E1-E5 (E5 applies a source schema change and re-runs the pipeline).
python -m src.experiments.run_all --config "$CONF" --out "results/experiment_results_${SCALE}.json"
