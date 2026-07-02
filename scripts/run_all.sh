#!/usr/bin/env bash
# End-to-end experiment run (thesis Section 4.5). Assumes `docker compose up -d --build`
# is healthy (including the openmetadata service).
#
# Preferred invocation (inside the spark container, dependencies preinstalled):
#   docker compose exec spark bash -lc "cd /opt/lakehouse && SCALE=small bash scripts/run_all.sh"
# Host invocation also works if spark-submit and requirements.txt are installed locally.
set -euo pipefail
CONF=${CONF:-metadata/pipeline_config.yaml}
SCALE=${SCALE:-small}

# Auto-detect the execution environment: inside the compose network the service
# hostnames resolve directly; from the host we use the published localhost ports
# and override the container hostnames baked into the YAML config via env vars
# (supported by src/common/config.py and src/common/spark_session.py).
if [ -f /.dockerenv ] || getent hosts postgres-source >/dev/null 2>&1; then
  ENV_MODE=container
  PG_DSN=${PG_DSN:-postgresql://lakehouse:lakehouse@postgres-source:5432/source_db}
  export TRINO_HOST=${TRINO_HOST:-trino}
  export TRINO_PORT=${TRINO_PORT:-8080}
  export OPENMETADATA_URL=${OPENMETADATA_URL:-http://openmetadata:8585/api}
else
  ENV_MODE=host
  PG_DSN=${PG_DSN:-postgresql://lakehouse:lakehouse@localhost:5432/source_db}
  export TRINO_HOST=${TRINO_HOST:-localhost}
  export TRINO_PORT=${TRINO_PORT:-8088}
  export OPENMETADATA_URL=${OPENMETADATA_URL:-http://localhost:8585/api}
  export SOURCE_JDBC_URL=${SOURCE_JDBC_URL:-jdbc:postgresql://localhost:5432/source_db}
  export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
  export MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://localhost:9000}
  export LAKEHOUSE_CATALOG_URI=${LAKEHOUSE_CATALOG_URI:-thrift://localhost:9083}
fi
echo "run_all: environment=$ENV_MODE scale=$SCALE"

# 0. Record host environment (Table 4.3).
python scripts/collect_host_env.py --out results/host_environment.json

# 1. Register the catalog in OpenMetadata (control-plane bootstrap).
python -m src.jobs.openmetadata_bootstrap --config "$CONF" --openmetadata-url "$OPENMETADATA_URL"

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
#    (inside the container use --bootstrap kafka:29092)

# 7. Experiments E1-E5 (E5 applies a source schema change and re-runs the pipeline).
python -m src.experiments.run_all --config "$CONF" --out "results/experiment_results_${SCALE}.json"
