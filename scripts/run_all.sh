#!/usr/bin/env bash
set -euo pipefail
CONF=${CONF:-metadata/pipeline_config.yaml}
SCALE=${SCALE:-small}

python scripts/generate_data.py --scale "$SCALE" --out "data/generated/$SCALE"
python scripts/load_csv_to_postgres.py --input "data/generated/$SCALE" --dsn postgresql://lakehouse:lakehouse@localhost:5432/source_db

for table in customers products orders order_items payments; do
  spark-submit src/jobs/01_batch_ingest_raw.py --config "$CONF" --table "$table" --scale "$SCALE"
  spark-submit src/jobs/02_work_to_silver.py --config "$CONF" --table "$table"
done

spark-submit src/jobs/05_gold_mart.py --config "$CONF"
python -m src.experiments.run_all --config "$CONF" --out results/experiment_results.json
