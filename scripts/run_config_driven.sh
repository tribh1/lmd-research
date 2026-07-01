#!/usr/bin/env bash
set -euo pipefail

CONFIG=${CONFIG:-metadata/config_driven_tables.yaml}
STAGE=${STAGE:-all}
TABLES=${TABLES:-}
MODELS=${MODELS:-}
SPARK_SUBMIT=${SPARK_SUBMIT:-spark-submit}
PACKAGES=${PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1}

CMD=(python src/orchestrator/config_driven_runner.py --config "$CONFIG" --stage "$STAGE" --spark-submit "$SPARK_SUBMIT" --packages "$PACKAGES")
if [[ -n "$TABLES" ]]; then
  CMD+=(--tables "$TABLES")
fi
if [[ -n "$MODELS" ]]; then
  CMD+=(--models "$MODELS")
fi

"${CMD[@]}"
