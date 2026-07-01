#!/usr/bin/env bash
set -euo pipefail
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @scripts/register_debezium_postgres.json
