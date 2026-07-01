# OpenMetadata Integration for Embedded Metadata Kappa Pipeline

This package integrates the metadata-driven Kappa pipeline with OpenMetadata.
The integration is intentionally defensive: when OpenMetadata is offline, the
payloads are written to `results/openmetadata_events` so that the experiment is
still reproducible.

## 1. What is registered

The integration registers the following assets:

1. Kafka topics used by CDC and batch-as-event flows.
2. Iceberg Raw tables.
3. Iceberg Work tables.
4. Iceberg Silver tables.
5. Iceberg Quarantine tables.
6. Gold/Mart SQL models.
7. Lineage edges:
   - Kafka Topic -> Raw
   - Raw -> Work
   - Work -> Silver
   - Work -> Quarantine
   - Silver -> Gold/Mart
8. Data quality results per micro-batch.
9. Runtime metrics per micro-batch.

## 2. Embedded metadata columns

The OpenMetadata table payload includes both business columns and embedded
metadata columns. The embedded metadata columns are treated as first-class
columns with the tag `Governance.EmbeddedMetadata`.

Typical embedded metadata fields:

```text
_meta_event_id
_meta_source_system
_meta_source_database
_meta_source_schema
_meta_source_table
_meta_source_operation
_meta_source_ts_ms
_meta_kafka_topic
_meta_kafka_partition
_meta_kafka_offset
_meta_ingest_ts
_meta_config_version
_meta_pipeline_name
_meta_layer
_meta_record_hash
_meta_schema_hash
_meta_dq_errors
_meta_pii_tags
_meta_lineage
_meta_micro_batch_id
_meta_is_deleted
_meta_deleted_at
_meta_closed_by_event_id
_meta_closed_at
_meta_reconciled_at
_meta_reconciled_by
```

## 3. Why embedded metadata matters

The record-level embedded metadata bridges the gap between runtime processing
and catalog governance:

- `_meta_event_id` supports event-level traceability.
- `_meta_kafka_topic`, `_meta_kafka_partition`, and `_meta_kafka_offset`
  support replay and audit.
- `_meta_record_hash` supports idempotent replay and SCD2 change detection.
- `_meta_source_operation` preserves CDC semantics.
- `_meta_dq_errors` records failed data quality rules.
- `_meta_pii_tags` preserves classification context.
- `_meta_lineage` carries record-level lineage context.
- `_meta_config_version` links each processed record to the configuration
  version that produced it.

OpenMetadata receives the column contract, table custom properties, lineage
edges, quality metrics and batch metrics. The raw record still carries metadata
inside Iceberg so that governance does not depend only on an external catalog.

## 4. Configuration

OpenMetadata settings are stored in:

```text
metadata/openmetadata_config.yaml
```

Common environment variables:

```bash
export OPENMETADATA_URL=http://openmetadata:8585/api/v1
export OPENMETADATA_JWT_TOKEN=<token-if-required>
```

## 5. Register assets and lineage

```bash
PRINT_SUMMARY=true ./scripts/sync_openmetadata.sh
```

If OpenMetadata is not running, generated payloads are written to:

```text
results/openmetadata_events/
```

## 6. Runtime integration

`src/jobs/kappa_config_pipeline.py` creates `OpenMetadataEmitter` at startup.
For each micro-batch, the pipeline:

1. Registers flow assets and static lineage if needed.
2. Emits DQ rule results.
3. Emits micro-batch metrics.
4. Registers Gold/Mart models and model lineage in `models-once` mode.

## 7. Late-arriving dimension reconciliation

The reconciliation job resolves fact rows with unknown foreign keys, for example
`customer_sk = -1`, after the dimension arrives later.

```bash
./scripts/run_reconcile_unknown_fk.sh
```

The job updates the fact table and writes audit metrics to:

```text
lakehouse.audit.reconciliation_metrics
```

## 8. Airflow orchestration

The Airflow DAG now includes:

```text
validate_kappa_flow_config
-> sync_static_openmetadata_assets
-> publish_initial_snapshot_as_events
-> start_kappa_streaming_job
-> reconcile_late_arriving_foreign_keys
-> build_gold_models_once
-> run_experiment_suite
```

Airflow remains the orchestration layer only. Spark remains the execution
engine for data processing, and OpenMetadata remains the catalog/governance hub.
