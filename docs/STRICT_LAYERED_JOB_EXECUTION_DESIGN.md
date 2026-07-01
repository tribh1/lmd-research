# Strict Layered Job Execution Design v7

## Design decision

Version 7 enforces one physical job per lakehouse layer transition. The goal is to avoid a monolithic ETL job while preserving one shared metadata-driven Kappa semantics.

## Physical jobs

| Job | Layer transition | Main file |
|---|---|---|
| Batch-as-event publisher | Source snapshot/backfill -> Kafka | `src/jobs/kappa_batch_to_event.py` |
| Raw Writer | Kafka -> Raw | `src/jobs/kappa_config_pipeline.py` with `MODE=stream-raw-only` |
| Raw-to-Work Processor | Raw -> Work | `src/jobs/kappa_raw_to_work.py` |
| Work-to-Silver Processor | Work -> Silver/Quarantine | `src/jobs/kappa_work_to_silver.py` |
| Reconciliation | Silver -> corrected Silver | `src/jobs/reconcile_unknown_fk.py` |
| Gold Runner | Silver -> Gold | `src/jobs/gold_model_runner.py` with `LAYERS=gold` |
| Mart Runner | Gold -> Mart | `src/jobs/gold_model_runner.py` with `LAYERS=mart` |
| OpenMetadata Sync | Config/runtime metadata -> OpenMetadata | `src/jobs/openmetadata_sync.py` |
| Dashboard Builder | Audit/experiment metrics -> dashboard | `src/jobs/experiment_dashboard_builder.py` |

## Execution order

```text
Batch snapshot/backfill -> Kafka
CDC -> Kafka
Kafka -> Raw
Raw -> Work
Work -> Silver/Quarantine
Silver -> Reconciliation
Silver -> Gold
Gold -> Mart
Audit/metrics -> Dashboard
```

## Why this still follows Kappa

Batch data is still converted to bounded events and goes through Kafka. The strict separation applies to physical jobs and table transitions, not to separate batch-vs-stream business logic.

## Why this is better for governance

- Raw can be replayed without Kafka.
- Work can be inspected before Silver governance.
- DQ/PII/SCD runs only after Work is materialized.
- Gold and Mart can be rebuilt without reprocessing ingestion.
- Airflow can retry each layer transition independently.
