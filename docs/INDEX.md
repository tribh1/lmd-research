# Documentation Index

This folder contains the design and setup documentation for the Metadata-Driven Kappa Lakehouse prototype.

## Recommended reading order

1. `DESIGN_AND_SETUP_GUIDE.md` — full design and setup guide.
2. `STRICT_LAYERED_JOB_EXECUTION_DESIGN.md` — final v7 strict layered architecture.
3. `KAPPA_BATCH_AS_EVENT_FLOW.md` — batch-as-event design.
4. `KAPPA_CONFIG_DRIVEN_FLOW.md` — Kappa config-driven processing model.
5. `KAPPA_SCD2_MERGE_DESIGN.md` — Silver merge, SCD, SK/FK handling.
6. `OPENMETADATA_EMBEDDED_METADATA_INTEGRATION.md` — catalog/governance integration.
7. `GOLD_MART_AND_DASHBOARD.md` — Gold/Mart models and experiment dashboard.
8. `EXPERIMENT_DATA_DESIGN.md` — experiment data and metrics.
9. `CONFIG_DRIVEN_PIPELINE.md` — earlier generic config-driven pipeline design.
10. `LAYERED_JOB_EXECUTION_DESIGN.md` — v6 layered design retained for historical comparison.
11. `GITHUB_UPLOAD_GUIDE.md` — GitHub publishing guide.

## Key implementation files

| Area | Files |
|---|---|
| Metadata config | `metadata/*.yaml` |
| Kafka -> Raw | `src/jobs/kappa_config_pipeline.py`, `scripts/run_kappa_config.sh` |
| Raw -> Work | `src/jobs/kappa_raw_to_work.py`, `scripts/run_kappa_raw_to_work.sh` |
| Work -> Silver | `src/jobs/kappa_work_to_silver.py`, `scripts/run_kappa_work_to_silver.sh` |
| Shared layer logic | `src/common/kappa_layer_processor.py` |
| SCD/SK/FK merge | `src/common/kappa_merge.py` |
| OpenMetadata | `src/common/kappa_openmetadata.py`, `src/jobs/openmetadata_sync.py` |
| Gold/Mart | `src/jobs/gold_model_runner.py`, `metadata/gold_models.yaml` |
| Airflow | `airflow/dags/lakehouse_kappa_airflow_dag.py` |
