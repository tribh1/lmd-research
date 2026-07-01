# Changelog

## v7 — Strict layered jobs

- Enforced one physical job per Lakehouse layer transition.
- Added `kappa_raw_to_work.py` for Raw -> Work.
- Added `kappa_work_to_silver.py` for Work -> Silver/Quarantine.
- Updated Airflow DAG to strict layered orchestration.
- Added `DESIGN_AND_SETUP_GUIDE.md`.
- Added GitHub-ready repository documentation.

## v6 — Layered execution option

- Added optional raw-only and raw-to-silver execution profiles.
- Introduced reusable `KappaLayerProcessor`.

## v5 — Gold/Mart and dashboard

- Added config-driven Gold/Mart model runner.
- Added dashboard builder.

## v4 — OpenMetadata and reconciliation

- Added OpenMetadata emitter.
- Added late-arriving FK reconciliation.

## v3 — Batch-as-event

- Added batch snapshot/backfill publisher to Kafka.

## v2 — SCD/SK/FK

- Added SCD Type 1/2 merge logic, deterministic surrogate keys, delete handling.

## v1 — Kappa config-driven prototype

- Initial metadata-driven Kappa pipeline.
