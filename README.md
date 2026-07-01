# Metadata-Driven Kappa Lakehouse Prototype

This repository contains a thesis-oriented prototype of a **Strict Layered Metadata-Driven Kappa Lakehouse Architecture**.

The prototype demonstrates how batch snapshots, CDC events, and streaming events can be unified through Kafka and processed through a strict layered Lakehouse pipeline:

```text
Batch Snapshot / Backfill -> Kafka Event
CDC / Streaming Event      -> Kafka Event
Kafka -> Raw -> Work -> Silver/Quarantine -> Gold -> Data Mart
```

The system is designed around these principles:

- **Kappa-oriented ingestion**: batch, CDC, and streaming are normalized into events.
- **Strict layered jobs**: each Lakehouse layer transition is implemented as a separate physical job.
- **Metadata-driven control plane**: YAML config drives schemas, mappings, standardization, DQ, PII, SK/FK, SCD, Gold/Mart models, and lineage.
- **Embedded metadata**: operational and governance metadata is embedded into each Lakehouse record.
- **Embedded governance**: data quality, quarantine, PII masking, SCD, and surrogate keys are enforced in the pipeline.
- **OpenMetadata integration**: assets, lineage, embedded metadata fields, tags, DQ results, and runtime metrics can be published to OpenMetadata.
- **Airflow orchestration**: Airflow coordinates jobs only; Spark/Trino perform data processing.

## Architecture

```text
Source DB / Snapshot / Backfill
        |
        v
Batch-as-event Publisher / Debezium CDC
        |
        v
Kafka topics by table
        |
        v
Job 1: Kafka -> Raw
        |
        v
Job 2: Raw -> Work
        |
        v
Job 3: Work -> Silver / Quarantine
        |
        v
Job 4: Reconciliation
        |
        v
Job 5: Silver -> Gold
        |
        v
Job 6: Gold -> Data Mart
        |
        v
Experiment Dashboard / OpenMetadata / BI
```

## Repository structure

```text
metadata/       YAML configuration files for Kappa flows, batch sources, Gold/Mart models, OpenMetadata, reconciliation, dashboard metrics
src/common/     Shared runtime modules: registry, transformation, DQ, masking, SCD merge, OpenMetadata emitter, layer processor
src/jobs/       Executable jobs for each strict layer transition
src/experiments Experiment scripts for thesis evaluation
scripts/        Shell entrypoints for running the pipeline
sql/            Source schema and benchmark SQL
trino/          Trino catalog configuration
airflow/dags/   Airflow DAG for strict layered orchestration
docs/           Design, setup, and component documentation
results/        Runtime output folder, ignored by Git except placeholders
```

## Main documentation

Start with these documents:

1. [`docs/DESIGN_AND_SETUP_GUIDE.md`](docs/DESIGN_AND_SETUP_GUIDE.md) — end-to-end design, environment setup, execution guide.
2. [`docs/STRICT_LAYERED_JOB_EXECUTION_DESIGN.md`](docs/STRICT_LAYERED_JOB_EXECUTION_DESIGN.md) — final v7 strict layered job design.
3. [`docs/KAPPA_BATCH_AS_EVENT_FLOW.md`](docs/KAPPA_BATCH_AS_EVENT_FLOW.md) — batch-as-event design.
4. [`docs/KAPPA_CONFIG_DRIVEN_FLOW.md`](docs/KAPPA_CONFIG_DRIVEN_FLOW.md) — Kappa metadata-driven flow.
5. [`docs/OPENMETADATA_EMBEDDED_METADATA_INTEGRATION.md`](docs/OPENMETADATA_EMBEDDED_METADATA_INTEGRATION.md) — OpenMetadata integration.
6. [`docs/EXPERIMENT_DATA_DESIGN.md`](docs/EXPERIMENT_DATA_DESIGN.md) — experiment dataset and metrics.
7. [`docs/GITHUB_UPLOAD_GUIDE.md`](docs/GITHUB_UPLOAD_GUIDE.md) — how to publish this repository to GitHub.

## Quick start

Copy the example environment file:

```bash
cp .env.example .env
```

Start infrastructure:

```bash
docker compose up -d
```

Create source schema and seed sample data:

```bash
psql postgresql://lakehouse:lakehouse@localhost:5432/source_db \
  -f sql/01_source_schema.sql

python scripts/generate_data.py --scale small --out data/generated/small

python scripts/load_csv_to_postgres.py \
  --input data/generated/small \
  --dsn postgresql://lakehouse:lakehouse@localhost:5432/source_db
```

Run the strict layered pipeline:

```bash
# 0. Validate metadata
MODE=summary ./scripts/run_kappa_config.sh

# 1. Batch snapshot/backfill -> Kafka events
./scripts/run_kappa_batch_publish.sh

# 2. Kafka -> Raw
MODE=stream-raw-only ./scripts/run_kappa_config.sh

# 3. Raw -> Work
./scripts/run_kappa_raw_to_work.sh

# 4. Work -> Silver/Quarantine
./scripts/run_kappa_work_to_silver.sh

# 5. Reconcile late-arriving foreign keys
./scripts/run_reconcile_unknown_fk.sh

# 6. Silver -> Gold
./scripts/run_gold_models.sh

# 7. Gold -> Data Mart
./scripts/run_mart_models.sh

# 8. Dashboard
./scripts/run_build_dashboard.sh
```

## Airflow orchestration

The DAG is available at:

```text
airflow/dags/lakehouse_kappa_airflow_dag.py
```

DAG id:

```text
lakehouse_kappa_strict_layered_dag
```

## Thesis positioning

This prototype is intended to support a thesis/prototype chapter on:

```text
Strict Layered Metadata-Driven Kappa Lakehouse Architecture
```

The key research contribution is not a monolithic ETL job, but a metadata-driven Kappa architecture where the physical jobs are separated by Lakehouse layer while sharing centralized transformation and governance semantics.
