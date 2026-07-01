# Layered Job Execution Design for Kappa Metadata-Driven Lakehouse

## 1. Design decision

The prototype does not implement the entire `Raw -> Work -> Silver -> Gold -> Data Mart` lifecycle as one monolithic job.
It keeps one unified Kappa processing semantics, but separates physical jobs by responsibility.

## 2. Two execution profiles

### 2.1 Prototype unified profile

Use this profile for thesis experiments and demos where simplicity and low-latency end-to-end processing are preferred.

```text
Batch snapshot/backfill -> Kafka topic
CDC -> Kafka topic
Kafka -> Spark Structured Streaming
        -> Raw
        -> Work
        -> Quarantine/Silver
Silver -> Reconciliation -> Gold/Mart -> Dashboard
```

Command:

```bash
MODE=stream-full ./scripts/run_kappa_config.sh
```

### 2.2 Production layered profile

Use this profile when ingestion SLA and Silver governance SLA must be operated independently.

```text
Kafka -> Raw Writer Stream -> Raw Iceberg
Raw Iceberg -> Raw-to-Silver Processor -> Work/Quarantine/Silver
Silver -> Reconciliation -> Gold/Mart -> Dashboard
```

Commands:

```bash
MODE=stream-raw-only ./scripts/run_kappa_config.sh
./scripts/run_kappa_raw_to_silver.sh
```

## 3. Physical jobs

| Job | Responsibility | Input | Output | Main file |
|---|---|---|---|---|
| Batch-as-event publisher | Convert snapshot/backfill to event stream | Source DB | Kafka topic | `src/jobs/kappa_batch_to_event.py` |
| Raw writer | Consume Kafka and write Raw | Kafka topics | Raw Iceberg | `src/jobs/kappa_raw_writer.py` |
| Unified Kappa processor | Kafka -> Raw -> Work -> Silver | Kafka topics | Raw/Work/Silver/Quarantine | `src/jobs/kappa_config_pipeline.py` |
| Raw-to-Silver processor | Replay/process Raw into Work/Silver | Raw Iceberg | Work/Silver/Quarantine | `src/jobs/kappa_raw_to_silver.py` |
| Reconciliation | Resolve late-arriving dimension | Silver fact/dim | Updated fact | `src/jobs/reconcile_unknown_fk.py` |
| Gold/Mart runner | Build analytical serving models | Silver/Gold | Gold/Mart | `src/jobs/gold_model_runner.py` |
| OpenMetadata sync | Register assets and lineage | Config/metrics | OpenMetadata/fallback | `src/jobs/openmetadata_sync.py` |
| Dashboard builder | Generate experiment dashboard | Results | Markdown/JSON dashboard | `src/jobs/experiment_dashboard_builder.py` |

## 4. Why Raw/Work/Silver can be one job in the prototype

Raw, Work and Silver belong to the low-latency event processing path. A single Spark Structured Streaming `foreachBatch` can:

1. write Raw append-only data;
2. standardize into Work;
3. validate quality;
4. route failed records to Quarantine;
5. mask PII;
6. generate deterministic surrogate keys;
7. merge SCD1/SCD2/fact into Silver.

This reduces latency and keeps the thesis prototype compact.

## 5. Why Gold/Data Mart are separate jobs

Gold and Data Mart layers have different operational characteristics:

- they serve BI/analytics workloads;
- they are often refreshed by schedule or incrementally;
- they require query optimization and model-level quality checks;
- they should be rebuildable without replaying Kafka CDC;
- they should not block Raw ingestion or Silver governance.

Therefore Gold/Mart is implemented by `gold_model_runner.py`, driven by `metadata/gold_models.yaml`.

## 6. Configuration

`metadata/job_execution_plan.yaml` documents the two execution profiles and the contract of each physical job.

## 7. Airflow profile switch

Set:

```bash
EXECUTION_PROFILE=prototype_unified
```

or:

```bash
EXECUTION_PROFILE=production_layered
```

The Airflow DAG uses this variable to decide whether to run the unified Raw/Work/Silver streaming path or the separated Raw Writer + Raw-to-Silver path.
