# Architecture Summary

## Name

Strict Layered Metadata-Driven Kappa Lakehouse Architecture

## Core idea

The architecture unifies batch, CDC, and streaming ingestion through Kafka while separating every Lakehouse layer transition into an independent physical job.

```text
Batch / CDC / Stream
        |
        v
Kafka topic by table
        |
        v
Kafka -> Raw
        |
        v
Raw -> Work
        |
        v
Work -> Silver / Quarantine
        |
        v
Silver -> Gold
        |
        v
Gold -> Data Mart
```

## Why strict layered jobs?

- Independent retry at each layer.
- Replay from Raw or Work without re-consuming Kafka.
- Clear lineage: Kafka -> Raw -> Work -> Silver -> Gold -> Mart.
- Separate scaling for ingestion, governance, and serving.
- Easier code review and experimental evaluation.

## Shared semantics

Although physical jobs are separated, transformation and governance logic is centralized in:

```text
src/common/kappa_layer_processor.py
```

This prevents duplicate logic while preserving strict job boundaries.

## Metadata control plane

The pipeline is driven by YAML configuration:

```text
metadata/kappa_flows.yaml
metadata/kappa_batch_sources.yaml
metadata/gold_models.yaml
metadata/reconciliation_jobs.yaml
metadata/openmetadata_config.yaml
metadata/job_execution_plan.yaml
```

## Embedded metadata

Each record carries `_meta_*` fields for audit, lineage, replay, quality, and governance.
