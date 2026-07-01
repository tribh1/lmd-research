# Kappa Batch-as-Event Flow

## 1. Design intent

The Kappa architecture should not maintain a separate batch transformation path and a streaming transformation path. For the prototype, batch data is represented as a bounded event stream. A batch snapshot or backfill job reads source records and publishes Debezium-compatible events to the same Kafka topics used by CDC.

This means batch, CDC, and application streaming share the same processing pipeline:

```text
Batch snapshot / Backfill
        |
        v
kappa_batch_to_event.py
        |
        v
Kafka topic: cdc.public.<table>
        |
        v
Spark Structured Streaming Kappa Config Pipeline
        |
        +--> Raw Iceberg
        +--> Work Iceberg
        +--> Quarantine Iceberg
        +--> Silver Iceberg
        +--> Gold / Mart
```

## 2. Why batch still exists in Kappa

Kappa does not mean there is no historical or batch data. It means historical/batch data is replayed through the same event log and processed by the same stream processor. In this implementation:

- initial load is emitted as Debezium `op = r` snapshot events;
- backfill is emitted as bounded `op = r` events for a selected time window;
- CDC continues using Debezium `op = c/u/d/r` events;
- all events are consumed by the same Spark Structured Streaming job;
- Raw/Work/Silver logic is not duplicated.

## 3. Batch event envelope

A batch snapshot row is serialized as:

```json
{
  "schema": null,
  "payload": {
    "before": null,
    "after": {
      "customer_id": 1,
      "full_name": "Nguyen Van A",
      "email": "a@example.com"
    },
    "op": "r",
    "ts_ms": 1782721093000,
    "source": {
      "version": "config-driven-batch-as-event",
      "connector": "batch-snapshot",
      "name": "postgres",
      "db": "source_db",
      "schema": "public",
      "table": "src_customer",
      "snapshot": "true",
      "batch_run_id": "batch_snapshot_customer_initial_snapshot_...",
      "config_version": "2026.06.30-v1.3",
      "extraction_mode": "snapshot_to_kafka",
      "event_id": "...",
      "row_hash": "..."
    },
    "transaction": null
  }
}
```

The existing Kappa parser reads `payload.after.*`, `payload.op`, `payload.source.*` and therefore processes batch snapshot rows exactly like CDC snapshot rows.

## 4. Configuration

Batch source configuration is stored in:

```text
metadata/kappa_batch_sources.yaml
```

Example:

```yaml
batch_sources:
  - name: customer_initial_snapshot
    enabled: true
    flow_name: dim_customer
    mode: snapshot_to_kafka
    source:
      connection: source_postgres
      database: source_db
      schema: public
      table: src_customer
      query: |
        SELECT customer_id, full_name, email, telephone, address, province,
               customer_segment, created_at, updated_at
        FROM public.src_customer
        ORDER BY customer_id
    event:
      connection: kafka_default
      topic: "cdc.public.src_customer"
      key_columns: ["customer_id"]
      op: "r"
      source_table: "src_customer"
```

## 5. Commands

Dry run and print sample events:

```bash
DRY_RUN=true JOBS=customer_initial_snapshot ./scripts/run_kappa_batch_publish.sh
```

Publish all enabled initial snapshots:

```bash
./scripts/run_kappa_batch_publish.sh
```

Publish one snapshot:

```bash
JOBS=customer_initial_snapshot ./scripts/run_kappa_batch_publish.sh
```

Run a time-window backfill:

```bash
JOBS=order_backfill_by_time_window \
FROM_TS="2026-06-01 00:00:00" \
TO_TS="2026-06-02 00:00:00" \
./scripts/run_kappa_batch_publish.sh
```

Then run the Kappa pipeline:

```bash
./scripts/run_kappa_config.sh
```

## 6. Runtime responsibilities

| Concern | Component |
|---|---|
| Batch extraction | `kappa_batch_to_event.py` |
| Event transport | Kafka |
| CDC capture | Debezium |
| Unified processing | Spark Structured Streaming |
| Embedded metadata | `extract_debezium_payload()` |
| Standardization / business logic | `apply_standardization()`, `apply_business_logic()` |
| DQ / quarantine | `apply_kappa_dq()` |
| PII masking | `apply_pii_masking()` |
| SCD/SK/FK | `kappa_merge.py`, `kappa_transform.py` |
| Orchestration | Airflow |

## 7. What this proves in the thesis

This implementation demonstrates that batch, CDC, and streaming can be unified without duplicating transformation logic. Batch remains visible in the architecture, but it is converted into a bounded event stream before entering the Kappa processing layer.
