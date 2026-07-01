# Kappa Silver Merge Design: SCD1, SCD2, Fact, Delete Handling

## 1. Purpose

This module completes the Kappa-only metadata-driven Lakehouse pipeline by adding entity-aware Silver writes:

- SCD Type 1 merge for dimensions such as `dim_product`.
- SCD Type 2 merge for dimensions such as `dim_customer`.
- Fact upsert for tables such as `fact_order`.
- Soft delete / hard delete handling from Debezium CDC operation `d`.
- Idempotent replay behavior through deterministic surrogate keys and `_meta_record_hash`.

## 2. New code

```text
src/common/kappa_merge.py
```

Main functions:

```text
write_silver_configured()
merge_scd1_or_fact()
merge_scd2()
ensure_target_schema()
```

## 3. SCD2 merge semantics

For a dimension configured as:

```yaml
entity_type: dimension
scd_type: 2
surrogate_key:
  column: customer_sk
  method: hash64
  keys: ["customer_id"]
  scd_type: 2
  effective_from: "updated_at"
  effective_to_column: "effective_to"
  current_flag_column: "is_current"
```

The pipeline applies the following rules:

1. The current active version has `is_current = true`.
2. New CDC event is compared with the current row using `_meta_record_hash`.
3. If the payload is unchanged, the event is ignored for Silver; Raw and Work still retain the event.
4. If the payload changes, the old current row is closed:

```text
is_current = false
effective_to = source.updated_at
_meta_closed_by_event_id = source._meta_event_id
_meta_closed_at = current_timestamp()
```

5. The new version is inserted with a deterministic `customer_sk`.
6. Delete events do not physically remove history; they close the current row and set:

```text
_meta_is_deleted = true
_meta_deleted_at = current_timestamp()
```

## 4. Why deterministic surrogate keys

Streaming replay, checkpoint recovery, and CDC reprocessing require the same input event to produce the same output key. Therefore, the design avoids sequences and uses:

```text
SCD1 key = hash64(natural_key)
SCD2 key = hash64(natural_key + effective_from)
Fact key = hash64(business_transaction_key)
```

## 5. Delete modes

Configure per target:

```yaml
target:
  write_mode: merge
  delete_mode: soft_delete
```

Supported values:

```text
soft_delete   preserve row and mark _meta_is_deleted = true
hard_delete   physically delete matched rows from target table
```

For dimensions with SCD2, delete always closes the current version to preserve history.

## 6. Schema evolution

When a new column appears in the configured schema contract, `ensure_target_schema()` executes:

```sql
ALTER TABLE <target> ADD COLUMN <column> <type>
```

before executing MERGE/append.
