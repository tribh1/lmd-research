from __future__ import annotations

from typing import Iterable, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from src.common.config_io import iceberg_table_exists, write_iceberg
from src.common.kappa_registry import KappaFlow


def _sql_type(dt) -> str:
    if isinstance(dt, StringType):
        return "string"
    if isinstance(dt, LongType):
        return "bigint"
    if isinstance(dt, IntegerType):
        return "int"
    if isinstance(dt, BooleanType):
        return "boolean"
    if isinstance(dt, TimestampType):
        return "timestamp"
    if isinstance(dt, DateType):
        return "date"
    if isinstance(dt, DoubleType):
        return "double"
    if isinstance(dt, FloatType):
        return "float"
    if isinstance(dt, DecimalType):
        return f"decimal({dt.precision},{dt.scale})"
    return dt.simpleString()


def _view_name(prefix: str, table_ident: str) -> str:
    return f"_{prefix}_" + table_ident.replace(".", "_").replace("-", "_")


def _create_or_append_empty_safe(
    df: DataFrame,
    target_ident: str,
    *,
    partition_by: Optional[str],
) -> bool:
    """Create the target table from a non-empty dataframe. Return True if table exists after call."""
    if df.rdd.isEmpty():
        return iceberg_table_exists(df.sparkSession, target_ident)

    if not iceberg_table_exists(df.sparkSession, target_ident):
        write_iceberg(df, target_ident, partition_by=partition_by, write_mode="append")
        return True

    return True


def ensure_target_schema(df: DataFrame, target_ident: str) -> None:
    """Add newly appeared columns to an existing Iceberg table.

    This makes schema evolution metadata-driven: if schema_contract.on_schema_change
    is add_columns and the source emits a new column, the streaming batch can evolve
    the table before MERGE/append.
    """
    spark = df.sparkSession
    if not iceberg_table_exists(spark, target_ident):
        return

    target_cols = {field.name for field in spark.table(target_ident).schema.fields}
    for field in df.schema.fields:
        if field.name not in target_cols:
            spark.sql(
                f"ALTER TABLE {target_ident} ADD COLUMN {field.name} {_sql_type(field.dataType)}"
            )


def _align_to_target(df: DataFrame, target_ident: str) -> DataFrame:
    spark = df.sparkSession
    target_schema = spark.table(target_ident).schema
    out = df

    for field in target_schema.fields:
        if field.name not in out.columns:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))

    return out.select(*[field.name for field in target_schema.fields])


def _key_condition(alias_left: str, alias_right: str, keys: Iterable[str]) -> str:
    return " AND ".join([f"{alias_left}.{k} <=> {alias_right}.{k}" for k in keys])


def _active_filter(df: DataFrame) -> DataFrame:
    if "_meta_source_operation" not in df.columns:
        return df
    return df.filter(~F.col("_meta_source_operation").isin("d", "delete", "DELETE"))


def _delete_filter(df: DataFrame) -> DataFrame:
    if "_meta_source_operation" not in df.columns:
        return df.filter(F.lit(False))
    return df.filter(F.col("_meta_source_operation").isin("d", "delete", "DELETE"))


def merge_scd1_or_fact(
    df: DataFrame,
    target_ident: str,
    flow: KappaFlow,
    *,
    partition_by: Optional[str] = None,
) -> None:
    """Upsert SCD1 dimension or fact rows with deterministic keys and delete handling.

    Delete handling:
    - Default: soft delete by setting _meta_is_deleted=true.
    - If flow.target.delete_mode == hard_delete, delete matched records.
    """
    spark = df.sparkSession
    keys = [flow.surrogate_key.get("column")] if flow.surrogate_key.get("column") else flow.natural_key
    delete_mode = (flow.target.get("delete_mode") or "soft_delete").lower()

    active_df = _active_filter(df)
    delete_df = _delete_filter(df)

    # For first creation, create from non-delete rows only.
    if not iceberg_table_exists(spark, target_ident):
        if not active_df.rdd.isEmpty():
            write_iceberg(active_df, target_ident, partition_by=partition_by, write_mode="append")
        return

    ensure_target_schema(df, target_ident)

    # Upsert insert/update records.
    if not active_df.rdd.isEmpty():
        src_view = _view_name("merge_src", target_ident)
        active_df = _align_to_target(active_df, target_ident)
        active_df.createOrReplaceTempView(src_view)
        target_cols = active_df.columns
        on_clause = _key_condition("t", "s", keys)
        update_clause = ", ".join([f"t.{c} = s.{c}" for c in target_cols if c not in keys])
        insert_cols = ", ".join(target_cols)
        insert_vals = ", ".join([f"s.{c}" for c in target_cols])
        spark.sql(
            f"""
            MERGE INTO {target_ident} t
            USING {src_view} s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
        )
        spark.catalog.dropTempView(src_view)

    # Apply delete events.
    if not delete_df.rdd.isEmpty():
        del_view = _view_name("delete_src", target_ident)
        delete_df.select(*[k for k in keys if k in delete_df.columns]).dropDuplicates().createOrReplaceTempView(del_view)
        on_clause = _key_condition("t", "s", keys)

        if delete_mode == "hard_delete":
            spark.sql(
                f"""
                MERGE INTO {target_ident} t
                USING {del_view} s
                ON {on_clause}
                WHEN MATCHED THEN DELETE
                """
            )
        else:
            # Soft delete preserves history and auditability.
            set_parts = ["t._meta_is_deleted = true"]
            if "_meta_deleted_at" in spark.table(target_ident).columns:
                set_parts.append("t._meta_deleted_at = current_timestamp()")
            spark.sql(
                f"""
                MERGE INTO {target_ident} t
                USING {del_view} s
                ON {on_clause}
                WHEN MATCHED THEN UPDATE SET {', '.join(set_parts)}
                """
            )
        spark.catalog.dropTempView(del_view)


def merge_scd2(
    df: DataFrame,
    target_ident: str,
    flow: KappaFlow,
    *,
    partition_by: Optional[str] = None,
) -> None:
    """Merge a Type-2 dimension from a streaming micro-batch.

    Semantics:
    1. Current version is identified by is_current=true.
    2. A new version is inserted only when there is no current row with the same
       natural key and same _meta_record_hash.
    3. When the payload changes, the previous current row is closed by setting
       effective_to and is_current=false.
    4. Delete events close the current version, preserving historical versions.
    5. Surrogate key must be deterministic, typically hash64(natural_key + effective_from).
    """
    spark = df.sparkSession
    sk = flow.surrogate_key or {}
    keys = flow.natural_key
    effective_from = sk.get("effective_from") or flow.sequence_column or "_meta_ingest_ts"
    effective_to_col = sk.get("effective_to_column", "effective_to")
    current_flag_col = sk.get("current_flag_column", "is_current")

    active_df = _active_filter(df)
    delete_df = _delete_filter(df)

    if not iceberg_table_exists(spark, target_ident):
        if not active_df.rdd.isEmpty():
            write_iceberg(active_df, target_ident, partition_by=partition_by, write_mode="append")
        return

    ensure_target_schema(df, target_ident)

    # Close changed current rows.
    if not active_df.rdd.isEmpty():
        src_view = _view_name("scd2_src", target_ident)
        active_df.createOrReplaceTempView(src_view)
        key_cond = _key_condition("t", "s", keys)
        # Null-safe hash comparison allows replay idempotency: same hash is ignored.
        spark.sql(
            f"""
            MERGE INTO {target_ident} t
            USING {src_view} s
            ON {key_cond} AND t.{current_flag_col} = true
            WHEN MATCHED AND NOT (t._meta_record_hash <=> s._meta_record_hash) THEN UPDATE SET
                t.{current_flag_col} = false,
                t.{effective_to_col} = s.{effective_from},
                t._meta_closed_by_event_id = s._meta_event_id,
                t._meta_closed_at = current_timestamp()
            """
        )

        # Insert the new current version if no unchanged current version exists.
        target_current = spark.table(target_ident).filter(F.col(current_flag_col) == F.lit(True))
        join_cond = [active_df[k].eqNullSafe(target_current[k]) for k in keys]
        join_cond.append(active_df["_meta_record_hash"].eqNullSafe(target_current["_meta_record_hash"]))
        insert_df = active_df.join(target_current, join_cond, "left_anti")

        if not insert_df.rdd.isEmpty():
            insert_df = _align_to_target(insert_df, target_ident)
            insert_df.writeTo(target_ident).append()

        spark.catalog.dropTempView(src_view)

    # Delete event closes the current version.
    if not delete_df.rdd.isEmpty():
        del_view = _view_name("scd2_delete_src", target_ident)
        delete_df.createOrReplaceTempView(del_view)
        key_cond = _key_condition("t", "s", keys)
        close_time = effective_from if effective_from in delete_df.columns else "_meta_ingest_ts"
        spark.sql(
            f"""
            MERGE INTO {target_ident} t
            USING {del_view} s
            ON {key_cond} AND t.{current_flag_col} = true
            WHEN MATCHED THEN UPDATE SET
                t.{current_flag_col} = false,
                t.{effective_to_col} = s.{close_time},
                t._meta_is_deleted = true,
                t._meta_deleted_at = current_timestamp(),
                t._meta_closed_by_event_id = s._meta_event_id,
                t._meta_closed_at = current_timestamp()
            """
        )
        spark.catalog.dropTempView(del_view)


def write_silver_configured(
    df: DataFrame,
    target_ident: str,
    flow: KappaFlow,
    *,
    partition_by: Optional[str] = None,
) -> None:
    """Write Silver table according to entity semantics instead of one generic merge.

    - dimension + SCD2 -> merge_scd2
    - dimension + SCD1 -> merge_scd1_or_fact
    - fact -> merge_scd1_or_fact
    - other -> fallback to configured write_iceberg
    """
    entity_type = (flow.entity_type or "table").lower()
    scd_type = str((flow.surrogate_key or {}).get("scd_type", flow.scd_type or "none")).lower()

    if entity_type == "dimension" and scd_type == "2":
        merge_scd2(df, target_ident, flow, partition_by=partition_by)
        return

    if entity_type in {"dimension", "fact"}:
        merge_scd1_or_fact(df, target_ident, flow, partition_by=partition_by)
        return

    write_iceberg(
        df,
        target_ident,
        partition_by=partition_by,
        write_mode=flow.write_mode,
        primary_key=[flow.surrogate_key.get("column")] if flow.surrogate_key.get("column") else flow.natural_key,
    )
