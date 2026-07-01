from __future__ import annotations

from typing import Dict, Iterable, List, Optional
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from src.common.table_registry import ColumnSpec, TableSpec


def apply_schema_contract(df: DataFrame, table: TableSpec, strict: bool = False) -> DataFrame:
    """Apply schema contract from metadata.

    Missing nullable columns are created as NULL. Missing non-nullable columns raise
    an error in strict mode and are created as NULL otherwise so that schema-evolution
    experiments can continue and surface DQ violations downstream.
    """
    out = df
    for col in table.columns:
        if col.name not in out.columns:
            if strict and not col.nullable:
                raise ValueError(f"Table {table.name}: required column {col.name} is missing")
            out = out.withColumn(col.name, F.lit(None).cast(col.type))
        else:
            out = out.withColumn(col.name, F.col(col.name).cast(col.type))
    return out


def apply_work_transformations(df: DataFrame, table: TableSpec) -> DataFrame:
    spec = table.transformations.get("work", {}) or {}
    out = df

    # Optional column renaming: [{from: old_name, to: new_name}]
    for item in spec.get("rename_columns", []) or []:
        if item["from"] in out.columns:
            out = out.withColumnRenamed(item["from"], item["to"])

    if spec.get("cast_to_schema", False):
        out = apply_schema_contract(out, table, strict=False)

    for item in spec.get("derive_columns", []) or []:
        out = out.withColumn(item["name"], F.expr(item["expr"]))

    where_expr = spec.get("where")
    if where_expr:
        out = out.filter(F.expr(where_expr))

    return out


def apply_silver_transformations(df: DataFrame, table: TableSpec) -> DataFrame:
    spec = table.transformations.get("silver", {}) or {}
    out = df

    dedup = spec.get("deduplicate")
    if dedup:
        keys = dedup.get("keys") or table.primary_key
        order_by = dedup.get("order_by") or table.watermark_column
        if keys and order_by and order_by in out.columns:
            w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(F.col(order_by).desc_nulls_last())
            out = out.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    for item in spec.get("derive_columns", []) or []:
        out = out.withColumn(item["name"], F.expr(item["expr"]))

    select_cols = spec.get("select_columns")
    if select_cols:
        final_cols = [c for c in select_cols if c in out.columns]
        technical_cols = [c for c in out.columns if c.startswith("_")]
        out = out.select(*final_cols, *[c for c in technical_cols if c not in final_cols])

    return out


def add_audit_columns(df: DataFrame, *, layer: str, table_name: str, batch_id: str, source_system: str) -> DataFrame:
    base_cols = [c for c in df.columns if not c.startswith("_")]
    return (
        df.withColumn("_table_name", F.lit(table_name))
        .withColumn("_layer", F.lit(layer))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in base_cols]), 256))
    )


def drop_audit_columns(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in df.columns if c.startswith("_")])
