from __future__ import annotations

from typing import Any, Dict, Iterable, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.kappa_registry import KappaFlow, KappaRegistry


def ident(catalog: str, table_ref: str) -> str:
    if table_ref.count(".") >= 2:
        return table_ref
    return f"{catalog}.{table_ref}"


def json_path(path: str) -> str:
    if path.startswith("$"):
        return path
    return "$.." + path if path.startswith("payload.") else "$.payload." + path


def extract_debezium_payload(df: DataFrame, flow: KappaFlow, registry: KappaRegistry) -> DataFrame:
    """Project configured columns from Debezium JSON envelope and add embedded metadata."""
    value_col = F.col("value").cast("string")
    out = df.select(
        value_col.alias("_raw_event_json"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    )
    out = out.withColumn("_source_operation", F.get_json_object(F.col("_raw_event_json"), "$.payload.op"))
    out = out.withColumn("_source_ts_ms", F.get_json_object(F.col("_raw_event_json"), "$.payload.ts_ms").cast("long"))
    out = out.withColumn("_source_database", F.get_json_object(F.col("_raw_event_json"), "$.payload.source.db"))
    out = out.withColumn("_source_schema", F.get_json_object(F.col("_raw_event_json"), "$.payload.source.schema"))
    out = out.withColumn("_source_table", F.get_json_object(F.col("_raw_event_json"), "$.payload.source.table"))

    # For delete events, values usually live in before.*. For other ops, after.* is used.
    for col in flow.columns:
        configured_path = col.get("path", f"after.{col['name']}")
        after_path = json_path(configured_path)
        before_path = json_path(configured_path.replace("after.", "before."))
        raw_value = F.when(F.col("_source_operation") == F.lit("d"), F.get_json_object(F.col("_raw_event_json"), before_path)).otherwise(
            F.get_json_object(F.col("_raw_event_json"), after_path)
        )
        out = out.withColumn(col["name"], raw_value.cast(col.get("type", "string")))

    base_cols = [c["name"] for c in flow.columns]
    schema_str = "|".join([f"{c['name']}:{c.get('type','string')}" for c in flow.columns])
    pii_tags = [c["name"] for c in flow.columns if c.get("classification")]
    lineage_json = F.to_json(
        F.struct(
            F.lit(flow.name).alias("flow"),
            F.array(*[F.lit(t) for t in flow.topic_list]).alias("topics"),
            F.lit(flow.raw_table).alias("raw"),
            F.lit(flow.work_table).alias("work"),
            F.lit(flow.silver_table).alias("silver"),
        )
    )
    out = (
        out.withColumn("_meta_event_id", F.sha2(F.concat_ws("|", F.col("_kafka_topic"), F.col("_kafka_partition"), F.col("_kafka_offset")), 256))
        .withColumn("_meta_source_system", F.lit(registry.runtime.source_system))
        .withColumn("_meta_source_database", F.col("_source_database"))
        .withColumn("_meta_source_schema", F.col("_source_schema"))
        .withColumn("_meta_source_table", F.col("_source_table"))
        .withColumn("_meta_source_operation", F.col("_source_operation"))
        .withColumn("_meta_source_ts_ms", F.col("_source_ts_ms"))
        .withColumn("_meta_kafka_topic", F.col("_kafka_topic"))
        .withColumn("_meta_kafka_partition", F.col("_kafka_partition"))
        .withColumn("_meta_kafka_offset", F.col("_kafka_offset"))
        .withColumn("_meta_ingest_ts", F.current_timestamp())
        .withColumn("_meta_config_version", F.lit(registry.runtime.config_version))
        .withColumn("_meta_pipeline_name", F.lit(flow.name))
        .withColumn("_meta_layer", F.lit("raw"))
        .withColumn("_meta_record_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in base_cols]), 256))
        .withColumn("_meta_schema_hash", F.sha2(F.lit(schema_str), 256))
        .withColumn("_meta_pii_tags", F.to_json(F.array(*[F.lit(x) for x in pii_tags])))
        .withColumn("_meta_lineage", lineage_json)
        .withColumn("_meta_is_deleted", F.col("_meta_source_operation").isin("d", "delete", "DELETE"))
        .withColumn("_meta_deleted_at", F.when(F.col("_meta_source_operation").isin("d", "delete", "DELETE"), F.current_timestamp()).otherwise(F.lit(None).cast("timestamp")))
        .withColumn("_meta_closed_by_event_id", F.lit(None).cast("string"))
        .withColumn("_meta_closed_at", F.lit(None).cast("timestamp"))
    )
    return out.drop("_source_database", "_source_schema", "_source_table", "_source_operation", "_source_ts_ms")


def business_columns(flow: KappaFlow) -> List[str]:
    return [c["name"] for c in flow.columns]


def metadata_columns(df: DataFrame) -> List[str]:
    return [c for c in df.columns if c.startswith("_meta_") or c in {"_raw_event_json", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp"}]


def expand_standard_rules(flow: KappaFlow, registry: KappaRegistry) -> List[Dict[str, Any]]:
    spec = flow.standardization or {}
    rules: List[Dict[str, Any]] = []
    for name in spec.get("use_rule_sets", []) or []:
        rules.extend(registry.rule_sets.get(name, []) or [])
    rules.extend(spec.get("rules", []) or [])
    return rules


def apply_standardization(df: DataFrame, flow: KappaFlow, registry: KappaRegistry) -> DataFrame:
    out = df
    for rule in expand_standard_rules(flow, registry):
        action = rule.get("action")
        col = rule.get("column")
        name = rule.get("name") or col
        if action == "trim" and col in out.columns:
            out = out.withColumn(col, F.trim(F.col(col)))
        elif action == "upper" and col in out.columns:
            out = out.withColumn(col, F.upper(F.col(col)))
        elif action == "lower" and col in out.columns:
            out = out.withColumn(col, F.lower(F.col(col)))
        elif action == "cast" and col in out.columns:
            out = out.withColumn(col, F.col(col).cast(rule["type"]))
        elif action == "regexp_replace" and col in out.columns:
            out = out.withColumn(col, F.regexp_replace(F.col(col).cast("string"), rule["pattern"], rule.get("replacement", "")))
        elif action == "coalesce":
            out = out.withColumn(name, F.coalesce(*[F.col(c) for c in rule.get("columns", [])]))
        elif action == "expr":
            out = out.withColumn(name, F.expr(rule["expr"]))
        else:
            raise ValueError(f"Unsupported standardization rule: {rule}")
    return out.withColumn("_meta_layer", F.lit("work"))


def apply_business_logic(df: DataFrame, flow: KappaFlow) -> DataFrame:
    out = df
    for item in (flow.business_logic or {}).get("derive_columns", []) or []:
        out = out.withColumn(item["name"], F.expr(item["expr"]))
    where_expr = (flow.business_logic or {}).get("where")
    if where_expr:
        out = out.filter(F.expr(where_expr))
    return out


def add_surrogate_key(df: DataFrame, flow: KappaFlow) -> DataFrame:
    sk = flow.surrogate_key or {}
    col_name = sk.get("column")
    if not col_name:
        return df
    keys = sk.get("keys") or flow.natural_key
    scd_type = str(sk.get("scd_type", flow.scd_type or "none")).lower()
    hash_inputs = [F.col(k).cast("string") for k in keys]
    if scd_type == "2" and sk.get("effective_from"):
        hash_inputs.append(F.col(sk["effective_from"]).cast("string"))
    # Deterministic distributed surrogate key. Avoids non-repeatable sequences in streaming.
    return df.withColumn(col_name, F.abs(F.xxhash64(*hash_inputs)).cast("long"))


def add_scd_columns(df: DataFrame, flow: KappaFlow) -> DataFrame:
    sk = flow.surrogate_key or {}
    if str(sk.get("scd_type", flow.scd_type or "none")).lower() != "2":
        return df
    effective_from = sk.get("effective_from") or flow.sequence_column
    effective_to_col = sk.get("effective_to_column", "effective_to")
    current_flag_col = sk.get("current_flag_column", "is_current")
    return (
        df.withColumn("effective_from", F.col(effective_from).cast("timestamp") if effective_from in df.columns else F.current_timestamp())
        .withColumn(effective_to_col, F.lit(None).cast("timestamp"))
        .withColumn(current_flag_col, F.lit(True))
    )


def resolve_foreign_keys(df: DataFrame, flow: KappaFlow) -> DataFrame:
    out = df
    spark = df.sparkSession
    for fk in flow.foreign_keys:
        lookup = spark.table(fk["lookup_table"]).select(*(fk.get("lookup_key", []) + [fk["lookup_value"]]))
        left_keys = fk.get("source_key") or fk.get("lookup_key") or []
        right_keys = fk.get("lookup_key") or []
        cond = [out[l] == lookup[r] for l, r in zip(left_keys, right_keys)]
        joined = out.join(F.broadcast(lookup), cond, "left")
        out = joined.withColumn(fk["name"], F.coalesce(F.col(fk["lookup_value"]), F.lit(fk.get("unknown_value", -1))).cast("long"))
        # Drop duplicate lookup natural keys only if they were added from lookup side with same name is ambiguous in Spark; select safe columns.
    return out


def select_final_columns(df: DataFrame, flow: KappaFlow) -> DataFrame:
    configured = [c for c in business_columns(flow) if c in df.columns]
    derived = [c for c in df.columns if not c.startswith("_") and c not in configured]
    meta = [c for c in df.columns if c.startswith("_meta_")]
    return df.select(*(configured + derived + meta))
