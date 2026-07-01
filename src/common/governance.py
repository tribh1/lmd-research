from __future__ import annotations

import re
from functools import reduce
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.column import Column


def _rule_condition(rule: Dict) -> Column:
    col = F.col(rule["column"])
    rtype = rule["type"]
    if rtype == "not_null":
        return col.isNotNull()
    if rtype == "regex":
        return col.rlike(rule["pattern"])
    if rtype == "positive":
        return col.cast("double") > F.lit(0)
    if rtype == "in_set":
        return col.isin(rule["values"])
    if rtype == "freshness_hours":
        return F.abs(F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(col)) <= F.lit(int(rule["threshold"]) * 3600)
    if rtype == "pii_present":
        # true means PII exists in source; it will be masked downstream, so do not quarantine only for existence.
        return F.lit(True)
    raise ValueError(f"Unsupported rule type: {rtype}")


def apply_quality_rules(df: DataFrame, rules: List[Dict]) -> Tuple[DataFrame, DataFrame]:
    """Adds governance metadata and splits pass/fail records.

    Output columns added:
    - _dq_errors: array of failed rule ids
    - _dq_passed: boolean
    """
    error_exprs = []
    for rule in rules:
        condition = _rule_condition(rule)
        error_exprs.append(F.when(~condition, F.lit(rule["rule_id"])).otherwise(F.lit(None)))
    with_errors = df.withColumn("_dq_errors", F.array(*error_exprs))
    with_errors = with_errors.withColumn("_dq_errors", F.expr("filter(_dq_errors, x -> x is not null)"))
    with_errors = with_errors.withColumn("_dq_passed", F.size(F.col("_dq_errors")) == 0)
    passed = with_errors.filter(F.col("_dq_passed"))
    failed = with_errors.filter(~F.col("_dq_passed"))
    return passed, failed


def mask_email(col: Column) -> Column:
    return F.when(col.isNull(), None).otherwise(
        F.concat(F.substring_index(col, "@", 1).substr(1, 1), F.lit("***@"), F.substring_index(col, "@", -1))
    )


def mask_phone(col: Column) -> Column:
    return F.when(col.isNull(), None).otherwise(F.concat(F.lit("***"), F.substring(col, -4, 4)))


def last4(col: Column) -> Column:
    return F.when(col.isNull(), None).otherwise(F.concat(F.lit("****-****-****-"), F.substring(col, -4, 4)))


def apply_pii_masking(df: DataFrame, pii_columns: Dict[str, Dict]) -> DataFrame:
    out = df
    for column_name, spec in (pii_columns or {}).items():
        method = spec.get("method")
        if column_name not in out.columns:
            continue
        if method == "sha256":
            out = out.withColumn(column_name, F.sha2(F.col(column_name).cast("string"), 256))
        elif method == "email_mask":
            out = out.withColumn(column_name, mask_email(F.col(column_name)))
        elif method == "phone_mask":
            out = out.withColumn(column_name, mask_phone(F.col(column_name)))
        elif method == "nullify":
            out = out.withColumn(column_name, F.lit(None).cast("string"))
        elif method == "last4":
            out = out.withColumn(column_name, last4(F.col(column_name)))
        else:
            raise ValueError(f"Unsupported PII masking method: {method}")
    return out


def add_audit_columns(df: DataFrame, layer: str, batch_id: str, source_system: str = "postgres") -> DataFrame:
    return (
        df.withColumn("_layer", F.lit(layer))
          .withColumn("_source_system", F.lit(source_system))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in df.columns]), 256))
    )
