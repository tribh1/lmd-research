from __future__ import annotations

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def _email_mask(col: Column) -> Column:
    return F.when(col.isNull(), F.lit(None)).otherwise(
        F.concat(F.substring_index(col.cast("string"), "@", 1).substr(1, 1), F.lit("***@"), F.substring_index(col.cast("string"), "@", -1))
    )


def _phone_mask(col: Column) -> Column:
    return F.when(col.isNull(), F.lit(None)).otherwise(F.concat(F.lit("***"), F.substring(col.cast("string"), -4, 4)))


def _last4(col: Column) -> Column:
    return F.when(col.isNull(), F.lit(None)).otherwise(F.concat(F.lit("****-****-****-"), F.substring(col.cast("string"), -4, 4)))


def apply_pii_masking(df: DataFrame, pii_columns: Dict[str, Dict]) -> DataFrame:
    out = df
    for column_name, spec in (pii_columns or {}).items():
        if column_name not in out.columns:
            continue
        method = spec.get("method", "nullify")
        source_col = F.col(column_name)
        if method == "sha256":
            out = out.withColumn(column_name, F.sha2(source_col.cast("string"), 256))
        elif method == "email_mask":
            out = out.withColumn(column_name, _email_mask(source_col))
        elif method == "phone_mask":
            out = out.withColumn(column_name, _phone_mask(source_col))
        elif method == "last4":
            out = out.withColumn(column_name, _last4(source_col))
        elif method == "nullify":
            out = out.withColumn(column_name, F.lit(None).cast("string"))
        elif method == "keep":
            out = out
        else:
            raise ValueError(f"Unsupported PII masking method for {column_name}: {method}")
    return out
