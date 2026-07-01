from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def build_condition(rule: Dict[str, Any]) -> Column:
    rtype = rule["type"]
    if rtype == "not_null":
        return F.col(rule["column"]).isNotNull()
    if rtype == "regex":
        return F.col(rule["column"]).cast("string").rlike(rule["pattern"])
    if rtype == "positive":
        return F.col(rule["column"]).cast("double") > F.lit(0)
    if rtype == "non_negative":
        return F.col(rule["column"]).cast("double") >= F.lit(0)
    if rtype == "in_set":
        return F.col(rule["column"]).isin(rule.get("values", []))
    if rtype == "range":
        col = F.col(rule["column"]).cast("double")
        cond = F.lit(True)
        if "min" in rule:
            cond = cond & (col >= F.lit(float(rule["min"])))
        if "max" in rule:
            cond = cond & (col <= F.lit(float(rule["max"])))
        return cond
    if rtype == "expr":
        return F.expr(rule["expr"])
    raise ValueError(f"Unsupported Kappa DQ rule type: {rtype}")


def apply_kappa_dq(df: DataFrame, rules: Iterable[Dict[str, Any]]) -> Tuple[DataFrame, DataFrame, List[Dict[str, Any]]]:
    rules = list(rules or [])
    if not rules:
        out = df.withColumn("_meta_dq_errors", F.array()).withColumn("_meta_dq_passed", F.lit(True))
        return out, out.filter(F.lit(False)), []
    out = df
    errors: List[Column] = []
    for rule in rules:
        cond = build_condition(rule)
        errors.append(F.when(~cond, F.lit(rule["rule_id"])).otherwise(F.lit(None)))
    out = out.withColumn("_meta_dq_errors", F.array(*errors))
    out = out.withColumn("_meta_dq_errors", F.expr("filter(_meta_dq_errors, x -> x is not null)"))
    out = out.withColumn("_meta_dq_passed", F.size(F.col("_meta_dq_errors")) == 0)
    passed = out.filter(F.col("_meta_dq_passed"))
    failed = out.filter(~F.col("_meta_dq_passed"))
    metrics: List[Dict[str, Any]] = []
    total = out.count()
    for rule in rules:
        failed_count = failed.filter(F.array_contains(F.col("_meta_dq_errors"), rule["rule_id"])).count()
        metrics.append({"rule_id": rule["rule_id"], "severity": rule.get("severity", "critical"), "total_rows": total, "failed_rows": failed_count})
    return passed, failed, metrics
