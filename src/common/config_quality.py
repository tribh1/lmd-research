from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def _columns(rule: Dict[str, Any]) -> List[str]:
    if "columns" in rule:
        return list(rule["columns"])
    return [rule["column"]]


def build_rule_condition(df: DataFrame, rule: Dict[str, Any]) -> Column:
    """Translate a metadata DQ rule to a Spark boolean expression.

    Supported types:
    - not_null
    - regex
    - positive
    - non_negative
    - in_set
    - range
    - freshness_hours
    - unique
    - always_pass
    """
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
        min_value = rule.get("min")
        max_value = rule.get("max")
        cond = F.lit(True)
        if min_value is not None:
            cond = cond & (col >= F.lit(float(min_value)))
        if max_value is not None:
            cond = cond & (col <= F.lit(float(max_value)))
        return cond

    if rtype == "freshness_hours":
        col = F.col(rule["column"])
        return F.abs(F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(col)) <= F.lit(int(rule["threshold"]) * 3600)

    if rtype == "unique":
        keys = _columns(rule)
        # Spark window count allows the failing duplicate records to be quarantined.
        w = Window.partitionBy(*[F.col(k) for k in keys])
        return F.count(F.lit(1)).over(w) == 1

    if rtype == "always_pass":
        return F.lit(True)

    raise ValueError(f"Unsupported data-quality rule type: {rtype}")


def apply_quality_rules(df: DataFrame, rules: Iterable[Dict[str, Any]]) -> Tuple[DataFrame, DataFrame]:
    rules = list(rules or [])
    if not rules:
        out = df.withColumn("_dq_errors", F.array()).withColumn("_dq_passed", F.lit(True))
        return out, out.filter(F.lit(False))

    with_checks = df
    error_exprs: List[Column] = []
    for rule in rules:
        condition = build_rule_condition(with_checks, rule)
        error_exprs.append(F.when(~condition, F.lit(rule["rule_id"])).otherwise(F.lit(None)))

    with_checks = with_checks.withColumn("_dq_errors", F.array(*error_exprs))
    with_checks = with_checks.withColumn("_dq_errors", F.expr("filter(_dq_errors, x -> x is not null)"))
    with_checks = with_checks.withColumn("_dq_passed", F.size(F.col("_dq_errors")) == 0)
    return with_checks.filter(F.col("_dq_passed")), with_checks.filter(~F.col("_dq_passed"))


def summarize_rule_results(passed: DataFrame, failed: DataFrame, rules: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    passed_count = passed.count()
    failed_count_total = failed.count()
    result = []
    for rule in rules or []:
        failed_count = failed.filter(F.array_contains(F.col("_dq_errors"), rule["rule_id"])).count()
        result.append(
            {
                "rule_id": rule["rule_id"],
                "severity": rule.get("severity", "critical"),
                "passed_rows": passed_count,
                "failed_rows": failed_count,
                "total_failed_rows": failed_count_total,
            }
        )
    return result
