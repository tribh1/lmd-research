from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except Exception:  # allows --mode summary in non-Spark environments
    DataFrame = Any  # type: ignore
    F = None  # type: ignore


try:
    import trino  # type: ignore
except Exception:  # pragma: no cover - optional dependency in local checks
    trino = None

try:
    from src.common.kappa_openmetadata import OpenMetadataEmitter, table_fqn
except Exception:  # pragma: no cover
    OpenMetadataEmitter = None  # type: ignore
    table_fqn = None  # type: ignore


class GoldModelError(ValueError):
    pass


@dataclass(frozen=True)
class GoldRuntime:
    app_name: str = "kappa-gold-model-runner"
    catalog: str = "lakehouse"
    default_engine: str = "spark_sql"
    audit_table: str = "lakehouse.audit.gold_model_metrics"
    quality_table: str = "lakehouse.audit.gold_quality_metrics"
    local_result_dir: str = "results/gold_models"


@dataclass(frozen=True)
class GoldModel:
    name: str
    enabled: bool
    layer: str
    engine: str
    target: str
    write_mode: str
    sql: str
    upstream: List[str]
    partition_by: Optional[str] = None
    owner: Optional[str] = None
    domain: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    quality: Dict[str, Any] = field(default_factory=dict)


def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def load_gold_config(path: str) -> Tuple[GoldRuntime, Dict[str, Any], Dict[str, List[Dict[str, Any]]], List[GoldModel]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = _expand_env_vars(yaml.safe_load(f) or {})

    rt = raw.get("runtime", {}) or {}
    runtime = GoldRuntime(
        app_name=rt.get("app_name", "kappa-gold-model-runner"),
        catalog=rt.get("catalog", "lakehouse"),
        default_engine=rt.get("default_engine", "spark_sql"),
        audit_table=rt.get("audit_table", "lakehouse.audit.gold_model_metrics"),
        quality_table=rt.get("quality_table", "lakehouse.audit.gold_quality_metrics"),
        local_result_dir=rt.get("local_result_dir", "results/gold_models"),
    )

    models: List[GoldModel] = []
    for item in raw.get("models", []) or []:
        models.append(
            GoldModel(
                name=item["name"],
                enabled=bool(item.get("enabled", True)),
                layer=item.get("layer", "gold"),
                engine=item.get("engine", runtime.default_engine),
                target=item["target"],
                write_mode=item.get("write_mode", "overwrite"),
                sql=item["sql"],
                upstream=item.get("upstream", []) or [],
                partition_by=item.get("partition_by"),
                owner=item.get("owner"),
                domain=item.get("domain"),
                description=item.get("description"),
                tags=item.get("tags", []) or [],
                quality=item.get("quality", {}) or {},
            )
        )

    return runtime, raw.get("connections", {}) or {}, raw.get("quality_rule_sets", {}) or {}, models


def selected_models(models: Iterable[GoldModel], names: Optional[str], layers: Optional[str] = None) -> List[GoldModel]:
    selected = {x.strip() for x in (names or "").split(",") if x.strip()}
    selected_layers = {x.strip().lower() for x in (layers or "").split(",") if x.strip()}
    return [
        m for m in models
        if m.enabled
        and (not selected or m.name in selected)
        and (not selected_layers or m.layer.lower() in selected_layers)
    ]


def expand_quality_rules(model: GoldModel, rule_sets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rules: List[Dict[str, Any]] = []
    for name in (model.quality or {}).get("use_rule_sets", []) or []:
        rules.extend(rule_sets.get(name, []) or [])
    rules.extend((model.quality or {}).get("rules", []) or [])
    return rules


def build_quality_condition(df: DataFrame, rule: Dict[str, Any]):
    rtype = rule["type"]
    if rtype == "row_count_positive":
        return None
    if rtype == "not_null":
        return F.col(rule["column"]).isNotNull()
    if rtype == "non_negative":
        return F.col(rule["column"]).cast("double") >= F.lit(0)
    if rtype == "positive":
        return F.col(rule["column"]).cast("double") > F.lit(0)
    if rtype == "range":
        cond = F.lit(True)
        if "min" in rule:
            cond = cond & (F.col(rule["column"]).cast("double") >= F.lit(float(rule["min"])))
        if "max" in rule:
            cond = cond & (F.col(rule["column"]).cast("double") <= F.lit(float(rule["max"])))
        return cond
    if rtype == "expr":
        return F.expr(rule["expr"])
    raise GoldModelError(f"Unsupported gold quality rule: {rule}")


def run_quality(df: DataFrame, model: GoldModel, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total = df.count()
    results: List[Dict[str, Any]] = []
    for rule in rules:
        if rule["type"] == "row_count_positive":
            failed = 0 if total > 0 else 1
        else:
            cond = build_quality_condition(df, rule)
            failed = df.filter(~cond).count() if cond is not None else 0
        results.append(
            {
                "model": model.name,
                "target": model.target,
                "rule_id": rule["rule_id"],
                "severity": rule.get("severity", "critical"),
                "total_rows": total,
                "failed_rows": int(failed),
                "status": "success" if int(failed) == 0 else "failed",
            }
        )
    return results


def write_metrics(spark, runtime: GoldRuntime, rows: List[Dict[str, Any]], table_ident: str) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows)
    from src.common.config_io import write_iceberg
    write_iceberg(df, table_ident, write_mode="append")


def run_spark_model(spark, runtime: GoldRuntime, model: GoldModel) -> Tuple[int, List[Dict[str, Any]], float]:
    started = time.time()
    df = spark.sql(model.sql)
    row_count = df.count()
    write_iceberg(df, model.target, partition_by=model.partition_by, write_mode=model.write_mode)
    quality = run_quality(df, model, [])
    elapsed = time.time() - started
    return row_count, quality, elapsed


def run_trino_statement(connections: Dict[str, Any], model: GoldModel) -> Dict[str, Any]:
    if trino is None:
        raise GoldModelError("trino package is not available")
    conn_cfg = connections.get("trino_default") or {}
    if model.write_mode.lower() == "overwrite":
        pre_sql = f"DROP TABLE IF EXISTS {model.target}"
        create_sql = f"CREATE TABLE {model.target} AS {model.sql}"
    else:
        create_sql = f"INSERT INTO {model.target} {model.sql}"
        pre_sql = None
    conn = trino.dbapi.connect(
        host=conn_cfg.get("host", "trino"),
        port=int(conn_cfg.get("port", 8080)),
        user=conn_cfg.get("user", "lakehouse"),
        catalog=conn_cfg.get("catalog", "lakehouse"),
        schema=conn_cfg.get("schema", model.layer),
        http_scheme=conn_cfg.get("http_scheme", "http"),
    )
    cur = conn.cursor()
    if pre_sql:
        cur.execute(pre_sql)
    cur.execute(create_sql)
    return {"status": "submitted", "target": model.target}


def emit_model_openmetadata(emitter: Any, runtime: GoldRuntime, model: GoldModel) -> None:
    if emitter is None:
        return
    try:
        # Register as a lightweight KappaModel-compatible object.
        class _Model:
            pass
        m = _Model()
        m.name = model.name
        m.layer = model.layer
        m.sql = model.sql
        m.upstream = model.upstream
        m.write_mode = model.write_mode
        class _Runtime:
            pass
        rt = _Runtime()
        rt.catalog = runtime.catalog
        rt.config_version = "gold-models"
        class _Registry:
            pass
        reg = _Registry()
        reg.runtime = rt
        emitter.register_model(m, reg)
        for upstream in model.upstream:
            source_ref = upstream if upstream.count(".") >= 2 else f"{runtime.catalog}.{upstream}"
            emitter.add_lineage(
                table_fqn(emitter.cfg, source_ref, runtime.catalog),
                table_fqn(emitter.cfg, model.target, runtime.catalog),
                model.name,
                "Gold/Mart model generated by metadata-configured runner.",
            )
    except Exception:
        return


def main() -> None:
    ap = argparse.ArgumentParser(description="Run metadata-configured Gold/Mart SQL models")
    ap.add_argument("--config", default="metadata/gold_models.yaml")
    ap.add_argument("--models", default="")
    ap.add_argument("--layers", default="", help="Comma-separated layers, e.g. gold or mart")
    ap.add_argument("--mode", choices=["summary", "run"], default="run")
    ap.add_argument("--openmetadata-config", default="metadata/openmetadata_config.yaml")
    args = ap.parse_args()

    runtime, connections, rule_sets, models = load_gold_config(args.config)
    chosen = selected_models(models, args.models, args.layers)
    Path(runtime.local_result_dir).mkdir(parents=True, exist_ok=True)

    if args.mode == "summary":
        print(json.dumps([m.__dict__ for m in chosen], indent=2, ensure_ascii=False))
        return

    from src.common.spark_session import build_spark
    spark = build_spark(runtime.app_name)
    emitter = None
    if OpenMetadataEmitter is not None:
        try:
            emitter = OpenMetadataEmitter.from_file(args.openmetadata_config)
        except Exception:
            emitter = None

    run_results: List[Dict[str, Any]] = []
    all_quality: List[Dict[str, Any]] = []
    try:
        for model in chosen:
            started = time.time()
            if model.engine == "spark_sql":
                df = spark.sql(model.sql)
                rows = df.count()
                from src.common.config_io import write_iceberg
                write_iceberg(df, model.target, partition_by=model.partition_by, write_mode=model.write_mode)
                quality = run_quality(df, model, expand_quality_rules(model, rule_sets))
                elapsed = round(time.time() - started, 3)
            elif model.engine == "trino":
                run_trino_statement(connections, model)
                rows = -1
                quality = []
                elapsed = round(time.time() - started, 3)
            else:
                raise GoldModelError(f"Unsupported engine: {model.engine}")

            metrics = {
                "model": model.name,
                "target": model.target,
                "engine": model.engine,
                "rows": int(rows),
                "elapsed_seconds": elapsed,
                "status": "success",
                "timestamp_ms": int(time.time() * 1000),
            }
            run_results.append(metrics)
            all_quality.extend(quality)
            emit_model_openmetadata(emitter, runtime, model)

        write_metrics(spark, runtime, run_results, runtime.audit_table)
        write_metrics(spark, runtime, all_quality, runtime.quality_table)

    finally:
        spark.stop()

    out_path = Path(runtime.local_result_dir) / "gold_model_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"models": run_results, "quality": all_quality}, f, indent=2, ensure_ascii=False)
    print(json.dumps({"status": "success", "output": str(out_path), "models": len(run_results)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
