from __future__ import annotations

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.common.table_registry import RuntimeConfig, TableSpec


def table_ident(cfg: RuntimeConfig, layer: str, table_name: str) -> str:
    return f"{cfg.catalog}.{layer}.{table_name}"


def namespace_ident(cfg: RuntimeConfig, layer: str) -> str:
    return f"{cfg.catalog}.{layer}"


def create_namespace(spark: SparkSession, cfg: RuntimeConfig, layer: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_ident(cfg, layer)}")


def iceberg_table_exists(spark: SparkSession, ident: str) -> bool:
    try:
        spark.table(ident).limit(1).count()
        return True
    except Exception:
        return False


def read_source(spark: SparkSession, cfg: RuntimeConfig, table: TableSpec) -> DataFrame:
    source = table.source
    source_type = source.get("type")
    if source_type == "jdbc":
        conn = cfg.connections[source["connection"]]
        opts = conn.options
        reader = (
            spark.read.format("jdbc")
            .option("url", opts["url"])
            .option("user", opts["user"])
            .option("password", opts["password"])
            .option("driver", opts.get("driver", "org.postgresql.Driver"))
        )
        if source.get("query"):
            reader = reader.option("query", source["query"])
        else:
            reader = reader.option("dbtable", source["table"])
        return reader.load()

    if source_type == "file":
        fmt = source.get("format", "parquet")
        reader = spark.read.format(fmt)
        for k, v in (source.get("options") or {}).items():
            reader = reader.option(k, v)
        return reader.load(source["path"])

    raise ValueError(f"Unsupported source type for {table.name}: {source_type}")


def write_iceberg(
    df: DataFrame,
    ident: str,
    *,
    partition_by: Optional[str] = None,
    write_mode: str = "append",
    primary_key: Optional[list[str]] = None,
) -> None:
    spark = df.sparkSession
    namespace = ".".join(ident.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    exists = iceberg_table_exists(spark, ident)
    write_mode = (write_mode or "append").lower()

    if write_mode == "overwrite":
        if exists:
            df.createOrReplaceTempView("_overwrite_src")
            spark.sql(f"INSERT OVERWRITE {ident} SELECT * FROM _overwrite_src")
            spark.catalog.dropTempView("_overwrite_src")
            return
        _create_table(df, ident, partition_by)
        return

    if write_mode == "merge" and exists and primary_key:
        merge_into(df, ident, primary_key)
        return

    if not exists:
        _create_table(df, ident, partition_by)
    else:
        df.writeTo(ident).append()


def _create_table(df: DataFrame, ident: str, partition_by: Optional[str]) -> None:
    if partition_by and partition_by in df.columns:
        df.writeTo(ident).using("iceberg").partitionedBy(F.days(F.col(partition_by))).create()
    else:
        df.writeTo(ident).using("iceberg").create()


def merge_into(df: DataFrame, ident: str, primary_key: list[str]) -> None:
    spark = df.sparkSession
    view_name = "_merge_src"
    df.createOrReplaceTempView(view_name)
    cols = df.columns
    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in primary_key])
    update_clause = ", ".join([f"t.{c} = s.{c}" for c in cols if c not in primary_key])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{c}" for c in cols])
    sql = f"""
    MERGE INTO {ident} t
    USING {view_name} s
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(sql)
    spark.catalog.dropTempView(view_name)
