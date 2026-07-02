from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


def _append(df: DataFrame, ident: str) -> None:
    spark = df.sparkSession
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.audit")
    try:
        df.writeTo(ident).using("iceberg").create()
    except Exception:
        df.writeTo(ident).append()


def record_lineage_event(spark: SparkSession, from_entity: str, to_entity: str,
                         job_name: str, batch_id: str, emitted_to_catalog: bool) -> None:
    """Persist a lineage event as immutable audit evidence (thesis Section 3.4.4).

    The same edge is emitted to OpenMetadata; this Iceberg copy is what
    Experiment 2 counts, so lineage evidence survives catalog downtime.
    """
    df = spark.createDataFrame([{
        "from_entity": from_entity,
        "to_entity": to_entity,
        "job_name": job_name,
        "batch_id": batch_id,
        "emitted_to_catalog": emitted_to_catalog,
    }]).withColumn("created_at", F.current_timestamp())
    _append(df, "lakehouse.audit.lineage_events")
