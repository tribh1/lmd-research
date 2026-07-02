import os

from pyspark.sql import SparkSession


DEFAULT_SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.postgresql:postgresql:42.7.3",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    ]
)


def build_spark(app_name: str) -> SparkSession:
    packages = os.getenv("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES)
    catalog = os.getenv("LAKEHOUSE_CATALOG", "lakehouse")
    catalog_type = os.getenv("LAKEHOUSE_CATALOG_TYPE", "hive")
    warehouse = os.getenv("LAKEHOUSE_WAREHOUSE", "s3a://lakehouse-raw/warehouse")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", packages)
    )
    # Reuse the pre-resolved jar cache baked into the Docker image (docker/spark.Dockerfile).
    ivy_dir = os.getenv("SPARK_IVY_DIR")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)
    builder = (
        builder
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", catalog_type)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.sql.shuffle.partitions", "8")
    )

    if catalog_type == "hive":
        builder = builder.config(
            f"spark.sql.catalog.{catalog}.uri",
            os.getenv("LAKEHOUSE_CATALOG_URI", "thrift://hive-metastore:9083"),
        )
    if warehouse.startswith("s3a://"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    return builder.getOrCreate()
