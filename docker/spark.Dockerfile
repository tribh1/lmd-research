# Spark image for the MDL-EG prototype: Python dependencies preinstalled and
# Spark runtime packages pre-resolved, so `docker compose exec spark` can run
# scripts/run_all.sh immediately without per-run pip installs or jar downloads.
FROM bitnami/spark:3.5

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-resolve the Iceberg/Kafka/JDBC packages into a shared ivy cache; the
# runtime session picks it up via SPARK_IVY_DIR (see src/common/spark_session.py).
ENV SPARK_IVY_DIR=/opt/ivy-cache
RUN python - <<'PY' && chmod -R a+rX /opt/ivy-cache
from pyspark.sql import SparkSession
packages = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.3",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
])
spark = (SparkSession.builder.master("local[1]").appName("warm-ivy-cache")
         .config("spark.jars.packages", packages)
         .config("spark.jars.ivy", "/opt/ivy-cache")
         .getOrCreate())
spark.stop()
PY

USER 1001
