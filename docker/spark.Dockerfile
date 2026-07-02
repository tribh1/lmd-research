# Spark runtime for the MDL-EG prototype. Built on the official Python image
# (bitnami/spark was withdrawn from Docker Hub in 2025): OpenJDK 17 + PySpark
# from pip, Python dependencies preinstalled, and the Iceberg/Kafka/JDBC Spark
# packages pre-resolved so `docker compose exec spark` can run
# scripts/run_all.sh immediately with no per-run downloads.
FROM python:3.10-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps bash \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt pyspark==3.5.1

# Pre-resolve the runtime packages into a shared ivy cache; the session builder
# picks it up via SPARK_IVY_DIR (see src/common/spark_session.py).
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

WORKDIR /opt/lakehouse
# The service is an execution host for spark-submit (local mode); keep it alive.
CMD ["sleep", "infinity"]
