from __future__ import annotations

from pyspark.sql import SparkSession


def path_size_bytes(spark: SparkSession, path: str) -> int:
    """Total size in bytes of all files under a Hadoop-compatible path (s3a/file)."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(conf)
    if not fs.exists(hpath):
        return 0
    return fs.getContentSummary(hpath).getLength()
