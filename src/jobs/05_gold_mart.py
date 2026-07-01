from __future__ import annotations

import argparse
import uuid
from pyspark.sql import functions as F
from src.common.config import load_config
from src.common.spark_session import build_spark
from src.common.metadata_client import MetadataClient


def create_or_replace(df, ident: str):
    namespace = ".".join(ident.split(".")[:-1])
    spark = df.sparkSession
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    df.writeTo(ident).using("iceberg").createOrReplace()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = load_config(args.config)
    batch_id = str(uuid.uuid4())
    spark = build_spark("gold-mart-build")
    om = MetadataClient(cfg.openmetadata_url)

    orders = spark.table("lakehouse.silver.orders")
    payments = spark.table("lakehouse.silver.payments")
    customers = spark.table("lakehouse.silver.customers")
    products = spark.table("lakehouse.silver.products")
    items = spark.table("lakehouse.silver.order_items")

    daily_revenue = (orders.alias("o")
        .join(payments.alias("p"), "order_id", "left")
        .groupBy(F.to_date("order_date").alias("business_date"), "channel")
        .agg(F.countDistinct("order_id").alias("order_count"), F.sum("total_amount").alias("gross_revenue"), F.sum("amount").alias("paid_amount"))
        .withColumn("_batch_id", F.lit(batch_id)).withColumn("_created_at", F.current_timestamp()))
    create_or_replace(daily_revenue, "lakehouse.gold.daily_revenue_kpi")

    product_sales = (items.alias("i")
        .join(products.alias("p"), "product_id", "left")
        .groupBy("category", "product_id", "product_name")
        .agg(F.sum("quantity").alias("qty_sold"), F.sum("line_amount").alias("net_sales"))
        .withColumn("_batch_id", F.lit(batch_id)).withColumn("_created_at", F.current_timestamp()))
    create_or_replace(product_sales, "lakehouse.gold.product_sales_kpi")

    mart = (daily_revenue.groupBy("business_date", "channel")
        .agg(F.sum("order_count").alias("order_count"), F.sum("gross_revenue").alias("gross_revenue"), F.sum("paid_amount").alias("paid_amount"))
        .withColumn("revenue_gap", F.col("gross_revenue") - F.col("paid_amount")))
    create_or_replace(mart, "lakehouse.mart.sales_dashboard")

    om.emit_lineage("lakehouse.silver.orders", "lakehouse.gold.daily_revenue_kpi", "build-daily-revenue", batch_id)
    om.emit_lineage("lakehouse.silver.payments", "lakehouse.gold.daily_revenue_kpi", "build-daily-revenue", batch_id)
    om.emit_lineage("lakehouse.gold.daily_revenue_kpi", "lakehouse.mart.sales_dashboard", "build-sales-dashboard", batch_id)
    spark.stop()

if __name__ == "__main__":
    main()
