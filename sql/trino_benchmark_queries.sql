-- Proposed architecture workload
SELECT channel, count(*) AS order_count, sum(total_amount) AS revenue
FROM iceberg.silver.orders
WHERE order_date >= current_date - INTERVAL '90' DAY
GROUP BY channel;

SELECT d.business_date, d.channel, d.gross_revenue, count(c.customer_id) AS customer_count
FROM iceberg.gold.daily_revenue_kpi d
LEFT JOIN iceberg.silver.customers c ON c.province IS NOT NULL
WHERE d.business_date >= current_date - INTERVAL '90' DAY
GROUP BY d.business_date, d.channel, d.gross_revenue;

SELECT category, sum(qty_sold) AS qty_sold, sum(net_sales) AS net_sales
FROM iceberg.gold.product_sales_kpi
GROUP BY category;

-- Baseline workload should use the same logical data but query partitioned Parquet external tables.
