# Thiết kế dữ liệu và thực nghiệm

## 1. Mục tiêu

Bộ dữ liệu phục vụ 5 thực nghiệm trong Chương 4:

1. Metadata discoverability.
2. Governance coverage.
3. Processing performance and scalability.
4. Query performance.
5. Schema evolution.

## 2. Mô hình nguồn PostgreSQL

| Bảng | Vai trò | Cột chính | Dữ liệu nhạy cảm | Dữ liệu gây lỗi có chủ đích |
|---|---|---|---|---|
| `src_customer` | Master khách hàng | `customer_id`, `email`, `province`, `updated_at` | `full_name`, `email`, `telephone`, `address` | email null / sai định dạng |
| `src_product` | Danh mục sản phẩm | `product_id`, `sku`, `category`, `unit_price` | Không | giá âm / SKU null nếu cần mở rộng |
| `src_order` | Giao dịch đơn hàng | `order_id`, `customer_id`, `order_date`, `total_amount` | Không trực tiếp | `total_amount <= 0`, trạng thái sai |
| `src_order_item` | Dòng đơn hàng | `order_item_id`, `order_id`, `product_id`, `quantity` | Không | `quantity <= 0`, `line_amount <= 0` |
| `src_payment` | Thanh toán | `payment_id`, `order_id`, `card_number`, `amount` | `card_number` | amount <= 0, số thẻ chưa mask |
| `src_app_event` | Sự kiện streaming | `event_id`, `event_time`, `session_id` | Có thể mở rộng | trễ thời gian / sự kiện thiếu trường |
| `exp_ground_truth_violation` | Ground truth | `source_table`, `source_pk`, `rule_id` | Không | dùng để tính tỷ lệ phát hiện lỗi |

## 3. Mô hình Medallion

| Layer | Bảng ví dụ | Quy tắc |
|---|---|---|
| Raw | `lakehouse.raw.customers`, `lakehouse.raw.orders` | Bảo toàn dữ liệu nguồn; bổ sung `_batch_id`, `_ingest_ts`, `_row_hash` |
| Work | `lakehouse.work.customers` | Chuẩn hóa kỹ thuật; kiểm tra dữ liệu trước khi governance |
| Silver | `lakehouse.silver.customers` | Chỉ chứa bản ghi pass critical DQ; PII đã được mask |
| Quarantine | `lakehouse.quarantine.customers_failed` | Bản ghi lỗi kèm `_dq_errors`, `_quarantine_reason` |
| Gold | `lakehouse.gold.daily_revenue_kpi`, `lakehouse.gold.product_sales_kpi` | KPI và tổng hợp nghiệp vụ |
| Mart | `lakehouse.mart.sales_dashboard` | Dữ liệu phục vụ báo cáo/BI |
| Audit | `lakehouse.audit.batch_metrics`, `lakehouse.audit.governance_metrics` | Metric thực nghiệm, lineage, chất lượng |

## 4. Ground-truth và cách tính chỉ số

### E1 — Metadata discoverability

- `metadata_coverage_rate = assets_with_description_owner_tag / total_assets * 100`.
- `search_latency = median(response_time(keyword_search_10_queries))`.
- `lineage_depth = max_hops(mart_table -> source_table)`.

### E2 — Governance coverage

- `quality_enforcement_rate = quarantined_ground_truth_violations / injected_ground_truth_violations * 100`.
- `pii_masking_accuracy = correctly_masked_pii_values / total_pii_values * 100`.
- `lineage_auto_capture_rate = jobs_with_lineage / total_jobs * 100`.

### E3 — Processing performance

- `batch_throughput_rows = processed_rows / elapsed_seconds`.
- `batch_throughput_mb = processed_mb / elapsed_seconds`.
- `streaming_latency_p50/p95/p99 = ingest_visible_ts - event_created_ts`.

### E4 — Query performance

- `query_exec_time_ms = median(3 executions)`.
- `scan_reduction_ratio = (baseline_scanned_mb - proposed_scanned_mb) / baseline_scanned_mb * 100`.

### E5 — Schema evolution

- `schema_evolution_latency = visible_in_all_layers_ts - source_alter_table_ts`.
- `pipeline_availability = successful_probe_queries / total_probe_queries * 100`.

## 5. Baseline

Baseline sử dụng cùng dữ liệu nguồn, cùng MinIO và cùng Trino, nhưng:

- ghi trực tiếp partitioned Parquet vào thư mục layer;
- không dùng OpenMetadata làm control plane;
- không có Work layer để chuẩn hóa và quarantine;
- không enforce DQ/PII inline;
- lineage, glossary, policy không tự động hóa.
