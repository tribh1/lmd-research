# Thiết kế luồng Kappa duy nhất: Embedded Metadata + Config-Driven ETL + Surrogate Key

## 1. Nguyên tắc thiết kế

Luồng này thay thế cách tách batch/CDC/stream thành nhiều pipeline bằng một **Kappa-only pipeline**. Mọi nguồn dữ liệu được đưa về Kafka event stream:

- Initial load/snapshot: publish thành sự kiện Kafka có cùng envelope với CDC.
- CDC: Debezium đọc WAL/binlog và publish vào Kafka.
- Application events: publish trực tiếp vào Kafka theo envelope chuẩn.

Spark Structured Streaming là engine xử lý duy nhất. Không viết job riêng cho từng bảng; job đọc `metadata/kappa_flows.yaml` để biết topic, trường, kiểu dữ liệu, rule chuẩn hóa, rule làm sạch, PII, business logic, SK/FK và bảng đích.

## 2. Luồng xử lý

```text
Source DB / App / File Snapshot
        |
        v
Kafka unified event topics
        |
        v
Kappa Config Pipeline
        |
        +--> Raw Iceberg: lưu payload + embedded metadata
        |
        +--> Work Iceberg: chuẩn hóa kiểu dữ liệu, trim/upper/lower, derive column
        |
        +--> Silver Iceberg: DQ, masking, SK/FK, SCD, quarantine
        |
        +--> Gold/Mart model: SQL model cấu hình
```

## 3. Embedded metadata

Mỗi bản ghi ở Raw/Work/Silver có metadata đi kèm:

| Metadata column | Ý nghĩa |
|---|---|
| `_meta_event_id` | ID sự kiện sinh từ topic/partition/offset |
| `_meta_source_system` | hệ thống nguồn |
| `_meta_source_database` | database nguồn |
| `_meta_source_schema` | schema nguồn |
| `_meta_source_table` | bảng nguồn |
| `_meta_source_operation` | Debezium op: c/u/d/r |
| `_meta_source_ts_ms` | thời gian commit ở nguồn |
| `_meta_kafka_topic` | topic Kafka |
| `_meta_kafka_partition` | partition Kafka |
| `_meta_kafka_offset` | offset Kafka |
| `_meta_ingest_ts` | thời gian lakehouse ingest |
| `_meta_config_version` | version cấu hình YAML |
| `_meta_pipeline_name` | tên flow |
| `_meta_layer` | raw/work/silver/quarantine |
| `_meta_record_hash` | hash nội dung bản ghi |
| `_meta_schema_hash` | hash schema contract |
| `_meta_dq_errors` | danh sách lỗi DQ |
| `_meta_pii_tags` | danh sách trường nhạy cảm |
| `_meta_lineage` | lineage JSON từ topic đến các layer |

## 4. Cấu hình trường bảng

Ví dụ một trường trong `schema_contract`:

```yaml
- {name: customer_id, type: long, nullable: false, path: "after.customer_id"}
- {name: email, type: string, nullable: true, path: "after.email", classification: ["PII"]}
```

`path` trỏ vào Debezium envelope. Với delete event, engine tự đọc `before.*` thay vì `after.*`.

## 5. Cấu hình rule chuẩn hóa/làm sạch

```yaml
standardization:
  use_rule_sets: ["email_standard_rules"]
  rules:
    - {column: customer_segment, action: upper}
    - {name: customer_segment_norm, action: expr, expr: "upper(customer_segment)"}
```

Các action đã hỗ trợ: `trim`, `upper`, `lower`, `cast`, `regexp_replace`, `coalesce`, `expr`.

## 6. Cấu hình logic nghiệp vụ

```yaml
business_logic:
  derive_columns:
    - {name: customer_type, expr: "CASE WHEN customer_segment_norm IN ('VIP','ENTERPRISE') THEN 'HIGH_VALUE' ELSE 'STANDARD' END"}
```

Logic nghiệp vụ dùng Spark SQL expression để dễ cấu hình và audit.

## 7. Cấu hình rule data quality

```yaml
data_quality:
  on_critical_fail: quarantine
  rules:
    - {rule_id: CUST_ID_NOT_NULL, column: customer_id, type: not_null, severity: critical}
    - {rule_id: CUST_EMAIL_VALID, column: email, type: regex, pattern: "^[^@]+@[^@]+\\.[^@]+$", severity: warning}
```

Các rule đã hỗ trợ: `not_null`, `regex`, `positive`, `non_negative`, `in_set`, `range`, `expr`.

## 8. Cấu hình PII masking

```yaml
pii_policy:
  full_name: {method: sha256}
  email: {method: email_mask}
  telephone: {method: phone_mask}
  address: {method: nullify}
```

Tái sử dụng masking engine hiện có: `sha256`, `email_mask`, `phone_mask`, `last4`, `nullify`.

## 9. Cấu hình surrogate key

Dimension SCD Type 1:

```yaml
surrogate_key:
  column: product_sk
  method: hash64
  keys: ["product_id"]
  scd_type: 1
```

Dimension SCD Type 2:

```yaml
surrogate_key:
  column: customer_sk
  method: hash64
  keys: ["customer_id"]
  scd_type: 2
  effective_from: "updated_at"
  effective_to_column: "effective_to"
  current_flag_column: "is_current"
```

Fact table:

```yaml
surrogate_key:
  column: order_sk
  method: hash64
  keys: ["order_id"]
  scd_type: none
foreign_keys:
  - name: customer_sk
    lookup_table: "lakehouse.silver.dim_customer"
    lookup_key: ["customer_id"]
    lookup_value: "customer_sk"
    unknown_value: -1
```

Trong streaming/distributed processing, không nên dùng sequence tăng dần kiểu `monotonically_increasing_id()` để làm SK vì không ổn định khi replay. Prototype này dùng deterministic hash64 từ natural key và effective time. Với SCD2, SK = hash(natural_key + effective_from), giúp tái xử lý/replay vẫn ra cùng SK.

## 10. Cách chạy

Validate cấu hình:

```bash
cd lakehouse_experiment_pack
MODE=summary ./scripts/run_kappa_config.sh
```

Chạy toàn bộ stream:

```bash
./scripts/run_kappa_config.sh
```

Chạy riêng một flow:

```bash
FLOWS=dim_customer ./scripts/run_kappa_config.sh
```

Chạy model Gold một lần:

```bash
MODE=models-once ./scripts/run_kappa_config.sh
```

