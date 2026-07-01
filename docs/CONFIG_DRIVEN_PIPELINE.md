# Metadata-configured Lakehouse Pipeline

## 1. Mục tiêu

Bộ code này bổ sung cơ chế **metadata-driven orchestration** cho prototype luận văn. Thay vì viết riêng job Spark cho từng bảng, pipeline chỉ đọc cấu hình tại `metadata/config_driven_tables.yaml` và tự động chạy các bước:

```text
Source -> Raw -> Work -> Silver -> Gold -> Mart -> Audit/OpenMetadata
```

Khi cần thêm bảng mới, chỉ cần thêm một block trong YAML gồm nguồn dữ liệu, khóa chính, watermark, schema contract, quy tắc DQ, cột PII, biến đổi Work/Silver và metadata nghiệp vụ. Không cần viết thêm job ETL.

## 2. Thành phần code mới

```text
metadata/config_driven_tables.yaml                  # registry metadata cho bảng và model
src/common/table_registry.py                        # đọc/validate metadata cấu hình
src/common/config_io.py                             # đọc source và ghi Iceberg generic
src/common/config_transform.py                      # schema contract, derive, deduplicate
src/common/config_quality.py                        # generic DQ rule engine
src/common/config_masking.py                        # generic PII masking engine
src/common/config_audit.py                          # ghi audit event, quality result
src/common/metadata_provider.py                     # emit metadata/lineage/quality sang OpenMetadata nếu bật
src/jobs/config_driven_etl.py                       # Spark job generic Raw/Work/Silver/Gold/Mart
src/orchestrator/config_driven_runner.py            # runner sinh spark-submit command từ registry
scripts/run_config_driven.sh                        # chạy nhanh toàn bộ pipeline
airflow/dags/config_driven_lakehouse_dag.py         # Airflow DAG tự sinh task theo registry
```

## 3. Cấu hình bảng mới

Ví dụ thêm bảng `employees`:

```yaml
tables:
  - name: employees
    enabled: true
    domain: "hr"
    owner: "data-platform-team"
    glossary_terms: ["Employee", "PII"]
    source:
      type: jdbc
      connection: source_postgres
      table: "public.src_employee"
      load_strategy: incremental
    target:
      table_name: "employees"
      partition_by: "updated_at"
      write_mode: merge
    primary_key: ["employee_id"]
    watermark_column: "updated_at"
    schema_contract:
      on_schema_change: add_columns
      columns:
        - {name: employee_id, type: long, nullable: false}
        - {name: employee_code, type: string, nullable: false}
        - {name: full_name, type: string, nullable: true, classification: ["PII"]}
        - {name: email, type: string, nullable: true, classification: ["PII"]}
        - {name: department_code, type: string, nullable: true}
        - {name: updated_at, type: timestamp, nullable: true}
    transformations:
      work:
        cast_to_schema: true
        derive_columns:
          - {name: department_code_norm, expr: "upper(department_code)"}
      silver:
        deduplicate: {keys: ["employee_id"], order_by: "updated_at"}
    governance:
      pii_columns:
        full_name: {method: sha256}
        email: {method: email_mask}
      dq_rules:
        - {rule_id: EMP_CODE_NOT_NULL, column: employee_code, type: not_null, severity: critical}
        - {rule_id: EMP_EMAIL_VALID, column: email, type: regex, pattern: "^[^@]+@[^@]+\\.[^@]+$", severity: warning}
```

## 4. Các loại rule hỗ trợ

| Rule type | Ý nghĩa | Ví dụ |
|---|---|---|
| `not_null` | Không được null | `{type: not_null, column: email}` |
| `regex` | Đúng biểu thức định dạng | `{type: regex, column: email, pattern: "..."}` |
| `positive` | Giá trị > 0 | `{type: positive, column: amount}` |
| `non_negative` | Giá trị >= 0 | `{type: non_negative, column: quantity}` |
| `in_set` | Thuộc danh sách hợp lệ | `{type: in_set, column: status, values: [A, B]}` |
| `range` | Trong khoảng min/max | `{type: range, column: age, min: 0, max: 120}` |
| `freshness_hours` | Dữ liệu đủ mới | `{type: freshness_hours, column: updated_at, threshold: 24}` |
| `unique` | Không trùng khóa | `{type: unique, columns: [customer_id]}` |

## 5. Các phương pháp PII masking hỗ trợ

| Method | Ý nghĩa |
|---|---|
| `sha256` | Băm một chiều |
| `email_mask` | Che email dạng `a***@domain.com` |
| `phone_mask` | Che số điện thoại, giữ 4 số cuối |
| `last4` | Che số thẻ, giữ 4 số cuối |
| `nullify` | Gán null |
| `keep` | Giữ nguyên, dùng cho kiểm thử |

## 6. Cách chạy

Kiểm tra cấu hình trước khi chạy:

```bash
python src/jobs/config_driven_etl.py \
  --config metadata/config_driven_tables.yaml \
  --stage summary
```

Chạy toàn bộ bảng và model:

```bash
./scripts/run_config_driven.sh
```

Chạy riêng một bảng:

```bash
TABLES=customers STAGE=all ./scripts/run_config_driven.sh
```

Chạy riêng một stage:

```bash
TABLES=orders STAGE=raw ./scripts/run_config_driven.sh
TABLES=orders STAGE=work ./scripts/run_config_driven.sh
TABLES=orders STAGE=silver ./scripts/run_config_driven.sh
```

Chạy riêng model Gold/Mart:

```bash
STAGE=models MODELS=daily_revenue_kpi ./scripts/run_config_driven.sh
```

In spark-submit command mà chưa chạy:

```bash
python src/orchestrator/config_driven_runner.py \
  --config metadata/config_driven_tables.yaml \
  --tables customers \
  --stage all \
  --print-only
```

## 7. Luồng runtime

1. `table_registry.py` đọc YAML và validate cấu hình.
2. `config_driven_runner.py` sinh lệnh `spark-submit` chung.
3. `config_driven_etl.py` nhận danh sách bảng, tự chạy Raw/Work/Silver.
4. Raw đọc dữ liệu từ JDBC hoặc file và ghi Iceberg.
5. Work áp schema contract, cast type, derive column.
6. Silver chạy DQ, quarantine bản ghi lỗi, masking PII và ghi bảng Silver.
7. Model Gold/Mart chạy SQL từ YAML.
8. Audit event và quality result được ghi vào `lakehouse.audit`.
9. Nếu bật `openmetadata.enabled=true`, pipeline sẽ emit table metadata, lineage và quality result sang OpenMetadata.

## 8. Gợi ý đưa vào luận văn

Có thể mô tả cơ chế này như sau:

> The prototype implements a metadata-configured orchestration mechanism in which table-specific execution behavior is externalized into a YAML-based metadata registry. Each table definition specifies source connection, schema contract, primary key, watermark, quality rules, PII masking policies, transformation expressions, lineage declarations, and target layer configuration. A generic Spark pipeline reads this registry at runtime and executes Raw, Work, Silver, Gold, and Mart processing without table-specific code changes. This demonstrates the feasibility of metadata-driven orchestration and embedded governance in the proposed Lakehouse architecture.
