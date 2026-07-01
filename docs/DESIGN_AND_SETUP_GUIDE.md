# Strict Layered Metadata-Driven Kappa Lakehouse v7

Tài liệu này mô tả thiết kế code và hướng dẫn setup/chạy prototype **v7 strict layered jobs**.

Mục tiêu của v7 là tách job xử lý theo từng tầng dữ liệu, không gom chung Raw → Work → Silver → Gold → Data Mart trong một job nguyên khối, nhưng vẫn giữ một logic xử lý thống nhất thông qua metadata configuration và module dùng chung `KappaLayerProcessor`.

---

## 1. Tóm tắt mô hình thiết kế

Tên đề xuất:

> **Strict Layered Metadata-Driven Kappa Lakehouse Architecture**

Nguyên tắc thiết kế:

1. Batch, CDC và streaming đều được chuẩn hóa thành event.
2. Batch snapshot/backfill không ghi thẳng vào Raw/Work/Silver mà được publish vào Kafka theo envelope tương thích Debezium với `op = r`.
3. Mỗi bước chuyển tầng Lakehouse là một job vật lý riêng.
4. Metadata YAML là control plane điều khiển schema, mapping field, rule chuẩn hóa, DQ, PII, SK, FK, SCD, target table và lineage.
5. Embedded metadata được nhúng vào từng bản ghi để phục vụ audit, lineage, replay, governance và schema evolution.
6. OpenMetadata là lớp catalog/governance visibility; pipeline vẫn chạy được nếu OpenMetadata chưa online nhờ fallback local.
7. Airflow chỉ điều phối job; không xử lý dữ liệu trực tiếp.

---

## 2. Sơ đồ kiến trúc tổng thể

```text
+----------------------------------------------------------------------------------+
|                                      Airflow                                      |
|----------------------------------------------------------------------------------|
|  Orchestration / Scheduling / Retry / Backfill / Experiment Automation            |
|  DAG: lakehouse_kappa_strict_layered_dag                                          |
+----------------------------------------+-----------------------------------------+
                                         |
                                         v
+----------------------------------------------------------------------------------+
|                            Metadata Configuration Layer                           |
|----------------------------------------------------------------------------------|
|  metadata/kappa_flows.yaml                                                       |
|    - Kafka topics, schema contract, standardization rules                         |
|    - business logic, data quality rules, PII policy                               |
|    - surrogate key, foreign key, SCD type, target tables                          |
|                                                                                  |
|  metadata/kappa_batch_sources.yaml                                                |
|    - batch snapshot source, backfill query, target Kafka topic                    |
|                                                                                  |
|  metadata/gold_models.yaml                                                        |
|    - Gold model, Mart model, SQL logic, upstream dependencies, model quality      |
|                                                                                  |
|  metadata/openmetadata_config.yaml                                                |
|    - OpenMetadata endpoint, tag mapping, fallback mode                            |
|                                                                                  |
|  metadata/job_execution_plan.yaml                                                 |
|    - strict layered execution plan                                                |
+----------------------------------------+-----------------------------------------+
                                         |
                                         v
+----------------------------------------------------------------------------------+
|                                  Ingestion Layer                                  |
|----------------------------------------------------------------------------------|
|  Batch Snapshot / Historical Backfill                                             |
|       -> kappa_batch_to_event.py                                                  |
|       -> Debezium-compatible event, op = r                                        |
|       -> Kafka topic theo bảng                                                    |
|                                                                                  |
|  CDC Source DB                                                                    |
|       -> Debezium CDC, op = c/u/d/r                                               |
|       -> Kafka topic theo bảng                                                    |
+----------------------------------------+-----------------------------------------+
                                         |
                                         v
+----------------------------------------------------------------------------------+
|                            Strict Layered Processing Jobs                         |
|----------------------------------------------------------------------------------|
|  Job 1: Kafka -> Raw                                                              |
|  Job 2: Raw -> Work                                                               |
|  Job 3: Work -> Silver / Quarantine                                               |
|  Job 4: Silver Reconciliation                                                     |
|  Job 5: Silver -> Gold                                                            |
|  Job 6: Gold -> Data Mart                                                         |
+----------------------------------------+-----------------------------------------+
                                         |
                                         v
+----------------------------------------------------------------------------------+
|                              Serving & Governance Layer                           |
|----------------------------------------------------------------------------------|
|  Trino / BI / AI / Dashboard / OpenMetadata                                       |
+----------------------------------------------------------------------------------+
```

---

## 3. Luồng dữ liệu strict layered

```text
Batch snapshot / backfill
        |
        v
Batch-as-event publisher
        |
        v
Kafka topic theo bảng
        |
        v
Job 1: Kafka -> Raw
        |
        v
Raw Iceberg
        |
        v
Job 2: Raw -> Work
        |
        v
Work Iceberg
        |
        v
Job 3: Work -> Silver / Quarantine
        |
        +------------------+
        |                  |
        v                  v
Quarantine Iceberg     Silver Iceberg
                           |
                           v
                Job 4: Reconciliation
                           |
                           v
                    Silver corrected
                           |
                           v
                 Job 5: Silver -> Gold
                           |
                           v
                    Gold Iceberg
                           |
                           v
                 Job 6: Gold -> Data Mart
                           |
                           v
                    Data Mart Iceberg
```

---

## 4. Danh sách job và trách nhiệm

| STT | Job | Code | Script | Input | Output | Trách nhiệm |
|---:|---|---|---|---|---|---|
| 0 | Batch-as-event Publisher | `src/jobs/kappa_batch_to_event.py` | `scripts/run_kappa_batch_publish.sh` | Source DB snapshot/backfill | Kafka topic | Chuyển batch thành Debezium-compatible event `op=r` |
| 1 | Kafka to Raw Writer | `src/jobs/kappa_config_pipeline.py` | `scripts/run_kappa_config.sh` | Kafka topic | Raw Iceberg | Parse Debezium, nhúng metadata, ghi Raw |
| 2 | Raw to Work Processor | `src/jobs/kappa_raw_to_work.py` | `scripts/run_kappa_raw_to_work.sh` | Raw Iceberg | Work Iceberg | Chuẩn hóa, business logic, SK/FK preparation |
| 3 | Work to Silver Processor | `src/jobs/kappa_work_to_silver.py` | `scripts/run_kappa_work_to_silver.sh` | Work Iceberg | Silver + Quarantine | DQ, quarantine, PII masking, SCD/fact merge |
| 4 | Reconciliation Processor | `src/jobs/reconcile_unknown_fk.py` | `scripts/run_reconcile_unknown_fk.sh` | Silver fact + dim | Corrected Silver fact | Xử lý late-arriving dimension, unknown FK |
| 5 | Gold Model Runner | `src/jobs/gold_model_runner.py` | `scripts/run_gold_models.sh` | Silver | Gold | Chạy model `layer=gold` |
| 6 | Mart Model Runner | `src/jobs/gold_model_runner.py` | `scripts/run_mart_models.sh` | Gold/Silver | Data Mart | Chạy model `layer=mart` |
| 7 | OpenMetadata Sync | `src/jobs/openmetadata_sync.py` | `scripts/sync_openmetadata.sh` | Config + runtime metadata | OpenMetadata/fallback JSON | Register asset, lineage, embedded metadata |
| 8 | Experiment Runner | `src/jobs/experiment_runner_airflow.py` | `scripts/run_airflow_experiments.sh` | Audit/results | Experiment JSON | Chạy bộ thực nghiệm |
| 9 | Dashboard Builder | `src/jobs/experiment_dashboard_builder.py` | `scripts/run_build_dashboard.sh` | Experiment results | Dashboard Markdown/JSON | Tổng hợp dashboard |

---

## 5. Vai trò các module lõi

| Module | Vai trò |
|---|---|
| `src/common/kappa_registry.py` | Load và validate `metadata/kappa_flows.yaml` |
| `src/common/kappa_transform.py` | Parse Debezium event, mapping field, embedded metadata, standardization, business logic, SK/FK |
| `src/common/kappa_layer_processor.py` | Module dùng chung cho Raw → Work và Work → Silver, tránh duplicate logic |
| `src/common/kappa_quality.py` | Rule engine cho data quality |
| `src/common/config_masking.py` | PII masking: `sha256`, `email_mask`, `phone_mask`, `nullify`, `last4` |
| `src/common/kappa_merge.py` | Iceberg merge, SCD1, SCD2, fact upsert, schema evolution |
| `src/common/kappa_event_envelope.py` | Sinh Debezium-compatible event cho batch snapshot/backfill |
| `src/common/kappa_openmetadata.py` | Emit asset, lineage, DQ metrics, embedded metadata sang OpenMetadata hoặc fallback local |
| `src/common/config_io.py` | Ghi Iceberg append/overwrite/merge |
| `src/common/spark_session.py` | Khởi tạo SparkSession với Iceberg, Hive Metastore, MinIO/S3A |

---

## 6. Embedded metadata fields

Các trường metadata được nhúng vào bản ghi từ giai đoạn Kafka → Raw và tiếp tục đi qua các tầng sau.

| Field | Ý nghĩa |
|---|---|
| `_meta_event_id` | ID event |
| `_meta_source_system` | Hệ thống nguồn |
| `_meta_source_database` | Database nguồn |
| `_meta_source_schema` | Schema nguồn |
| `_meta_source_table` | Bảng nguồn |
| `_meta_source_operation` | CDC op: `c`, `u`, `d`, `r` |
| `_meta_source_ts_ms` | Timestamp nguồn |
| `_meta_kafka_topic` | Kafka topic |
| `_meta_kafka_partition` | Kafka partition |
| `_meta_kafka_offset` | Kafka offset |
| `_meta_micro_batch_id` | Spark micro-batch id |
| `_meta_ingest_ts` | Thời điểm ingest |
| `_meta_config_version` | Version cấu hình |
| `_meta_pipeline_name` | Tên flow |
| `_meta_layer` | Layer hiện tại: raw/work/silver/quarantine |
| `_meta_record_hash` | Hash payload, dùng cho replay và SCD2 change detection |
| `_meta_schema_hash` | Hash schema contract |
| `_meta_dq_errors` | Danh sách rule DQ bị lỗi |
| `_meta_dq_passed` | Cờ đạt DQ |
| `_meta_pii_tags` | Danh sách PII tags |
| `_meta_lineage` | Lineage context |
| `_meta_is_deleted` | Cờ soft delete |
| `_meta_deleted_at` | Thời điểm delete |
| `_meta_closed_by_event_id` | Event đóng version SCD2 |
| `_meta_closed_at` | Thời điểm đóng version SCD2 |
| `_meta_reconciled_at` | Thời điểm reconcile FK |
| `_meta_reconciled_by` | Job reconcile |

---

## 7. Điều kiện môi trường

### 7.1. Phần mềm cần có trên máy host

- Docker Engine và Docker Compose plugin.
- Python 3.10+ khuyến nghị.
- `unzip`, `curl`, `bash`.
- Tối thiểu 8 GB RAM cho demo nhỏ; khuyến nghị 16–32 GB nếu chạy Spark/Kafka/Trino cùng lúc.

### 7.2. Thành phần hạ tầng trong `docker-compose.yml`

| Service | Cổng host | Vai trò |
|---|---:|---|
| `postgres-source` | 5432 | Source DB có bật logical WAL cho CDC |
| `postgres-hms` | 5433 | PostgreSQL backend cho Hive Metastore |
| `minio` | 9000/9001 | S3-compatible object storage |
| `zookeeper` | internal | Kafka dependency |
| `kafka` | 9092 | Kafka broker |
| `debezium` | 8083 | Kafka Connect/Debezium |
| `hive-metastore` | 9083 | Hive Metastore cho Iceberg catalog |
| `spark` | 7077/8080 | Spark master/container chạy job |
| `trino` | 8088 | Query/serving engine |

> Lưu ý: file compose hiện tại chưa chạy OpenMetadata container. Tích hợp OpenMetadata có fallback ghi payload JSON vào `results/openmetadata_events` khi OpenMetadata offline. Nếu muốn chạy OpenMetadata thật, cần bổ sung stack OpenMetadata riêng hoặc trỏ `OPENMETADATA_URL` đến instance sẵn có.

---

## 8. Setup môi trường

### 8.1. Giải nén package

```bash
unzip lakehouse_kappa_config_driven_pack_v7_strict_layered_jobs.zip
cd lakehouse_v7
```

### 8.2. Kiểm tra `.env`

File `.env` mặc định:

```bash
POSTGRES_USER=lakehouse
POSTGRES_PASSWORD=lakehouse
SOURCE_DB=source_db
HMS_DB=hms_db
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
MINIO_ENDPOINT=http://minio:9000
OPENMETADATA_URL=http://openmetadata:8585/api
TRINO_HOST=localhost
TRINO_PORT=8080
```

Nếu chạy job trong container Spark, các hostname như `kafka`, `minio`, `hive-metastore`, `trino` dùng được vì nằm trong cùng Docker network.

Nếu chạy PySpark trực tiếp trên host, các hostname nội bộ như `hive-metastore` và `minio` có thể không resolve. Khuyến nghị chạy job trong container `spark` hoặc cấu hình lại endpoint về `localhost` tương ứng.

### 8.3. Start hạ tầng

```bash
docker compose up -d
```

Kiểm tra container:

```bash
docker compose ps
```

Kiểm tra MinIO console:

```text
http://localhost:9001
user/password: minioadmin/minioadmin
```

Kiểm tra Kafka Connect/Debezium:

```bash
curl http://localhost:8083/connectors
```

Kiểm tra Trino:

```text
http://localhost:8088
```

### 8.4. Cài Python dependencies trong Spark container

Khuyến nghị chạy job trong container Spark:

```bash
docker compose exec spark bash
cd /opt/lakehouse
pip install -r requirements.txt
```

Nếu container không cho phép ghi global package, dùng:

```bash
python -m pip install --user -r requirements.txt
```

### 8.5. Cấu hình Spark packages

Các job PySpark cần Iceberg, Hadoop AWS/S3A, Kafka connector và PostgreSQL JDBC. Nếu image Spark chưa có sẵn jar, cấu hình `PYSPARK_SUBMIT_ARGS` trước khi chạy job:

```bash
export PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 pyspark-shell"
```

Với môi trường không có Internet, cần pre-load các jar này vào image/container Spark.

---

## 9. Chuẩn bị dữ liệu nguồn

### 9.1. Tạo source schema

`postgres-source` tự mount `./sql` vào `/docker-entrypoint-initdb.d`, nên schema có thể được tạo khi container PostgreSQL khởi tạo lần đầu.

Nếu cần chạy lại thủ công từ host:

```bash
psql postgresql://lakehouse:lakehouse@localhost:5432/source_db -f sql/01_source_schema.sql
```

Hoặc từ container PostgreSQL:

```bash
docker compose exec postgres-source psql -U lakehouse -d source_db -f /docker-entrypoint-initdb.d/01_source_schema.sql
```

### 9.2. Sinh dữ liệu mẫu

Chạy trong thư mục project:

```bash
python scripts/generate_data.py --scale small --out data/generated/small
```

Nếu script yêu cầu scale khác, xem help:

```bash
python scripts/generate_data.py --help
```

### 9.3. Load dữ liệu vào PostgreSQL source

Từ host:

```bash
python scripts/load_csv_to_postgres.py \
  --input data/generated/small \
  --dsn postgresql://lakehouse:lakehouse@localhost:5432/source_db
```

Nếu chạy trong container Spark, DSN nên dùng hostname Docker:

```bash
python scripts/load_csv_to_postgres.py \
  --input data/generated/small \
  --dsn postgresql://lakehouse:lakehouse@postgres-source:5432/source_db
```

---

## 10. Đăng ký Debezium CDC connector

Đăng ký connector:

```bash
./scripts/register_debezium.sh
```

Kiểm tra:

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/postgres-source/status
```

Connector sẽ tạo CDC event vào các topic như:

```text
cdc.public.src_customer
cdc.public.src_product
cdc.public.src_order
```

Tên topic cần khớp với `metadata/kappa_flows.yaml`.

---

## 11. Chạy pipeline strict layered

### 11.1. Bước 0 — kiểm tra cấu hình flow

```bash
MODE=summary ./scripts/run_kappa_config.sh
```

Kết quả sẽ in danh sách flow, topic, bảng Raw/Work/Silver, DQ rules, PII columns, FK.

### 11.2. Bước 1 — publish batch snapshot/backfill vào Kafka

Dry-run để xem event mẫu:

```bash
DRY_RUN=true JOBS=customer_initial_snapshot ./scripts/run_kappa_batch_publish.sh
```

Publish snapshot thật:

```bash
./scripts/run_kappa_batch_publish.sh
```

Backfill theo khoảng thời gian:

```bash
JOBS=order_backfill_by_time_window \
FROM_TS="2026-06-01 00:00:00" \
TO_TS="2026-06-02 00:00:00" \
./scripts/run_kappa_batch_publish.sh
```

### 11.3. Bước 2 — Kafka → Raw

Job này là streaming job dài hạn:

```bash
MODE=stream-raw-only ./scripts/run_kappa_config.sh
```

Chạy riêng một flow:

```bash
FLOWS=dim_customer MODE=stream-raw-only ./scripts/run_kappa_config.sh
```

> Lưu ý vận hành: đây là job streaming, sẽ không tự kết thúc. Khi chạy thử bằng terminal, mở terminal khác để chạy các bước tiếp theo sau khi Raw đã có dữ liệu. Trong production nên submit job này bằng Airflow SparkSubmitOperator/KubernetesSparkOperator thay vì BashOperator blocking.

### 11.4. Bước 3 — Raw → Work

Chạy toàn bộ flow:

```bash
./scripts/run_kappa_raw_to_work.sh
```

Chạy riêng một flow:

```bash
FLOWS=dim_customer ./scripts/run_kappa_raw_to_work.sh
```

Chạy theo window:

```bash
FLOWS=fact_order \
FROM_TS="2026-06-01 00:00:00" \
TO_TS="2026-06-02 00:00:00" \
BATCH_ID=2026060101 \
./scripts/run_kappa_raw_to_work.sh
```

Chạy giới hạn 1.000 bản ghi để test:

```bash
FLOWS=dim_customer LIMIT=1000 ./scripts/run_kappa_raw_to_work.sh
```

Output metrics:

```text
results/raw_to_work_results.json
```

### 11.5. Bước 4 — Work → Silver/Quarantine

Chạy toàn bộ flow:

```bash
./scripts/run_kappa_work_to_silver.sh
```

Chạy riêng một flow:

```bash
FLOWS=fact_order ./scripts/run_kappa_work_to_silver.sh
```

Chạy theo window:

```bash
FLOWS=fact_order \
FROM_TS="2026-06-01 00:00:00" \
TO_TS="2026-06-02 00:00:00" \
BATCH_ID=2026060102 \
./scripts/run_kappa_work_to_silver.sh
```

Output metrics:

```text
results/work_to_silver_results.json
```

### 11.6. Bước 5 — reconcile unknown FK

```bash
./scripts/run_reconcile_unknown_fk.sh
```

Chạy riêng job:

```bash
JOBS=reconcile_fact_order_customer_sk ./scripts/run_reconcile_unknown_fk.sh
```

Output:

```text
results/reconciliation_results.json
```

### 11.7. Bước 6 — Silver → Gold

```bash
./scripts/run_gold_models.sh
```

Script mặc định chạy:

```bash
LAYERS=gold
```

Chạy riêng model:

```bash
MODELS=gold_daily_revenue ./scripts/run_gold_models.sh
```

Output:

```text
results/gold_models/gold_model_results.json
```

### 11.8. Bước 7 — Gold → Data Mart

```bash
./scripts/run_mart_models.sh
```

Script mặc định chạy:

```bash
LAYERS=mart
```

Chạy riêng model:

```bash
MODELS=mart_sales_dashboard ./scripts/run_mart_models.sh
```

Output:

```text
results/gold_models/mart_model_results.json
```

### 11.9. Bước 8 — đồng bộ OpenMetadata

```bash
PRINT_SUMMARY=true ./scripts/sync_openmetadata.sh
```

Nếu OpenMetadata offline, payload sẽ được ghi local vào:

```text
results/openmetadata_events/
```

### 11.10. Bước 9 — chạy thực nghiệm và dashboard

Chạy bộ thực nghiệm:

```bash
./scripts/run_airflow_experiments.sh
```

Sinh dashboard:

```bash
./scripts/run_build_dashboard.sh
```

Output:

```text
results/dashboard/dashboard.md
results/dashboard/dashboard_summary.json
```

---

## 12. Chạy qua Airflow

DAG:

```text
lakehouse_kappa_strict_layered_dag
```

Task dependency:

```text
explain_flow
  -> validate_kappa_flow_config
  -> sync_static_openmetadata_assets
  -> publish_initial_snapshot_as_events
  -> kafka_to_raw_writer_stream
  -> raw_to_work_processor
  -> work_to_silver_processor
  -> reconcile_late_arriving_foreign_keys
  -> silver_to_gold_models
  -> gold_to_mart_models
  -> run_experiment_suite
  -> build_experiment_dashboard
```

Lưu ý: Airflow không có trong `docker-compose.yml` hiện tại. Nếu muốn chạy DAG thật, cần bổ sung Airflow stack hoặc copy DAG vào môi trường Airflow hiện có. Với prototype luận văn, có thể mô tả Airflow DAG và chạy các script thủ công theo đúng thứ tự.

Cảnh báo vận hành: `kafka_to_raw_writer_stream` là streaming job dài hạn. Nếu dùng `BashOperator`, task sẽ giữ trạng thái running. Production nên chuyển sang operator submit Spark job, ví dụ `SparkSubmitOperator` hoặc Kubernetes Spark operator.

---

## 13. Kiểm tra dữ liệu sau từng tầng

Có thể dùng Spark SQL hoặc Trino để kiểm tra.

Ví dụ bằng Trino CLI nếu đã có catalog cấu hình đúng:

```sql
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.raw;
SHOW TABLES FROM lakehouse.work;
SHOW TABLES FROM lakehouse.silver;
SHOW TABLES FROM lakehouse.gold;
SHOW TABLES FROM lakehouse.mart;

SELECT count(*) FROM lakehouse.raw.customer_events;
SELECT count(*) FROM lakehouse.work.customer_work;
SELECT count(*) FROM lakehouse.silver.dim_customer;
SELECT count(*) FROM lakehouse.gold.daily_revenue;
SELECT count(*) FROM lakehouse.mart.sales_dashboard;
```

Nếu dùng PySpark:

```python
spark.sql("SHOW TABLES IN lakehouse.raw").show()
spark.sql("SELECT count(*) FROM lakehouse.raw.customer_events").show()
```

---

## 14. Cấu hình thêm flow/table mới

Để thêm bảng mới, chỉ cần thêm block vào `metadata/kappa_flows.yaml`.

Checklist:

1. Khai báo `name`, `entity_type`, `source.topics`.
2. Khai báo `target.raw_table`, `target.work_table`, `target.silver_table`, `target.quarantine_table`.
3. Khai báo `natural_key`, `sequence_column`.
4. Khai báo `schema_contract.columns` với `path` theo Debezium payload.
5. Khai báo `standardization.rules`.
6. Khai báo `business_logic.derive_columns` nếu có.
7. Khai báo `data_quality.rules`.
8. Khai báo `pii_policy` nếu có dữ liệu nhạy cảm.
9. Khai báo `surrogate_key`.
10. Khai báo `foreign_keys` nếu là fact cần lookup dimension.
11. Nếu cần batch snapshot, thêm job vào `metadata/kappa_batch_sources.yaml`.
12. Nếu cần KPI/Mart, thêm model vào `metadata/gold_models.yaml`.

---

## 15. Troubleshooting

### 15.1. Không kết nối được `hive-metastore` hoặc `minio`

Nguyên nhân thường gặp: chạy PySpark trên host, trong khi `spark_session.py` dùng hostname Docker nội bộ.

Cách xử lý:

- Chạy job trong container Spark:

```bash
docker compose exec spark bash
cd /opt/lakehouse
```

- Hoặc chỉnh `spark_session.py` endpoint sang `localhost` nếu chạy trên host.

### 15.2. Thiếu Iceberg/Kafka/S3A jar

Cấu hình:

```bash
export PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 pyspark-shell"
```

Nếu không có Internet trong container, cần build image có sẵn jar.

### 15.3. Kafka → Raw job chạy mãi không dừng

Đây là hành vi đúng vì đó là streaming job. Dùng Ctrl+C để dừng khi chạy thử. Trong production, submit job dưới dạng long-running service.

### 15.4. OpenMetadata offline

Không ảnh hưởng pipeline. Kiểm tra fallback:

```bash
ls results/openmetadata_events
```

### 15.5. Fact có `customer_sk = -1`

Đây là late-arriving dimension. Chạy:

```bash
./scripts/run_reconcile_unknown_fk.sh
```

### 15.6. Gold/Mart không có dữ liệu

Kiểm tra Silver trước:

```sql
SELECT count(*) FROM lakehouse.silver.fact_order;
```

Nếu Silver rỗng, kiểm tra Work và Quarantine:

```sql
SELECT count(*) FROM lakehouse.work.order_work;
SELECT count(*) FROM lakehouse.quarantine.fact_order_failed;
```

---

## 16. Lệnh chạy nhanh end-to-end cho demo

Terminal 1: chạy hạ tầng và dữ liệu:

```bash
docker compose up -d
python scripts/generate_data.py --scale small --out data/generated/small
python scripts/load_csv_to_postgres.py \
  --input data/generated/small \
  --dsn postgresql://lakehouse:lakehouse@localhost:5432/source_db
./scripts/register_debezium.sh
./scripts/run_kappa_batch_publish.sh
```

Terminal 2: chạy Kafka → Raw trong container Spark:

```bash
docker compose exec spark bash
cd /opt/lakehouse
pip install -r requirements.txt
export PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 pyspark-shell"
MODE=stream-raw-only ./scripts/run_kappa_config.sh
```

Terminal 3: sau khi Raw có dữ liệu, chạy các tầng còn lại:

```bash
docker compose exec spark bash
cd /opt/lakehouse
pip install -r requirements.txt
export PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 pyspark-shell"
./scripts/run_kappa_raw_to_work.sh
./scripts/run_kappa_work_to_silver.sh
./scripts/run_reconcile_unknown_fk.sh
./scripts/run_gold_models.sh
./scripts/run_mart_models.sh
./scripts/run_build_dashboard.sh
```

---

## 17. Đoạn mô tả ngắn đưa vào luận văn

Prototype áp dụng kiến trúc Data Lakehouse Kappa metadata-driven theo mô hình strict layered jobs. Thay vì hiện thực một ETL job nguyên khối từ Raw đến Data Mart, hệ thống tách mỗi bước chuyển tầng Lakehouse thành một job vật lý độc lập. Dữ liệu batch snapshot và backfill được chuyển thành bounded event stream tương thích Debezium và publish vào cùng Kafka topic được sử dụng cho CDC event. Tầng Kafka-to-Raw được xử lý bởi một Raw Writer streaming riêng. Bước Raw-to-Work được thực thi bởi một processor riêng cho chuẩn hóa dữ liệu và logic nghiệp vụ. Bước Work-to-Silver được xử lý bởi một governance processor riêng, áp dụng kiểm tra chất lượng dữ liệu, điều hướng Quarantine, masking PII, xử lý SCD và merge vào Silver. Các tầng phân tích phía sau cũng được tách riêng, trong đó Silver-to-Gold và Gold-to-Data-Mart được thực thi bởi các config-driven SQL model runner độc lập.

Mặc dù các job vật lý được tách riêng, hệ thống vẫn bảo toàn một semantics xử lý metadata-driven thống nhất thông qua module dùng chung KappaLayerProcessor. Tất cả các job đều đọc metadata cấu hình tập trung cho schema contract, rule chuẩn hóa, logic nghiệp vụ, chính sách chất lượng dữ liệu, PII masking, surrogate key, foreign key, SCD behavior và target table. Thiết kế này cho phép retry, replay, scale, lineage tracking và kiểm soát vận hành độc lập ở từng tầng Lakehouse, đồng thời tránh trùng lặp logic chuyển đổi dữ liệu.
