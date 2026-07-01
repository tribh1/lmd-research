from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

BASE = "/opt/lakehouse"
CONF = f"{BASE}/metadata/pipeline_config.yaml"

with DAG(
    dag_id="metadata_driven_lakehouse_experiments",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "thesis", "experiment"],
) as dag:
    ingest_customers = BashOperator(task_id="ingest_customers", bash_command=f"cd {BASE} && spark-submit src/jobs/01_batch_ingest_raw.py --config {CONF} --table customers --scale 1gb")
    ingest_orders = BashOperator(task_id="ingest_orders", bash_command=f"cd {BASE} && spark-submit src/jobs/01_batch_ingest_raw.py --config {CONF} --table orders --scale 1gb")
    silver_customers = BashOperator(task_id="silver_customers", bash_command=f"cd {BASE} && spark-submit src/jobs/02_work_to_silver.py --config {CONF} --table customers")
    silver_orders = BashOperator(task_id="silver_orders", bash_command=f"cd {BASE} && spark-submit src/jobs/02_work_to_silver.py --config {CONF} --table orders")
    gold_mart = BashOperator(task_id="gold_mart", bash_command=f"cd {BASE} && spark-submit src/jobs/05_gold_mart.py --config {CONF}")
    run_experiments = BashOperator(task_id="run_experiments", bash_command=f"cd {BASE} && python -m src.experiments.run_all --config {CONF} --out results/experiment_results.json")

    [ingest_customers, ingest_orders] >> [silver_customers, silver_orders] >> gold_mart >> run_experiments
