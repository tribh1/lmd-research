from __future__ import annotations

import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

CONFIG_PATH = os.getenv("LAKEHOUSE_CONFIG", "/opt/lakehouse/metadata/config_driven_tables.yaml")
REPO_DIR = os.getenv("LAKEHOUSE_REPO_DIR", "/opt/lakehouse")


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


registry = load_yaml(CONFIG_PATH)
enabled_tables = [t for t in registry.get("tables", []) if t.get("enabled", True)]
enabled_models = [m for m in registry.get("models", []) if m.get("enabled", True)]

with DAG(
    dag_id="config_driven_lakehouse_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "metadata-driven", "experiment"],
) as dag:
    with TaskGroup("raw_to_silver") as raw_to_silver:
        previous_table_last_task = None
        for table in enabled_tables:
            name = table["name"]
            raw = BashOperator(
                task_id=f"{name}_raw",
                bash_command=(
                    f"cd {REPO_DIR} && "
                    f"python src/orchestrator/config_driven_runner.py --config {CONFIG_PATH} "
                    f"--tables {name} --stage raw"
                ),
            )
            work = BashOperator(
                task_id=f"{name}_work",
                bash_command=(
                    f"cd {REPO_DIR} && "
                    f"python src/orchestrator/config_driven_runner.py --config {CONFIG_PATH} "
                    f"--tables {name} --stage work"
                ),
            )
            silver = BashOperator(
                task_id=f"{name}_silver",
                bash_command=(
                    f"cd {REPO_DIR} && "
                    f"python src/orchestrator/config_driven_runner.py --config {CONFIG_PATH} "
                    f"--tables {name} --stage silver"
                ),
            )
            raw >> work >> silver
            previous_table_last_task = silver

    with TaskGroup("gold_and_mart") as gold_and_mart:
        previous = None
        for model in enabled_models:
            task = BashOperator(
                task_id=f"model_{model['name']}",
                bash_command=(
                    f"cd {REPO_DIR} && "
                    f"python src/orchestrator/config_driven_runner.py --config {CONFIG_PATH} "
                    f"--models {model['name']} --stage models"
                ),
            )
            if previous:
                previous >> task
            previous = task

    raw_to_silver >> gold_and_mart
