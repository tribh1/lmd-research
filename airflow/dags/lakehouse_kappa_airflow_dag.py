from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

BASE_DIR = "/opt/airflow/lakehouse_experiment_pack"


@dag(
    dag_id="lakehouse_kappa_strict_layered_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "kappa", "strict-layered", "metadata-driven", "openmetadata"],
)
def lakehouse_kappa_strict_layered_dag():
    """
    Airflow only orchestrates.

    v7 rule: each task handles one physical layer transition only:
    source snapshot -> Kafka, Kafka -> Raw, Raw -> Work, Work -> Silver/Quarantine,
    Silver -> Gold, Gold -> Mart.
    """

    @task
    def explain_flow() -> str:
        return (
            "Strict layered Kappa flow: batch-as-event -> Kafka; CDC -> Kafka; "
            "Kafka -> Raw; Raw -> Work; Work -> Silver/Quarantine; "
            "Silver -> Gold; Gold -> Mart; metadata and experiments are separate jobs."
        )

    validate_kappa_flow_config = BashOperator(
        task_id="validate_kappa_flow_config",
        bash_command=f"cd {BASE_DIR} && MODE=summary ./scripts/run_kappa_config.sh",
    )

    sync_static_openmetadata = BashOperator(
        task_id="sync_static_openmetadata_assets",
        bash_command=f"cd {BASE_DIR} && PRINT_SUMMARY=true ./scripts/sync_openmetadata.sh",
    )

    publish_initial_snapshot = BashOperator(
        task_id="publish_initial_snapshot_as_events",
        bash_command=f"cd {BASE_DIR} && ./scripts/run_kappa_batch_publish.sh",
    )

    start_kappa_raw_writer = BashOperator(
        task_id="kafka_to_raw_writer_stream",
        bash_command=f"cd {BASE_DIR} && MODE=stream-raw-only ./scripts/run_kappa_config.sh",
    )

    raw_to_work = BashOperator(
        task_id="raw_to_work_processor",
        bash_command=f"cd {BASE_DIR} && ./scripts/run_kappa_raw_to_work.sh",
    )

    work_to_silver = BashOperator(
        task_id="work_to_silver_processor",
        bash_command=f"cd {BASE_DIR} && ./scripts/run_kappa_work_to_silver.sh",
    )

    reconcile_late_arriving_fk = BashOperator(
        task_id="reconcile_late_arriving_foreign_keys",
        bash_command=f"cd {BASE_DIR} && ./scripts/run_reconcile_unknown_fk.sh",
    )

    build_gold_models = BashOperator(
        task_id="silver_to_gold_models",
        bash_command=f"cd {BASE_DIR} && LAYERS=gold ./scripts/run_gold_models.sh",
    )

    build_mart_models = BashOperator(
        task_id="gold_to_mart_models",
        bash_command=f"cd {BASE_DIR} && LAYERS=mart ./scripts/run_mart_models.sh",
    )

    run_experiment_suite = BashOperator(
        task_id="run_experiment_suite",
        bash_command=f"cd {BASE_DIR} && ./scripts/run_airflow_experiments.sh",
    )

    build_dashboard = BashOperator(
        task_id="build_experiment_dashboard",
        bash_command=f"cd {BASE_DIR} && ./scripts/run_build_dashboard.sh",
    )

    (
        explain_flow()
        >> validate_kappa_flow_config
        >> sync_static_openmetadata
        >> publish_initial_snapshot
        >> start_kappa_raw_writer
        >> raw_to_work
        >> work_to_silver
        >> reconcile_late_arriving_fk
        >> build_gold_models
        >> build_mart_models
        >> run_experiment_suite
        >> build_dashboard
    )


lakehouse_kappa_strict_layered_dag()
