from datetime import datetime
from typing import Callable

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from orchestration.airflow.config_loader import load_pipeline_config
from orchestration.db.postgres_utils import validate_required_connections


def _build_connection_check(conn_ids: list[str]) -> Callable[[], None]:
    """Construit la callable de contrôle des connexions requises du pipeline."""

    def _check() -> None:
        if conn_ids:
            validate_required_connections(conn_ids=conn_ids)

    return _check


def build_standard_dag(pipeline_name: str, run_callable: Callable[[], None], description: str) -> DAG:
    """Construit un DAG standardisé pour limiter la duplication entre pipelines."""
    config = load_pipeline_config(pipeline_name=pipeline_name)

    default_args = {
        "owner": config.owner,
        "retries": config.retries,
        "retry_delay": config.retry_delay,
    }

    dag = DAG(
        dag_id=config.dag_id,
        schedule=config.schedule,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        description=description,
        tags=config.tags or [pipeline_name],
        default_args=default_args,
        max_active_runs=1,
    )

    with dag:
        start = EmptyOperator(task_id="start")
        check_connections = PythonOperator(
            task_id="check_connections",
            python_callable=_build_connection_check(conn_ids=config.required_connections),
        )
        run_pipeline = PythonOperator(
            task_id="run_pipeline",
            python_callable=run_callable,
        )
        end = EmptyOperator(task_id="end")

        start >> check_connections >> run_pipeline >> end

    return dag

