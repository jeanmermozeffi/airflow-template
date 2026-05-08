"""
Factories de TaskGroups réutilisables — Pôle Data SYNELIA.

Encapsule les patterns DAG les plus courants sous forme de TaskGroups
prêts à l'emploi pour éviter la duplication dans les DAGs.

Patterns disponibles :
  - build_etl_task_group   : extract → validate_count → load (pipeline standard)
  - build_dq_task_group    : contrôles qualité groupés (count, nulls, duplicates…)
  - build_notify_task_group: notification sur succès / échec en fin de DAG

Exemple dans un DAG :

    from orchestration.airflow.task_group_factory import build_etl_task_group, build_dq_task_group
    from orchestration.bi.orangescrum import extract_tasks, load_to_staging, run_dq_checks

    with DAG("dag_bi_orangescrum_extract_tasks", ...) as dag:
        etl_group = build_etl_task_group(
            group_id="extract_and_load",
            extract_callable=extract_tasks,
            load_callable=load_to_staging,
        )
        dq_group = build_dq_task_group(
            group_id="data_quality",
            checks_callable=run_dq_checks,
        )
        etl_group >> dq_group
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


# ── Pattern ETL standard ──────────────────────────────────────────────────────

def build_etl_task_group(
    group_id: str,
    extract_callable: Callable,
    load_callable: Callable,
    validate_callable: Optional[Callable] = None,
    extract_kwargs: Optional[dict] = None,
    load_kwargs: Optional[dict] = None,
    validate_kwargs: Optional[dict] = None,
    tooltip: str = "",
):
    """Construit un TaskGroup au pattern extract → [validate] → load.

    Args:
        group_id:          Identifiant du groupe (ex. "extract_and_load").
        extract_callable:  Fonction d'extraction (PythonOperator).
        load_callable:     Fonction de chargement (PythonOperator).
        validate_callable: Validation optionnelle entre extract et load
                           (ex. check de volume avant chargement en DWH).
        extract_kwargs:    op_kwargs pour la task d'extraction.
        load_kwargs:       op_kwargs pour la task de chargement.
        validate_kwargs:   op_kwargs pour la task de validation.
        tooltip:           Description affichée dans l'UI Airflow.

    Returns:
        TaskGroup Airflow contenant les tasks chaînées.
    """
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup

    tooltip = tooltip or f"Pattern ETL — {group_id}"

    with TaskGroup(group_id=group_id, tooltip=tooltip) as tg:
        extract_task = PythonOperator(
            task_id="extract",
            python_callable=extract_callable,
            op_kwargs=extract_kwargs or {},
        )

        if validate_callable:
            validate_task = PythonOperator(
                task_id="validate_extraction",
                python_callable=validate_callable,
                op_kwargs=validate_kwargs or {},
            )
            load_task = PythonOperator(
                task_id="load",
                python_callable=load_callable,
                op_kwargs=load_kwargs or {},
            )
            extract_task >> validate_task >> load_task
        else:
            load_task = PythonOperator(
                task_id="load",
                python_callable=load_callable,
                op_kwargs=load_kwargs or {},
            )
            extract_task >> load_task

    return tg


# ── Pattern Data Quality ──────────────────────────────────────────────────────

def build_dq_task_group(
    group_id: str,
    checks_callable: Callable,
    checks_kwargs: Optional[dict] = None,
    tooltip: str = "",
):
    """Construit un TaskGroup pour les contrôles qualité.

    Args:
        group_id:         Identifiant du groupe (ex. "data_quality_checks").
        checks_callable:  Fonction qui exécute les checks DQ.
                          Peut utiliser run_checks() du module data_quality.
        checks_kwargs:    op_kwargs supplémentaires pour la task de checks.
        tooltip:          Description affichée dans l'UI Airflow.

    Returns:
        TaskGroup avec une task start, la task de checks et une task end.
    """
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup

    tooltip = tooltip or f"Contrôles qualité — {group_id}"

    with TaskGroup(group_id=group_id, tooltip=tooltip) as tg:
        start = EmptyOperator(task_id="start_dq")
        run_checks = PythonOperator(
            task_id="run_quality_checks",
            python_callable=checks_callable,
            op_kwargs=checks_kwargs or {},
        )
        end = EmptyOperator(task_id="end_dq")
        start >> run_checks >> end

    return tg


# ── Pattern Notification ──────────────────────────────────────────────────────

def build_notify_task_group(
    group_id: str,
    dag_id: str,
    notify_email: str | list[str] = "",
    notify_slack_conn_id: Optional[str] = None,
    slack_channel: str = "#data-alerts",
    tooltip: str = "",
):
    """Construit un TaskGroup de notification avec branches succès / échec.

    Structure :
        check_status → [notify_on_success | notify_on_failure]

    Args:
        group_id:             Identifiant du groupe.
        dag_id:               Identifiant du DAG parent (pour le message).
        notify_email:         Adresse(s) email pour les alertes.
        notify_slack_conn_id: Connexion Airflow Slack (None = pas de Slack).
        slack_channel:        Canal Slack.
        tooltip:              Description.

    Returns:
        TaskGroup avec la logique de notification.
    """
    from airflow.operators.python import BranchPythonOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup

    from orchestration.airflow.notification import notify_failure, notify_success

    tooltip = tooltip or f"Notifications — {group_id}"

    def _branch(**context) -> str:
        upstream_failed = any(
            ti.state in ("failed", "upstream_failed")
            for ti in context["dag_run"].get_task_instances()
            if ti.task_id != context["task"].task_id
        )
        return f"{group_id}.notify_failure" if upstream_failed else f"{group_id}.notify_success"

    email_list = [notify_email] if isinstance(notify_email, str) and notify_email else (notify_email or [])

    with TaskGroup(group_id=group_id, tooltip=tooltip) as tg:
        branch = BranchPythonOperator(
            task_id="check_status",
            python_callable=_branch,
        )
        on_success = PythonOperator(
            task_id="notify_success",
            python_callable=notify_success,
            op_kwargs={
                "dag_id": dag_id,
                "run_id": "{{ run_id }}",
                "ds": "{{ ds }}",
                "slack_conn_id": notify_slack_conn_id,
                "slack_channel": slack_channel,
            },
        )
        on_failure = PythonOperator(
            task_id="notify_failure",
            python_callable=notify_failure,
            op_kwargs={
                "dag_id": dag_id,
                "task_id": "pipeline",
                "run_id": "{{ run_id }}",
                "ds": "{{ ds }}",
                "email": email_list,
                "slack_conn_id": notify_slack_conn_id,
                "slack_channel": slack_channel,
            },
        )
        done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
        branch >> [on_success, on_failure] >> done

    return tg
