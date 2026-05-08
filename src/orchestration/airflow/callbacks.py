"""
Callbacks DAG standardisés — Pôle Data SYNELIA.

Fournit des factories de callbacks prêts à l'emploi pour les DAGs Airflow :
  - on_failure_callback  : échec d'une task ou du DAG
  - on_retry_callback    : relance automatique d'une task
  - on_success_callback  : fin réussie du DAG
  - on_sla_miss_callback : dépassement de SLA

Utilisation typique dans un DAG :

    from orchestration.airflow.callbacks import build_failure_callback, build_sla_miss_callback

    with DAG(
        dag_id="mon_dag",
        default_args={
            "on_failure_callback": build_failure_callback(notify_slack=True),
            "on_retry_callback":   build_retry_callback(),
        },
        on_success_callback=build_success_callback(notify_slack=False),
        sla_miss_callback=build_sla_miss_callback(),
        ...
    ) as dag:
        ...
"""

from __future__ import annotations

import logging
from typing import Callable

logger = logging.getLogger(__name__)


# ── Type alias ────────────────────────────────────────────────────────────────

TaskContext = dict
SlaContext = tuple  # (dag, task_list, blocking_task_list, slas, blocking_tis)


# ── Factories publiques ───────────────────────────────────────────────────────

def build_failure_callback(
    *,
    notify_email: bool = True,
    notify_slack: bool = False,
    slack_conn_id: str = "slack_data_alerts",
    slack_channel: str = "#data-alerts",
) -> Callable[[TaskContext], None]:
    """Construit un callback d'échec de task avec notifications configurables.

    Args:
        notify_email:  Envoie un email via la config SMTP d'Airflow.
        notify_slack:  Envoie un message Slack via webhook (nécessite slack_conn_id).
        slack_conn_id: Connexion Airflow pour le webhook Slack.
        slack_channel: Canal Slack cible.

    Returns:
        Callable compatible avec on_failure_callback / default_args.
    """
    def _on_failure(context: TaskContext) -> None:
        dag_id   = context["dag"].dag_id
        task_id  = context["task_instance"].task_id
        run_id   = context["run_id"]
        exc      = context.get("exception", "Erreur inconnue")
        exec_dt  = str(context.get("logical_date", ""))

        logger.error(
            "[FAILURE] dag=%s task=%s run_id=%s logical_date=%s error=%s",
            dag_id, task_id, run_id, exec_dt, exc,
        )

        if notify_email:
            _try_send_email(
                subject=f"[AIRFLOW FAILURE] {dag_id} / {task_id}",
                body=(
                    f"<b>DAG :</b> {dag_id}<br>"
                    f"<b>Task :</b> {task_id}<br>"
                    f"<b>Run ID :</b> {run_id}<br>"
                    f"<b>Date :</b> {exec_dt}<br>"
                    f"<b>Erreur :</b> {exc}"
                ),
                context=context,
            )

        if notify_slack:
            _try_send_slack(
                message=(
                    f":red_circle: *[FAILURE]* `{dag_id}` / `{task_id}`\n"
                    f"Run ID : `{run_id}` | Date : `{exec_dt}`\n"
                    f"Erreur : {exc}"
                ),
                conn_id=slack_conn_id,
                channel=slack_channel,
            )

    return _on_failure


def build_retry_callback() -> Callable[[TaskContext], None]:
    """Construit un callback de retry — journalise la tentative sans bruit supplémentaire."""

    def _on_retry(context: TaskContext) -> None:
        dag_id    = context["dag"].dag_id
        task_id   = context["task_instance"].task_id
        try_num   = context["task_instance"].try_number
        exc       = context.get("exception", "")

        logger.warning(
            "[RETRY] dag=%s task=%s tentative=%s error=%s",
            dag_id, task_id, try_num, exc,
        )

    return _on_retry


def build_success_callback(
    *,
    notify_slack: bool = False,
    slack_conn_id: str = "slack_data_alerts",
    slack_channel: str = "#data-alerts",
) -> Callable[[TaskContext], None]:
    """Construit un callback de succès de DAG.

    Args:
        notify_slack:  Envoie un message Slack à la fin réussie du DAG.
        slack_conn_id: Connexion Airflow pour le webhook Slack.
        slack_channel: Canal Slack cible.
    """
    def _on_success(context: TaskContext) -> None:
        dag_id  = context["dag"].dag_id
        run_id  = context["run_id"]
        exec_dt = str(context.get("logical_date", ""))

        logger.info("[SUCCESS] dag=%s run_id=%s logical_date=%s", dag_id, run_id, exec_dt)

        if notify_slack:
            _try_send_slack(
                message=(
                    f":white_check_mark: *[SUCCESS]* `{dag_id}` terminé avec succès\n"
                    f"Run ID : `{run_id}` | Date : `{exec_dt}`"
                ),
                conn_id=slack_conn_id,
                channel=slack_channel,
            )

    return _on_success


def build_sla_miss_callback(
    *,
    notify_email: bool = True,
    notify_slack: bool = True,
    slack_conn_id: str = "slack_data_alerts",
    slack_channel: str = "#data-alerts",
) -> Callable:
    """Construit un callback de dépassement de SLA.

    La signature est différente des callbacks de task : Airflow appelle
    sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis).
    """
    def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
        dag_id = dag.dag_id
        tasks  = ", ".join(str(t) for t in task_list) if task_list else "—"

        logger.warning("[SLA MISS] dag=%s tasks=%s", dag_id, tasks)

        if notify_email:
            _try_send_email_raw(
                subject=f"[AIRFLOW SLA MISS] {dag_id}",
                body=(
                    f"<b>DAG :</b> {dag_id}<br>"
                    f"<b>Tasks en dépassement :</b> {tasks}<br>"
                    f"Le SLA défini pour ce DAG a été dépassé."
                ),
                to=[],  # Airflow envoie aux adresses configurées dans le DAG
            )

        if notify_slack:
            _try_send_slack(
                message=(
                    f":warning: *[SLA MISS]* `{dag_id}`\n"
                    f"Tasks impactées : `{tasks}`"
                ),
                conn_id=slack_conn_id,
                channel=slack_channel,
            )

    return _on_sla_miss


# ── Helpers privés ────────────────────────────────────────────────────────────

def _try_send_email(subject: str, body: str, context: TaskContext) -> None:
    """Envoie un email via l'infrastructure Airflow — échoue silencieusement si SMTP absent."""
    try:
        from airflow.utils.email import send_email_smtp

        task_instance = context.get("task_instance")
        recipients = []
        if task_instance:
            dag_default = context["dag"].default_args or {}
            recipients = dag_default.get("email", [])
            if isinstance(recipients, str):
                recipients = [recipients]

        if recipients:
            send_email_smtp(to=recipients, subject=subject, html_content=body)
    except Exception as exc:
        logger.warning("Envoi email impossible (SMTP non configuré ?) : %s", exc)


def _try_send_email_raw(subject: str, body: str, to: list[str]) -> None:
    try:
        from airflow.utils.email import send_email_smtp
        if to:
            send_email_smtp(to=to, subject=subject, html_content=body)
    except Exception as exc:
        logger.warning("Envoi email impossible : %s", exc)


def _try_send_slack(message: str, conn_id: str, channel: str) -> None:
    """Envoie un message Slack via webhook — échoue silencieusement si non configuré."""
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        hook.send(text=message, channel=channel)
    except Exception as exc:
        logger.warning("Envoi Slack impossible (connexion '%s' absente ?) : %s", conn_id, exc)
