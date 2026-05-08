"""
Helpers de notification — Pôle Data SYNELIA.

Centralise l'envoi de notifications email et Slack depuis les DAGs et callbacks.
Conçu pour être utilisé directement dans des PythonOperator ou des callbacks.

Exemple dans un DAG :

    from orchestration.airflow.notification import notify_failure, notify_success

    on_fail = PythonOperator(
        task_id="notify_team_on_failure",
        python_callable=notify_failure,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}",
            "task_id": "extract_tasks_from_source",
            "run_id": "{{ run_id }}",
            "ds": "{{ ds }}",
        },
        trigger_rule="one_failed",
    )
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_EMAIL = "data-alerts@synelia.com"
_DEFAULT_SLACK_CONN = "slack_data_alerts"
_DEFAULT_SLACK_CHANNEL = "#data-alerts"


# ── Fonctions de notification publiques ───────────────────────────────────────

def notify_failure(
    dag_id: str,
    task_id: str,
    run_id: str,
    ds: str,
    error_message: str = "",
    email: str | list[str] = _DEFAULT_EMAIL,
    slack_conn_id: Optional[str] = _DEFAULT_SLACK_CONN,
    slack_channel: str = _DEFAULT_SLACK_CHANNEL,
    **_context,
) -> None:
    """Notifie l'équipe d'un échec de pipeline par email et/ou Slack.

    Conçu pour être appelé dans un PythonOperator avec trigger_rule="one_failed".
    """
    subject = f"[AIRFLOW FAILURE] {dag_id} / {task_id}"
    body_html = (
        f"<h3>Pipeline en échec</h3>"
        f"<table>"
        f"<tr><td><b>DAG</b></td><td>{dag_id}</td></tr>"
        f"<tr><td><b>Task</b></td><td>{task_id}</td></tr>"
        f"<tr><td><b>Run ID</b></td><td>{run_id}</td></tr>"
        f"<tr><td><b>Date</b></td><td>{ds}</td></tr>"
        f"<tr><td><b>Erreur</b></td><td>{error_message or '—'}</td></tr>"
        f"</table>"
    )
    slack_msg = (
        f":red_circle: *[FAILURE]* `{dag_id}` / `{task_id}`\n"
        f"Run ID : `{run_id}` | Date : `{ds}`"
        + (f"\nErreur : {error_message}" if error_message else "")
    )

    send_email(subject=subject, body_html=body_html, to=email)
    if slack_conn_id:
        send_slack(message=slack_msg, conn_id=slack_conn_id, channel=slack_channel)


def notify_success(
    dag_id: str,
    run_id: str,
    ds: str,
    record_count: Optional[int] = None,
    slack_conn_id: Optional[str] = None,
    slack_channel: str = _DEFAULT_SLACK_CHANNEL,
    **_context,
) -> None:
    """Journalise (et optionnellement notifie Slack) d'une fin réussie de pipeline."""
    logger.info("[SUCCESS] dag=%s run_id=%s ds=%s records=%s", dag_id, run_id, ds, record_count)

    if slack_conn_id:
        count_info = f" — {record_count:,} enregistrements" if record_count is not None else ""
        send_slack(
            message=f":white_check_mark: *[SUCCESS]* `{dag_id}` terminé{count_info}\nDate : `{ds}`",
            conn_id=slack_conn_id,
            channel=slack_channel,
        )


def notify_sla_miss(
    dag_id: str,
    task_list: str,
    email: str | list[str] = _DEFAULT_EMAIL,
    slack_conn_id: Optional[str] = _DEFAULT_SLACK_CONN,
    slack_channel: str = _DEFAULT_SLACK_CHANNEL,
) -> None:
    """Notifie d'un dépassement de SLA."""
    subject = f"[AIRFLOW SLA MISS] {dag_id}"
    body_html = (
        f"<h3>SLA dépassé</h3>"
        f"<p><b>DAG :</b> {dag_id}</p>"
        f"<p><b>Tasks en dépassement :</b> {task_list}</p>"
        f"<p>Le SLA défini pour ce DAG a été dépassé. Veuillez investiguer.</p>"
    )
    slack_msg = (
        f":warning: *[SLA MISS]* `{dag_id}`\n"
        f"Tasks impactées : `{task_list}`"
    )

    send_email(subject=subject, body_html=body_html, to=email)
    if slack_conn_id:
        send_slack(message=slack_msg, conn_id=slack_conn_id, channel=slack_channel)


# ── Primitives bas niveau ─────────────────────────────────────────────────────

def send_email(
    subject: str,
    body_html: str,
    to: str | list[str],
    cc: Optional[list[str]] = None,
) -> None:
    """Envoie un email via la configuration SMTP d'Airflow.

    Échoue silencieusement si SMTP n'est pas configuré (environnement de dev).
    """
    recipients = [to] if isinstance(to, str) else to
    if not recipients:
        return

    try:
        from airflow.utils.email import send_email_smtp
        send_email_smtp(
            to=recipients,
            subject=subject,
            html_content=body_html,
            cc=cc or [],
        )
        logger.info("Email envoyé à %s : %s", recipients, subject)
    except Exception as exc:
        logger.warning("Envoi email impossible (SMTP non configuré ?) : %s", exc)


def send_slack(
    message: str,
    conn_id: str = _DEFAULT_SLACK_CONN,
    channel: str = _DEFAULT_SLACK_CHANNEL,
) -> None:
    """Envoie un message Slack via webhook Airflow.

    Nécessite la connexion Airflow `slack_data_alerts` (SlackWebhook).
    Échoue silencieusement si la connexion est absente.
    """
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        hook.send(text=message, channel=channel)
        logger.info("Message Slack envoyé sur %s", channel)
    except Exception as exc:
        logger.warning("Envoi Slack impossible (connexion '%s' absente ?) : %s", conn_id, exc)
