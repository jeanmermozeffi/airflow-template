"""
DAG: data_quality
Contrôle qualité post-transformation du DWH.

Position dans la chaîne ETL:
  transformation → [DWH_READY] → data_quality (terminal)

Architecture:
  - checks.yaml → définition des contrôles (type, seuil, sévérité, couche)
  - checks.sql  → SQL retournant un entier par contrôle (marqueur -- @nom)
  - Couches exécutées en séquence : dimensions → facts → marts
  - Au sein d'une couche, les checks sont parallèles
  - Résultats stockés en XCom → tâche finale génère le rapport

Flux:
  start
  ├── [dimensions] check_dim_* (parallèle)
  │     └── [facts]  check_fact_* (parallèle)
  │           └── [marts]  check_mart_* (parallèle)
  │                 └── dq_report_and_branch
  │                       ├── [CRITICAL KO] → send_critical_alert → end
  │                       └── [OK / WARN]   → checks_passed → end

Types de checks :
  min_count  → résultat doit être >= threshold  (ex: table non vide)
  max_count  → résultat doit être <= threshold  (ex: violations = 0)

Sévérités :
  critical → bloque, déclenche une alerte email
  warning  → inclus dans le rapport, non bloquant
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.email import send_email

from dags._bootstrap import bootstrap_project_paths

bootstrap_project_paths()

from orchestration.airflow.config_loader import load_pipeline_config

log = logging.getLogger(__name__)
config = load_pipeline_config("data_quality")

# ── Chargement des ressources au parse-time ───────────────────────────────────

_DQ_DIR    = config.sql_dir   # résolu via pipelines.yaml → include/sql/data_quality
_YAML_PATH = _DQ_DIR / "checks.yaml"
_SQL_PATH  = _DQ_DIR / "checks.sql"


def _load_sql_blocks(path: Path) -> dict[str, str]:
    blocks: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s.startswith("-- @"):
            if current_name and current_lines:
                sql = "\n".join(current_lines).strip()
                if sql:
                    blocks[current_name] = sql
            current_name = s[4:].strip()
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)
    if current_name and current_lines:
        sql = "\n".join(current_lines).strip()
        if sql:
            blocks[current_name] = sql
    return blocks


def _load_checks(path: Path) -> dict[str, dict]:
    with path.open(encoding="utf-8") as f:
        return (yaml.safe_load(f) or {}).get("checks", {})


_SQL_BLOCKS = _load_sql_blocks(_SQL_PATH)
_CHECKS     = _load_checks(_YAML_PATH)

# Grouper les checks par couche (ordre d'exécution : dimensions → facts → marts)
_LAYER_ORDER = ["dimensions", "facts", "marts"]
_GROUPS: dict[str, list[str]] = defaultdict(list)
for _check_name, _chk in _CHECKS.items():
    if _check_name in _SQL_BLOCKS:
        _GROUPS[_chk["layer"]].append(_check_name)


# ── Callable : exécute un check et pousse le résultat en XCom ─────────────────

def _run_check(check_name: str, **context: Any) -> dict[str, Any]:
    chk      = _CHECKS[check_name]
    sql      = _SQL_BLOCKS[check_name]
    kind     = chk["type"]         # min_count | max_count
    thresh   = int(chk["threshold"])
    severity = chk["severity"]     # critical | warning

    hook   = PostgresHook(postgres_conn_id=config.required_connections[0])
    result = hook.get_first(sql)
    value  = int(result[0]) if result else 0

    passed = (value >= thresh) if kind == "min_count" else (value <= thresh)
    status = "OK" if passed else ("CRITICAL" if severity == "critical" else "WARNING")

    record: dict[str, Any] = {
        "check":       check_name,
        "description": chk["description"],
        "layer":       chk["layer"],
        "severity":    severity,
        "type":        kind,
        "threshold":   thresh,
        "value":       value,
        "status":      status,
        "passed":      passed,
    }

    icon = "OK" if passed else ("CRITICAL" if severity == "critical" else "WARNING")
    log.info(
        "[%s] %s → %d (seuil %s %d)",
        icon, check_name, value,
        ">=" if kind == "min_count" else "<=", thresh,
    )

    context["ti"].xcom_push(key=f"dq_{check_name}", value=record)
    return record


# ── Callable : rapport final + décision de branchement ───────────────────────

_ALL_CHECK_NAMES = list(_CHECKS.keys())


def _dq_report_and_branch(**context: Any) -> str:
    """Lit tous les XComs, génère le rapport, retourne l'id de la prochaine tâche."""
    ti = context["ti"]

    results: list[dict] = []
    for check_name in _ALL_CHECK_NAMES:
        rec = ti.xcom_pull(task_ids=check_name, key=f"dq_{check_name}")
        if rec:
            results.append(rec)

    failed_critical = [r for r in results if not r["passed"] and r["severity"] == "critical"]
    failed_warning  = [r for r in results if not r["passed"] and r["severity"] == "warning"]
    passed          = [r for r in results if r["passed"]]

    log.info(
        "DQ Report → %d OK | %d WARNING | %d CRITICAL",
        len(passed), len(failed_warning), len(failed_critical),
    )

    _send_dq_report(results, failed_critical, failed_warning)

    return "send_critical_alert" if failed_critical else "checks_passed"


def _send_dq_report(
    results: list[dict],
    failed_critical: list[dict],
    failed_warning: list[dict],
) -> None:
    """Envoie le rapport DQ par email via la connexion smtp_default d'Airflow."""
    try:
        from airflow.sdk import Variable
        recipient = Variable.get("DQ_REPORT_EMAIL", default="")
    except Exception:
        recipient = ""

    if not recipient:
        log.info("DQ_REPORT_EMAIL non configuré — rapport email ignoré.")
        return

    total = len(results)
    n_ok   = sum(1 for r in results if r["passed"])
    n_warn = len(failed_warning)
    n_crit = len(failed_critical)

    if failed_critical:
        subject = f"[DQ CRITICAL] {n_crit} contrôle(s) critique(s) échoué(s)"
    elif failed_warning:
        subject = f"[DQ WARNING] {n_warn} avertissement(s) détecté(s)"
    else:
        subject = f"[DQ OK] Tous les contrôles sont OK ({total}/{total})"

    rows_html = ""
    for r in sorted(results, key=lambda x: (x["passed"], x["severity"] != "critical")):
        icon  = "OK" if r["passed"] else ("CRITICAL" if r["severity"] == "critical" else "WARNING")
        style = "" if r["passed"] else (
            "background:#fff3e0" if r["severity"] == "warning" else "background:#ffebee"
        )
        rows_html += (
            f"<tr style='{style}'>"
            f"<td><b>{icon}</b></td>"
            f"<td><b>{r['check']}</b><br><small>{r['description']}</small></td>"
            f"<td>{r['layer']}</td>"
            f"<td>{r['severity'].upper()}</td>"
            f"<td>{r['value']}</td>"
            f"<td>{'≥' if r['type'] == 'min_count' else '≤'} {r['threshold']}</td>"
            f"<td><b>{r['status']}</b></td>"
            f"</tr>"
        )

    html = f"""
    <html><body style="font-family:Arial,sans-serif;margin:20px">
    <h2>{subject}</h2>
    <p>Résumé : <b style="color:#388E3C">{n_ok} OK</b>
               | <b style="color:#F57C00">{n_warn} WARNING</b>
               | <b style="color:#D32F2F">{n_crit} CRITICAL</b>
               (sur {total} contrôles)</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%">
      <thead style="background:#E3F2FD">
        <tr>
          <th></th><th>Contrôle</th><th>Couche</th>
          <th>Sévérité</th><th>Valeur</th><th>Seuil</th><th>Statut</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    </body></html>
    """

    try:
        send_email(to=recipient, subject=subject, html_content=html)
        log.info("Rapport DQ envoyé à %s", recipient)
    except Exception as exc:
        log.warning("Envoi rapport DQ échoué : %s", exc)


def _send_critical_alert(**context: Any) -> None:
    """Tâche appelée uniquement si des checks critiques ont échoué."""
    log.error(
        "Des contrôles critiques ont échoué. DWH_READY NON publié. "
        "Vérifiez les logs et la notification envoyée."
    )


# ── Construction du DAG ───────────────────────────────────────────────────────

with DAG(
    dag_id=config.dag_id,
    schedule=config.dag_schedule,          # [DWH_READY] via pipelines.yaml
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description=config.description,
    tags=config.tags,
    max_active_runs=1,
    default_args={
        "owner":       config.owner,
        "retries":     config.retries,
        "retry_delay": config.retry_delay,
    },
) as dag:

    start = EmptyOperator(task_id="start")

    checks_passed = EmptyOperator(task_id="checks_passed")

    send_critical_alert = PythonOperator(
        task_id="send_critical_alert",
        python_callable=_send_critical_alert,
    )

    # trigger_rule pour accepter n'importe laquelle des deux branches
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    branch = BranchPythonOperator(
        task_id="dq_report_and_branch",
        python_callable=_dq_report_and_branch,
    )

    # ── Construire les tâches de check par couche (séquence inter-couches) ──
    prev_layer_end: list = [start]

    for layer in _LAYER_ORDER:
        check_names = _GROUPS.get(layer, [])
        if not check_names:
            continue

        layer_tasks = []
        for check_name in check_names:
            chk = _CHECKS[check_name]
            t = PythonOperator(
                task_id=check_name,
                python_callable=_run_check,
                op_kwargs={"check_name": check_name},
                doc=chk.get("description", ""),
            )
            for prev in prev_layer_end:
                prev >> t
            layer_tasks.append(t)

        prev_layer_end = layer_tasks

    for last_task in prev_layer_end:
        last_task >> branch

    branch >> checks_passed >> end
    branch >> send_critical_alert >> end
