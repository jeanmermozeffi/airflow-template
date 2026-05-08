"""
DAG: data_cleaning
Nettoyage des données staging après ingestion source → PostgreSQL.

Position dans la chaîne ETL:
  ingestion → [RAW_READY] → data_cleaning → [CLEAN_READY] → transformation

Déclenchement : automatique via asset RAW_READY (publié par ingestion_dag)
Publication    : asset CLEAN_READY après succès complet

Architecture dynamique (zero hardcoding):
  - operations.yaml → liste des opérations activées, groupées par table
  - operations.sql  → requêtes SQL, une par bloc -- @nom_operation
  - Tables différentes   → tâches parallèles (gain de temps)
  - Opérations sur la même table → séquentielles (ordre YAML préservé)

Pour ajouter une opération :
  1. Ajouter le bloc SQL dans include/sql/data_cleaning/operations.sql
     (marqueur -- @nom_operation)
  2. Ajouter l'entrée dans include/sql/data_cleaning/operations.yaml
  3. Relancer le scheduler Airflow (rechargement automatique du DAG)
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from dags._bootstrap import bootstrap_project_paths

bootstrap_project_paths()

from orchestration.airflow.config_loader import load_pipeline_config

log = logging.getLogger(__name__)
config = load_pipeline_config("data_cleaning")

# ── Chargement des ressources au parse-time du DAG ────────────────────────────

_DATA_CLEANING_DIR = config.sql_dir   # résolu via pipelines.yaml → include/sql/data_cleaning
_YAML_PATH = _DATA_CLEANING_DIR / "operations.yaml"
_SQL_PATH  = _DATA_CLEANING_DIR / "operations.sql"


def _load_sql_blocks(sql_path: Path) -> dict[str, str]:
    """Parse operations.sql → dict {op_name: sql_text} via marqueurs -- @op_name."""
    blocks: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []

    for line in sql_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("-- @"):
            if current_name and current_lines:
                sql = "\n".join(current_lines).strip()
                if sql:
                    blocks[current_name] = sql
            current_name = stripped[4:].strip()
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)

    if current_name and current_lines:
        sql = "\n".join(current_lines).strip()
        if sql:
            blocks[current_name] = sql

    return blocks


def _load_operations(yaml_path: Path) -> dict[str, dict]:
    """Charge operations.yaml → {op_name: {table, description, enabled}}."""
    with yaml_path.open(encoding="utf-8") as f:
        return (yaml.safe_load(f) or {}).get("operations", {})


_SQL_BLOCKS = _load_sql_blocks(_SQL_PATH)
_OPERATIONS = _load_operations(_YAML_PATH)


# ── Callable exécuté par chaque PythonOperator ────────────────────────────────

def _run_sql_operation(operation_name: str, table: str) -> None:
    """Exécute le bloc SQL de l'opération via PostgresHook et log les lignes affectées."""
    sql = _SQL_BLOCKS.get(operation_name)
    if not sql:
        log.warning("Aucun SQL trouvé pour '%s' — opération ignorée.", operation_name)
        return

    hook = PostgresHook(postgres_conn_id=config.required_connections[0])
    log.info("▶ %s [%s]", operation_name, table)

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            affected = cur.rowcount
        conn.commit()
        log.info("  ✓ %s : %d ligne(s) affectée(s)", operation_name, affected)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Construction du DAG ───────────────────────────────────────────────────────

# Grouper les opérations activées par table (ordre du YAML préservé)
_groups: dict[str, list[str]] = defaultdict(list)
for _op_name, _op_cfg in _OPERATIONS.items():
    if _op_cfg.get("enabled", True) and _op_name in _SQL_BLOCKS:
        _groups[_op_cfg["table"]].append(_op_name)

with DAG(
    dag_id=config.dag_id,
    schedule=config.dag_schedule,          # [RAW_READY] via pipelines.yaml
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

    publish_clean_ready = EmptyOperator(
        task_id="publish_clean_ready",
        outlets=config.outlet_assets,      # [CLEAN_READY] via pipelines.yaml
    )

    end = EmptyOperator(task_id="end")

    # Construire les tâches dynamiquement : une chaîne séquentielle par table
    table_last_tasks: list = []

    for table, op_names in _groups.items():
        prev = start

        for op_name in op_names:
            op_cfg = _OPERATIONS[op_name]
            task = PythonOperator(
                task_id=op_name,
                python_callable=_run_sql_operation,
                op_kwargs={"operation_name": op_name, "table": table},
                doc=op_cfg.get("description", ""),
            )
            prev >> task
            prev = task

        table_last_tasks.append(prev)

    if table_last_tasks:
        table_last_tasks >> publish_clean_ready
    else:
        start >> publish_clean_ready

    publish_clean_ready >> end
