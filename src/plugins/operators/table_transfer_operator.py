"""
TableTransferOperator — Transfert de données entre deux bases de données.

Copie les résultats d'une requête SQL source vers une table cible
sur une connexion différente (ou la même). Supporte PostgreSQL, MySQL, MSSQL.

Cas d'usage :
  - Copier une table depuis un ERP (MySQL) vers la Landing Zone (PostgreSQL)
  - Synchroniser une table de référence entre deux environnements
  - Extraire des données d'un OLTP vers un staging Airflow

Exemple dans un DAG :

    from plugins.operators.table_transfer_operator import TableTransferOperator

    transfer = TableTransferOperator(
        task_id="copy_tasks_to_landing",
        src_conn_id="crm_mysql",
        dst_conn_id="dwh_postgres",
        src_sql="SELECT id, name, status, updated_at FROM tasks WHERE updated_at > '{{ ds }}'",
        dst_table="landing.raw_crm_tasks",
        dst_key_columns=["id"],           # None = INSERT simple, liste = upsert
        batch_size=5_000,
        pre_execute_sql="DELETE FROM landing.raw_crm_tasks WHERE loaded_date = '{{ ds }}'",
    )
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.sdk.bases.operator import BaseOperator

log = logging.getLogger(__name__)


class TableTransferOperator(BaseOperator):
    """Transfère des données entre deux connexions SQL via SQLAlchemy.

    Attributes:
        src_conn_id:     Connexion Airflow source.
        dst_conn_id:     Connexion Airflow destination.
        src_sql:         Requête SQL à exécuter sur la source (template Jinja supporté).
        dst_table:       Table de destination (ex. "staging.stg_tasks").
        dst_key_columns: Colonnes de clé pour l'upsert. None = INSERT simple.
        dst_update_cols: Colonnes à mettre à jour en cas de conflit (None = toutes).
        batch_size:      Taille des lots d'insertion (défaut 5 000).
        pre_execute_sql: SQL à exécuter sur la destination avant le transfert
                         (ex. DELETE de la partition courante). Template Jinja supporté.
    """

    template_fields: Sequence[str] = ("src_sql", "pre_execute_sql")
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#e8f5e9"

    def __init__(
        self,
        *,
        src_conn_id: str,
        dst_conn_id: str,
        src_sql: str,
        dst_table: str,
        dst_key_columns: Optional[list[str]] = None,
        dst_update_columns: Optional[list[str]] = None,
        batch_size: int = 5_000,
        pre_execute_sql: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.src_conn_id        = src_conn_id
        self.dst_conn_id        = dst_conn_id
        self.src_sql            = src_sql
        self.dst_table          = dst_table
        self.dst_key_columns    = dst_key_columns
        self.dst_update_columns = dst_update_columns
        self.batch_size         = batch_size
        self.pre_execute_sql    = pre_execute_sql

    def execute(self, context: dict) -> int:
        from orchestration.db.batch_loader import insert_records, upsert_records
        from orchestration.db.connection_factory import get_engine_from_airflow_conn

        src_engine = get_engine_from_airflow_conn(self.src_conn_id)
        dst_engine = get_engine_from_airflow_conn(self.dst_conn_id)

        try:
            records = self._extract(src_engine)
            if not records:
                self.log.info("Aucun enregistrement à transférer depuis %s", self.src_conn_id)
                return 0

            if self.pre_execute_sql:
                self._run_pre_execute(dst_engine)

            if self.dst_key_columns:
                loaded = upsert_records(
                    engine=dst_engine,
                    table=self.dst_table,
                    records=records,
                    key_columns=self.dst_key_columns,
                    update_columns=self.dst_update_columns,
                    batch_size=self.batch_size,
                )
            else:
                loaded = insert_records(
                    engine=dst_engine,
                    table=self.dst_table,
                    records=records,
                    batch_size=self.batch_size,
                )

            self.log.info(
                "Transfert terminé : %d enregistrement(s) — %s → %s",
                loaded, self.src_conn_id, self.dst_table,
            )
            return loaded

        except Exception as exc:
            raise AirflowException(
                f"TableTransferOperator : échec du transfert {self.src_conn_id} → {self.dst_table} : {exc}"
            ) from exc
        finally:
            src_engine.dispose()
            dst_engine.dispose()

    def _extract(self, engine) -> list[dict]:
        from sqlalchemy import text

        self.log.info("Extraction depuis %s : %s", self.src_conn_id, self.src_sql[:120])
        with engine.connect() as conn:
            result = conn.execute(text(self.src_sql))
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]

        self.log.info("Extraction terminée : %d lignes", len(rows))
        return rows

    def _run_pre_execute(self, engine) -> None:
        from sqlalchemy import text

        self.log.info("Exécution du pre_execute_sql : %s", self.pre_execute_sql)
        with engine.begin() as conn:
            conn.execute(text(self.pre_execute_sql))
