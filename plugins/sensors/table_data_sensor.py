"""
TableDataSensor — Attend la disponibilité de données dans une table SQL.

Bloque l'exécution tant que la requête SQL ne retourne pas le nombre
de lignes minimum attendu. Utile pour synchroniser des DAGs dépendants.

Exemples dans un DAG :

    from plugins.sensors.table_data_sensor import TableDataSensor

    # Attend que la partition du jour soit chargée
    wait_for_staging = TableDataSensor(
        task_id="wait_for_staging_data",
        conn_id="dwh_postgres",
        sql="SELECT COUNT(*) FROM staging.stg_tasks WHERE loaded_date = '{{ ds }}'",
        min_rows=1,
        poke_interval=120,
        timeout=3600,
        mode="reschedule",
    )

    # Attend un nombre minimum d'enregistrements
    wait_for_min_volume = TableDataSensor(
        task_id="wait_for_minimum_volume",
        conn_id="dwh_postgres",
        sql="SELECT COUNT(*) FROM staging.stg_tasks WHERE loaded_date = '{{ ds }}'",
        min_rows=100,
        max_rows=None,      # pas de limite supérieure
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
        soft_fail=True,     # marque skipped plutôt qu'échoué si timeout
    )
"""

from __future__ import annotations

from typing import Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator


class TableDataSensor(BaseSensorOperator):
    """Sensor qui attend la disponibilité de données dans une table SQL.

    La requête SQL doit retourner un scalaire entier (COUNT(*) typiquement).
    Le sensor poke tant que le count est inférieur à min_rows.

    Attributes:
        conn_id:   Connexion Airflow (postgres, mysql, mssql…).
        sql:       Requête SQL retournant un entier. Template Jinja supporté.
        min_rows:  Nombre minimum de lignes attendu (défaut 1).
        max_rows:  Nombre maximum de lignes acceptable (None = pas de limite).
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#fff9c4"

    def __init__(
        self,
        *,
        conn_id: str,
        sql: str,
        min_rows: int = 1,
        max_rows: Optional[int] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id  = conn_id
        self.sql      = sql
        self.min_rows = min_rows
        self.max_rows = max_rows

    def poke(self, context: dict) -> bool:
        """Exécute la requête et retourne True si le seuil min_rows est atteint."""
        from orchestration.db.connection_factory import get_engine_from_airflow_conn
        from sqlalchemy import text

        engine = get_engine_from_airflow_conn(self.conn_id)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(self.sql)).scalar()

            count = int(result) if result is not None else 0

            if self.max_rows is not None and count > self.max_rows:
                raise AirflowException(
                    f"TableDataSensor : count={count:,} dépasse max_rows={self.max_rows:,}. "
                    f"SQL : {self.sql[:200]}"
                )

            if count < self.min_rows:
                self.log.info(
                    "TableDataSensor : count=%d < min_rows=%d — on attend…",
                    count, self.min_rows,
                )
                return False

            self.log.info(
                "TableDataSensor : count=%d >= min_rows=%d — condition remplie.",
                count, self.min_rows,
            )
            return True

        finally:
            engine.dispose()
