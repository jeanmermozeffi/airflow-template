from __future__ import annotations

from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class SqlQualityOperator(BaseOperator):
    """Exécute une requête de contrôle qualité et vérifie un seuil minimal de lignes."""

    template_fields: Sequence[str] = ("sql",)

    def __init__(
        self,
        *,
        postgres_conn_id: str,
        sql: str,
        min_rows: int = 1,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.min_rows = min_rows

    def execute(self, context: dict) -> None:
        """Lance la requête et échoue si le résultat est inférieur au seuil attendu."""
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        result = hook.get_first(self.sql)
        if result is None:
            raise AirflowException("La requête de qualité n'a retourné aucun résultat")

        row_count = int(result[0])
        if row_count < self.min_rows:
            raise AirflowException(
                f"Seuil de qualité non atteint: {row_count} < {self.min_rows}"
            )

        self.log.info("Contrôle qualité validé: %s ligne(s) (seuil=%s)", row_count, self.min_rows)

