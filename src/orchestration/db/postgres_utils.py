from __future__ import annotations

import logging
from typing import Iterable, Optional

from sqlalchemy import text

from orchestration.db.connection_factory import get_engine_from_airflow_conn

logger = logging.getLogger(__name__)


def check_connection(conn_id: str, test_query: str = "SELECT 1") -> bool:
    """Valide une connexion SQL Airflow (Postgres, MySQL, MSSQL, Oracle, SQLite)."""
    engine = get_engine_from_airflow_conn(conn_id=conn_id)
    try:
        with engine.connect() as connection:
            connection.execute(text(test_query))
        return True
    finally:
        engine.dispose()


def check_postgres_connection(conn_id: str, test_query: str = "SELECT 1") -> bool:
    """Alias de compatibilité historique (désormais multi-connecteurs)."""
    return check_connection(conn_id=conn_id, test_query=test_query)


def validate_required_connections(conn_ids: Iterable[str]) -> None:
    """Vérifie toutes les connexions SQL requises avant exécution du pipeline."""
    missing_or_invalid: list[str] = []
    for conn_id in conn_ids:
        try:
            check_connection(conn_id=conn_id)
        except Exception as error:  # pragma: no cover
            logger.error("Connexion invalide: %s (%s)", conn_id, error)
            missing_or_invalid.append(conn_id)

    if missing_or_invalid:
        raise RuntimeError(
            "Les connexions suivantes ne sont pas valides: "
            + ", ".join(sorted(missing_or_invalid))
        )


def fetch_one_value(conn_id: str, sql_query: str, params: Optional[dict] = None) -> Optional[object]:
    """Exécute une requête SQL et retourne la première valeur du premier enregistrement."""
    engine = get_engine_from_airflow_conn(conn_id=conn_id)
    try:
        with engine.connect() as connection:
            row = connection.execute(text(sql_query), params or {}).first()
            return None if row is None else row[0]
    finally:
        engine.dispose()

