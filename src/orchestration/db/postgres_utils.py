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
    """Vérifie toutes les connexions SQL requises avant exécution du pipeline.

    Note: Les connexions non-SQL (SMTP, HTTP, etc.) sont ignorées car elles
    ne supportent pas les requêtes SELECT pour la validation.
    """
    from airflow.models import Connection

    missing_or_invalid: list[str] = []
    non_sql_types = {'smtp', 'http', 'https', 'slack', 'webhook', 'generic'}

    for conn_id in conn_ids:
        try:
            # Récupérer le type de la connexion
            conn = Connection.get_connection_from_secrets(conn_id)

            # Ignorer les connexions non-SQL
            if conn and conn.conn_type in non_sql_types:
                logger.info(f"⏭️  Connexion '{conn_id}' ({conn.conn_type}) ignorée (non-SQL)")
                continue

            # Valider les connexions SQL
            check_connection(conn_id=conn_id)
        except Exception as error:  # pragma: no cover
            # Essayer de récupérer la connexion pour voir si c'est non-SQL
            try:
                conn = Connection.get_connection_from_secrets(conn_id)
                if conn and conn.conn_type in non_sql_types:
                    logger.info(f"⏭️  Connexion '{conn_id}' ({conn.conn_type}) ignorée (non-SQL)")
                    continue
            except Exception:
                pass
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

