from __future__ import annotations

from typing import Optional

from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from orchestration.common.config import ConnectorType, IntegrationConfig, RelationalSourceConfig


def get_engine(
    *,
    conn_id: Optional[str] = None,
    source_name: Optional[str] = None,
    integration_config: Optional[IntegrationConfig] = None,
) -> Engine:
    """Construit un moteur SQLAlchemy depuis un `conn_id` Airflow ou une source d'environnement."""
    if bool(conn_id) == bool(source_name):
        raise ValueError("Il faut fournir exactement un seul sélecteur: conn_id ou source_name")

    if conn_id:
        return get_engine_from_airflow_conn(conn_id=conn_id)
    return get_engine_from_source_name(source_name=source_name or "", integration_config=integration_config)


def get_engine_from_source_name(
    source_name: str,
    integration_config: Optional[IntegrationConfig] = None,
) -> Engine:
    """Construit un moteur depuis une source déclarée en variables d'environnement."""
    config = integration_config or IntegrationConfig.from_environment()
    source = config.require_database(source_name=source_name)
    return create_engine(source.sqlalchemy_uri())


def get_engine_from_airflow_conn(conn_id: str) -> Engine:
    """Construit un moteur SQLAlchemy depuis une connexion Airflow."""
    conn = BaseHook.get_connection(conn_id)
    uri = build_sqlalchemy_uri_from_airflow_connection(conn)
    return create_engine(uri)


def build_sqlalchemy_uri_from_airflow_connection(conn: Connection) -> str:
    """Convertit une connexion Airflow en URI SQLAlchemy multi-connecteurs."""
    conn_type = (conn.conn_type or "").lower()
    connector = _map_conn_type(conn_type)
    params = conn.extra_dejson or {}

    source = RelationalSourceConfig(
        name=conn.conn_id,
        connector=connector,
        host=conn.host,
        port=conn.port,
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        params={k: str(v) for k, v in params.items()},
    )
    return source.sqlalchemy_uri()


def close_engine(engine: Engine) -> None:
    """Ferme proprement un moteur SQLAlchemy."""
    engine.dispose()


def _map_conn_type(conn_type: str) -> ConnectorType:
    """Mappe les types Airflow vers les types de connecteurs internes."""
    if conn_type in {"postgres", "postgresql"}:
        return ConnectorType.POSTGRES
    if conn_type in {"mysql"}:
        return ConnectorType.MYSQL
    if conn_type in {"mssql", "sqlserver"}:
        return ConnectorType.MSSQL
    if conn_type in {"oracle"}:
        return ConnectorType.ORACLE
    if conn_type in {"sqlite"}:
        return ConnectorType.SQLITE
    raise ValueError(f"Type de connexion SQL non supporté: {conn_type}")
