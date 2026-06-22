from __future__ import annotations

import json
import logging
from pathlib import Path

from airflow import settings
from airflow.models import Connection, Variable

from orchestration.airflow.runtime_env import missing_required_connection_fields, read_yaml_config

log = logging.getLogger(__name__)


def seed_connections(file_path: Path) -> int:
    """Crée ou met à jour les connexions Airflow à partir d'un fichier YAML."""
    payload = read_yaml_config(file_path)
    rows = payload.get("connections", [])
    if not isinstance(rows, list):
        raise ValueError(f"Format invalide pour {file_path}: 'connections' doit être une liste")

    session = settings.Session()
    updated = 0

    try:
        for row in rows:
            conn_id = row["conn_id"]
            missing_fields = missing_required_connection_fields(row)
            if missing_fields:
                log.warning(
                    "Connexion Airflow ignoree pour conn_id=%s: champ(s) requis vide(s): %s. "
                    "Verifier les variables Kubernetes/CI utilisees par config/connections.yaml.",
                    conn_id,
                    ", ".join(missing_fields),
                )
                continue

            existing = session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
            extra = row.get("extra")
            extra_json = json.dumps(extra) if isinstance(extra, dict) else (extra or None)
            port_value = row.get("port")
            port = int(port_value) if port_value not in (None, "") else None

            if existing is None:
                session.add(
                    Connection(
                        conn_id=conn_id,
                        conn_type=row.get("conn_type", "postgres"),
                        host=row.get("host"),
                        schema=row.get("schema"),
                        login=row.get("login"),
                        password=row.get("password"),
                        port=port,
                        extra=extra_json,
                        description=row.get("description"),
                    )
                )
            else:
                existing.conn_type = row.get("conn_type", existing.conn_type)
                existing.host = row.get("host", existing.host)
                existing.schema = row.get("schema", existing.schema)
                existing.login = row.get("login", existing.login)
                existing.password = row.get("password", existing.password)
                existing.port = port
                existing.extra = extra_json if extra_json is not None else existing.extra
                existing.description = row.get("description", existing.description)
            updated += 1

        session.commit()
        return updated
    finally:
        session.close()


def seed_variables(file_path: Path) -> int:
    """Crée ou met à jour les variables Airflow à partir d'un fichier YAML."""
    payload = read_yaml_config(file_path)
    rows = payload.get("variables", [])
    if not isinstance(rows, list):
        raise ValueError(f"Format invalide pour {file_path}: 'variables' doit être une liste")

    for row in rows:
        Variable.set(
            key=row["key"],
            value=row.get("value", ""),
            description=row.get("description"),
            serialize_json=bool(row.get("serialize_json", False)),
        )
    return len(rows)
