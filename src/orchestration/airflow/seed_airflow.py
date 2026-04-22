from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import yaml
from airflow import settings
from airflow.models import Connection, Variable

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(:-([^}]*))?\}")


def _expand_env(value: Any) -> Any:
    """Résout les placeholders `${VAR}` et `${VAR:-default}` dans les YAML."""
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            key = match.group(1)
            default = match.group(3) or ""
            return os.getenv(key, default)

        return _ENV_PATTERN.sub(_replace, value)

    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    return value


def _read_yaml(file_path: Path) -> Any:
    """Lit un YAML de façon sûre en retournant une structure vide compatible."""
    if not file_path.exists():
        return {}
    with file_path.open("r", encoding="utf-8") as handle:
        return _expand_env(yaml.safe_load(handle) or {})


def seed_connections(file_path: Path) -> int:
    """Crée ou met à jour les connexions Airflow à partir d'un fichier YAML."""
    payload = _read_yaml(file_path)
    rows = payload.get("connections", [])
    if not isinstance(rows, list):
        raise ValueError(f"Format invalide pour {file_path}: 'connections' doit être une liste")

    session = settings.Session()
    updated = 0

    try:
        for row in rows:
            conn_id = row["conn_id"]
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
                    )
                )
            else:
                existing.conn_type = row.get("conn_type", existing.conn_type)
                existing.host = row.get("host", existing.host)
                existing.schema = row.get("schema", existing.schema)
                existing.login = row.get("login", existing.login)
                existing.password = row.get("password", existing.password)
                existing.port = port or existing.port
                existing.extra = extra_json if extra_json is not None else existing.extra
            updated += 1

        session.commit()
        return updated
    finally:
        session.close()


def seed_variables(file_path: Path) -> int:
    """Crée ou met à jour les variables Airflow à partir d'un fichier YAML."""
    payload = _read_yaml(file_path)
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
