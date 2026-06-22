from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(:-([^}]*))?\}")
_SQL_CONN_TYPES = {"mysql", "postgres", "postgresql", "mssql", "sqlserver", "oracle"}


def expand_env(value: Any) -> Any:
    """Resout les placeholders `${VAR}` et `${VAR:-default}` dans les YAML."""
    if isinstance(value, str):

        def _replace(match: re.Match[str]) -> str:
            key = match.group(1)
            default = match.group(3) or ""
            return os.getenv(key, default)

        return _ENV_PATTERN.sub(_replace, value)

    if isinstance(value, list):
        return [expand_env(item) for item in value]
    if isinstance(value, dict):
        return {key: expand_env(item) for key, item in value.items()}
    return value


def read_yaml_config(file_path: Path) -> Any:
    """Lit un YAML puis applique l'expansion des variables d'environnement."""
    if not file_path.exists():
        return {}
    with file_path.open("r", encoding="utf-8") as handle:
        return expand_env(yaml.safe_load(handle) or {})


def detect_runtime_environment() -> str:
    """Retourne `kubernetes`, `docker` ou `local` selon le runtime courant."""
    if os.getenv("KUBERNETES_SERVICE_HOST") or Path(
        "/var/run/secrets/kubernetes.io/serviceaccount"
    ).exists():
        return "kubernetes"
    if os.getenv("COMPOSE_PROJECT_NAME") or Path("/.dockerenv").exists():
        return "docker"
    return "local"


def build_airflow_runtime_env(
    *,
    project_root: Path | None = None,
    config_dir: Path | None = None,
) -> dict[str, str]:
    """Construit les `AIRFLOW_CONN_*` et `AIRFLOW_VAR_*` depuis les YAML projet."""
    if config_dir is None:
        if project_root is None:
            from orchestration.common.env_paths import resolve_project_root

            project_root = resolve_project_root()
        config_dir = project_root / "config"

    runtime_env: dict[str, str] = {}
    runtime_env.update(_connection_env(config_dir / "connections.yaml"))
    runtime_env.update(_variable_env(config_dir / "variables.yaml"))
    return runtime_env


def install_airflow_runtime_env(
    *,
    project_root: Path | None = None,
    config_dir: Path | None = None,
    overwrite: bool = False,
) -> dict[str, str]:
    """Installe les variables runtime dans `os.environ` pour Airflow Secrets."""
    runtime_env = build_airflow_runtime_env(project_root=project_root, config_dir=config_dir)
    installed: dict[str, str] = {}

    for key, value in runtime_env.items():
        if overwrite or key not in os.environ:
            os.environ[key] = value
            installed[key] = value

    return installed


def _connection_env(file_path: Path) -> dict[str, str]:
    payload = read_yaml_config(file_path)
    rows = payload.get("connections", [])
    if not isinstance(rows, list):
        raise ValueError(f"Format invalide pour {file_path}: 'connections' doit etre une liste")

    env: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        conn_id = row.get("conn_id")
        if not conn_id or not connection_is_usable(row):
            continue

        env[_airflow_env_key("AIRFLOW_CONN", str(conn_id))] = build_connection_uri(row)

    return env


def _variable_env(file_path: Path) -> dict[str, str]:
    payload = read_yaml_config(file_path)
    rows = payload.get("variables", [])
    if not isinstance(rows, list):
        raise ValueError(f"Format invalide pour {file_path}: 'variables' doit etre une liste")

    env: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = row.get("key")
        if not key:
            continue
        value = row.get("value", "")
        if row.get("serialize_json", False):
            rendered = json.dumps(value)
        elif isinstance(value, str):
            rendered = value
        else:
            rendered = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        env[_airflow_env_key("AIRFLOW_VAR", str(key))] = rendered

    return env


def build_connection_uri(row: dict[str, Any]) -> str:
    """Convertit une ligne `connections.yaml` en URI Airflow."""
    conn_type = str(row.get("conn_type") or "postgres")
    login = str(row.get("login") or "")
    password = str(row.get("password") or "")
    host = str(row.get("host") or "")
    schema = str(row.get("schema") or "")
    port = row.get("port")
    extra = row.get("extra")

    credentials = ""
    if login or password:
        credentials = quote(login, safe="")
        if password:
            credentials += f":{quote(password, safe='')}"
        credentials += "@"

    netloc = f"{credentials}{host}"
    if port not in (None, ""):
        netloc += f":{port}"

    path = f"/{quote(schema, safe='')}" if schema else ""
    query = ""
    if isinstance(extra, dict) and extra:
        query = "?" + urlencode({key: _stringify_extra(value) for key, value in extra.items()})
    elif isinstance(extra, str) and extra:
        query = f"?{extra}"

    return f"{conn_type}://{netloc}{path}{query}"


def missing_required_connection_fields(row: dict[str, Any]) -> list[str]:
    """Retourne les champs requis manquants pour une connexion Airflow."""
    conn_type = str(row.get("conn_type") or "").lower()
    if conn_type in _SQL_CONN_TYPES:
        return [field for field in ("host", "schema", "login") if not row.get(field)]
    if conn_type == "sqlite":
        return ["schema"] if not row.get("schema") else []
    if not (row.get("host") or row.get("login") or row.get("extra")):
        return ["host/login/extra"]
    return []


def connection_is_usable(row: dict[str, Any]) -> bool:
    return not missing_required_connection_fields(row)


def _airflow_env_key(prefix: str, key: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", key).strip("_").upper()
    return f"{prefix}_{normalized}"


def _stringify_extra(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)
