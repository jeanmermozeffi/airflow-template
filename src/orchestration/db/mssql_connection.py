from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from orchestration.common.config import ConnectorType, RelationalSourceConfig
from orchestration.common.env_paths import load_runtime_env, resolve_active_env


def get_mssql_engine_from_env(prefix: str = "MSSQL") -> Engine:
    """Crée un moteur SQLAlchemy SQL Server depuis un préfixe legacy `<PREFIX>_*`."""
    active_env = resolve_active_env(default_env="dev")
    load_runtime_env(default_env=active_env, override=True)

    source = RelationalSourceConfig.from_legacy_prefix(
        prefix=prefix,
        connector=ConnectorType.MSSQL,
        default_port=1433,
    )
    return create_engine(source.sqlalchemy_uri())

