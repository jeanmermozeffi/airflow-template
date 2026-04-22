from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from orchestration.common.config import ConnectorType, RelationalSourceConfig
from orchestration.common.env_paths import load_runtime_env, resolve_active_env


def get_mysql_engine_from_env(prefix: str = "MYSQL") -> Engine:
    """Crée un moteur SQLAlchemy MySQL depuis un préfixe legacy `<PREFIX>_*`."""
    active_env = resolve_active_env(default_env="dev")
    load_runtime_env(default_env=active_env, override=True)

    source = RelationalSourceConfig.from_legacy_prefix(
        prefix=prefix,
        connector=ConnectorType.MYSQL,
        default_port=3306,
    )
    return create_engine(source.sqlalchemy_uri())

