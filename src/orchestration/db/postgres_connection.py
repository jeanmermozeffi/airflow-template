import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from orchestration.common.config import ConnectorType, RelationalSourceConfig
from orchestration.common.env_paths import load_runtime_env, resolve_active_env

logger = logging.getLogger(__name__)


def _load_runtime_environment() -> None:
    """Charge le fichier `.env` si présent pour fiabiliser l'exécution locale."""
    env_name = resolve_active_env(default_env="dev")
    loaded_file = load_runtime_env(default_env=env_name, override=True)
    if loaded_file:
        logger.info("Variables chargées depuis %s", loaded_file)
    else:
        logger.info("Aucun .env chargé, utilisation de l'environnement du processus")


def get_postgres_engine_from_env(prefix: str = "DWH") -> Engine:
    """Crée un moteur SQLAlchemy PostgreSQL depuis un préfixe legacy `<PREFIX>_*`."""
    _load_runtime_environment()

    source = RelationalSourceConfig.from_legacy_prefix(
        prefix=prefix,
        connector=ConnectorType.POSTGRES,
        default_port=5432,
    )
    uri = source.sqlalchemy_uri()
    logger.info("Moteur PostgreSQL construit pour la source '%s'", source.name)
    return create_engine(uri)


def close_engine(engine: Engine) -> None:
    """Ferme proprement le moteur SQLAlchemy."""
    engine.dispose()
