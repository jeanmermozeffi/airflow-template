import logging
import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

logger = logging.getLogger(__name__)


def resolve_project_root() -> Path:
    """Résout la racine projet depuis l'environnement puis depuis la structure locale."""
    env_root = os.getenv("ORCHESTRATION_PROJECT_ROOT") or os.getenv("AIRFLOW_HOME")
    if env_root:
        return Path(env_root)

    # .../src/orchestration/common/env_paths.py -> repo root
    return Path(__file__).resolve().parents[3]


def resolve_active_env(default_env: str = "dev") -> str:
    """Résout le nom d'environnement actif à partir des variables standard."""
    return (
        os.getenv("AIRFLOW_ENV")
        or os.getenv("ENV")
        or os.getenv("ENVIRONMENT")
        or default_env
    ).lower()


def resolve_env_file(default_env: str = "dev") -> Optional[Path]:
    """Trouve le meilleur fichier `.env` disponible pour le runtime courant."""
    explicit_env_file = os.getenv("ENV_FILE_PATH")
    if explicit_env_file:
        explicit_path = Path(explicit_env_file)
        if explicit_path.exists():
            return explicit_path

    env_name = resolve_active_env(default_env=default_env)
    project_root = resolve_project_root()
    candidates = [
        project_root / f".env.{env_name}",
        project_root / ".env",
        Path("/opt/airflow") / f".env.{env_name}",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def load_runtime_env(default_env: str = "dev", override: bool = True, strict: bool = False) -> Optional[str]:
    """Charge le `.env` runtime si disponible, sinon garde l'environnement du processus."""
    if load_dotenv is None:
        if strict:
            raise ImportError("python-dotenv is not installed")
        return None

    env_file = resolve_env_file(default_env=default_env)
    if env_file:
        load_dotenv(env_file, override=override)
        return str(env_file)

    if strict:
        env_name = resolve_active_env(default_env=default_env)
        raise FileNotFoundError(f"No environment file found for env={env_name}")

    logger.debug("Aucun fichier .env trouvé, utilisation de l'environnement déjà injecté")
    return None

