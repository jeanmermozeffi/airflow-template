import os
import sys
from pathlib import Path


def bootstrap_project_paths() -> None:
    """
    Injecte les chemins du projet dans sys.path pour les imports DAG.
    Utilise ORCHESTRATION_PROJECT_ROOT si défini (Docker/K8s), sinon calcule relativement.
    Charge aussi le .env et installe les variables runtime Airflow (AIRFLOW_CONN_*/VAR_*).
    """
    # Charger le .env en premier (best-effort si orchestration est déjà importable)
    try:
        from orchestration.common.env_paths import load_runtime_env
        load_runtime_env()
    except ImportError:
        pass

    project_root_env = os.getenv("ORCHESTRATION_PROJECT_ROOT")

    if project_root_env:
        project_root = Path(project_root_env)
    else:
        # Calcul relatif : de src/dags/_bootstrap.py vers la racine applicative (src/)
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[1]

    src_dir = project_root / "src"

    root_str = str(project_root)
    src_str = str(src_dir)

    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

    os.environ.setdefault("ORCHESTRATION_PROJECT_ROOT", root_str)

    # Installe les connexions/variables déclaratives en variables d'environnement Airflow
    try:
        from orchestration.airflow.runtime_env import install_airflow_runtime_env

        install_airflow_runtime_env(project_root=project_root)
    except ImportError:
        pass
