#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Ajouter la racine applicative (src/) au sys.path pour permettre `import orchestration`
# en exécution directe (hors contexte Airflow). En Docker le code est à plat dans
# /opt/airflow, donc parent.parent reste cohérent.
_current_file = Path(__file__).resolve()
_app_root = _current_file.parent.parent  # .../src (ou /opt/airflow en conteneur)
if str(_app_root) not in sys.path:
    sys.path.insert(0, str(_app_root))

from orchestration.common.env_paths import load_runtime_env, resolve_project_root
from orchestration.airflow.runtime_env import (
    detect_runtime_environment,
    install_airflow_runtime_env,
)
from orchestration.airflow.seed_airflow import seed_connections, seed_variables


def main() -> None:
    """Initialise les connexions et variables Airflow depuis la configuration du template."""
    env_file = load_runtime_env()
    project_root = resolve_project_root()
    config_dir = project_root / "config"
    runtime = detect_runtime_environment()
    mode = os.getenv("AIRFLOW_BOOTSTRAP_MODE", "metadata").lower()

    installed_count = len(install_airflow_runtime_env(project_root=project_root))
    if env_file:
        print(f"Environnement runtime chargé: {env_file}")
    print(f"Runtime détecté: {runtime} | mode bootstrap: {mode}")
    print(f"Variables runtime Airflow disponibles: {installed_count}")

    if mode == "skip":
        print("Bootstrap ignoré: AIRFLOW_BOOTSTRAP_MODE=skip.")
        return

    if mode == "env" or (mode == "auto" and runtime == "kubernetes"):
        print(
            "Bootstrap metadata ignoré: les connexions/variables sont exposées "
            "via AIRFLOW_CONN_* et AIRFLOW_VAR_*."
        )
        return

    connections_count = seed_connections(config_dir / "connections.yaml")
    variables_count = seed_variables(config_dir / "variables.yaml")

    print(
        f"Bootstrap terminé: {connections_count} connexion(s) et {variables_count} variable(s) synchronisées."
    )


if __name__ == "__main__":
    main()
