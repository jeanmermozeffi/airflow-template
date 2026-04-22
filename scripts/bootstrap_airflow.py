#!/usr/bin/env python3
from pathlib import Path

from orchestration.common.env_paths import resolve_project_root
from orchestration.airflow.seed_airflow import seed_connections, seed_variables


def main() -> None:
    """Initialise les connexions et variables Airflow depuis la configuration du template."""
    project_root = resolve_project_root()
    config_dir = project_root / "config"

    connections_count = seed_connections(config_dir / "connections.yaml")
    variables_count = seed_variables(config_dir / "variables.yaml")

    print(
        f"Bootstrap terminé: {connections_count} connexion(s) et {variables_count} variable(s) synchronisées."
    )


if __name__ == "__main__":
    main()

