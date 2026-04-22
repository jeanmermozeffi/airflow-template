import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from orchestration.common.env_paths import resolve_project_root


@dataclass(frozen=True)
class PipelineConfig:
    """Modélise la configuration standard d'un DAG déclaratif."""

    name: str
    dag_id: str
    schedule: str
    owner: str
    retries: int
    retry_delay: timedelta
    tags: list[str]
    required_connections: list[str]


def resolve_pipelines_file() -> Path:
    """Résout le fichier de configuration des pipelines à charger."""
    explicit_path = os.getenv("ORCHESTRATION_PIPELINES_FILE")
    if explicit_path:
        return Path(explicit_path)
    return resolve_project_root() / "config" / "pipelines.yaml"


def _read_yaml(file_path: Path) -> dict[str, Any]:
    """Lit un fichier YAML en conservant une valeur vide sûre."""
    with file_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Le fichier YAML doit contenir un objet racine: {file_path}")
    return data


def load_pipeline_config(pipeline_name: str, file_path: Path | None = None) -> PipelineConfig:
    """Charge la configuration d'un pipeline donné depuis `pipelines.yaml`."""
    source = file_path or resolve_pipelines_file()
    payload = _read_yaml(source)
    pipelines = payload.get("pipelines", {})
    if pipeline_name not in pipelines:
        raise KeyError(f"Pipeline introuvable dans {source}: {pipeline_name}")

    node = pipelines[pipeline_name] or {}
    return PipelineConfig(
        name=pipeline_name,
        dag_id=node.get("dag_id", pipeline_name),
        schedule=node.get("schedule", "@daily"),
        owner=node.get("owner", "data-platform"),
        retries=int(node.get("retries", 1)),
        retry_delay=timedelta(minutes=int(node.get("retry_delay_minutes", 5))),
        tags=list(node.get("tags", [])),
        required_connections=list(node.get("required_connections", [])),
    )

