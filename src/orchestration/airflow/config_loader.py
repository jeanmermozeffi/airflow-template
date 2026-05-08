"""
Chargement de la configuration des pipelines depuis config/pipelines.yaml.

Chaque pipeline déclare :
  - dag_id, schedule, owner, retries, retry_delay_minutes, tags, required_connections
  - description       : description affichée dans l'UI Airflow
  - depends_on_asset  : nom de l'asset Airflow qui déclenche ce DAG (null = cron)
  - produces_asset    : nom de l'asset Airflow publié en fin de run (null = terminal)
  - sql_root          : répertoire SQL relatif à include/sql/ (null = pas de SQL fichier)

Résolution des assets Airflow :
  config.inlet_assets  → list[Asset] pour schedule=[...]  dans le DAG
  config.outlet_assets → list[Asset] pour outlets=[...]   dans l'opérateur de publication
  config.dag_schedule  → valeur directe pour schedule= (Asset list | cron str | None)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from orchestration.common.env_paths import resolve_project_root

if TYPE_CHECKING:
    from airflow.sdk import Asset  # import conditionnel — évite l'erreur hors contexte Airflow


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration déclarative d'un DAG, chargée depuis pipelines.yaml."""

    # ── Identité ──────────────────────────────────────────────────────────────
    name: str
    dag_id: str
    description: str

    # ── Scheduling ────────────────────────────────────────────────────────────
    schedule: str | None

    # ── Opérationnel ──────────────────────────────────────────────────────────
    owner: str
    retries: int
    retry_delay: timedelta
    tags: list[str]
    required_connections: list[str]

    # ── Graphe de dépendances (asset-driven, optionnel) ───────────────────────
    depends_on_asset: str | None  # Nom de l'asset Airflow déclencheur
    produces_asset: str | None    # Nom de l'asset Airflow publié en sortie

    # ── Ressources SQL (optionnel) ────────────────────────────────────────────
    sql_root: str | None          # Sous-répertoire de include/sql/

    # ── Helpers assets (lazy — chargés uniquement si Airflow est disponible) ──

    @property
    def inlet_assets(self) -> list["Asset"]:
        """
        Retourne la liste d'assets pour schedule=[...] du DAG.
        Retourne [] si ce pipeline est sur cron (depends_on_asset=null).
        """
        if not self.depends_on_asset:
            return []
        return [_build_asset(self.depends_on_asset)]

    @property
    def outlet_assets(self) -> list["Asset"]:
        """
        Retourne la liste d'assets pour outlets=[...] de l'opérateur de publication.
        Retourne [] si ce pipeline ne publie rien (terminal).
        """
        if not self.produces_asset:
            return []
        return [_build_asset(self.produces_asset)]

    @property
    def dag_schedule(self) -> Any:
        """
        Valeur à passer directement à schedule= dans la définition du DAG :
          - list[Asset] si asset-driven
          - str cron expression si sur cron
          - None si déclenchement manuel uniquement
        """
        assets = self.inlet_assets
        if assets:
            return assets
        return self.schedule

    @property
    def sql_dir(self) -> Path | None:
        """
        Résout le chemin absolu du répertoire SQL de ce pipeline.
        Retourne None si sql_root est null.
        """
        if not self.sql_root:
            return None
        return resolve_project_root() / "include" / "sql" / self.sql_root


# ── Helpers assets ────────────────────────────────────────────────────────────

def _build_asset(asset_name: str) -> "Asset":
    """
    Construit un objet Airflow Asset depuis un nom.
    Le nom devient l'URI de l'asset (format : urn:airflow:asset:<name>).
    """
    from airflow.sdk import Asset
    return Asset(name=asset_name)


# ── Lecture YAML ──────────────────────────────────────────────────────────────

def resolve_pipelines_file() -> Path:
    """Résout le fichier de configuration des pipelines à charger."""
    explicit_path = os.getenv("ORCHESTRATION_PIPELINES_FILE")
    if explicit_path:
        return Path(explicit_path)
    return resolve_project_root() / "config" / "pipelines.yaml"


def _read_yaml(file_path: Path) -> dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Le fichier YAML doit contenir un objet racine : {file_path}")
    return data


def load_pipeline_config(pipeline_name: str, file_path: Path | None = None) -> PipelineConfig:
    """
    Charge la configuration d'un pipeline depuis pipelines.yaml.

    Args:
        pipeline_name : Clé du pipeline dans la section `pipelines:`.
        file_path     : Chemin explicite (optionnel — utile pour les tests).

    Returns:
        PipelineConfig prêt à l'emploi dans les DAGs.

    Raises:
        KeyError  : pipeline_name absent du YAML.
        ValueError: fichier YAML mal formé.
    """
    source = file_path or resolve_pipelines_file()
    payload = _read_yaml(source)
    pipelines = payload.get("pipelines", {})
    if pipeline_name not in pipelines:
        raise KeyError(f"Pipeline introuvable dans {source} : '{pipeline_name}'")

    node = pipelines[pipeline_name] or {}
    return PipelineConfig(
        name=pipeline_name,
        dag_id=node.get("dag_id", pipeline_name),
        description=node.get("description", ""),
        schedule=node.get("schedule"),
        owner=node.get("owner", "data-platform"),
        retries=int(node.get("retries", 1)),
        retry_delay=timedelta(minutes=int(node.get("retry_delay_minutes", 5))),
        tags=list(node.get("tags", [])),
        required_connections=list(node.get("required_connections", [])),
        depends_on_asset=node.get("depends_on_asset"),
        produces_asset=node.get("produces_asset"),
        sql_root=node.get("sql_root"),
    )
