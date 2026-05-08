"""
SQL Query Loader — Module partagé pour charger les requêtes SQL depuis des fichiers.

Utilisé par les modules d'extraction et de transformation.
Les requêtes sont stockées dans include/sql/<project>/ organisées par type.

Exemple:
  from orchestration.sql_loader import load_query, load_extraction_config
  sql = load_query("extract/customers.sql")
  config = load_extraction_config("extract/tables.yaml")
"""

import logging
import os
from pathlib import Path

import yaml

from orchestration.common.env_paths import resolve_project_root

log = logging.getLogger(__name__)

# Répertoire SQL racine — surchargeable via ORCHESTRATION_SQL_ROOT (chemin absolu)
# ou ORCHESTRATION_SQL_SUBDIR (sous-répertoire relatif à include/sql/)
def _resolve_sql_root() -> Path:
    explicit = os.getenv("ORCHESTRATION_SQL_ROOT")
    if explicit:
        return Path(explicit)
    subdir = os.getenv("ORCHESTRATION_SQL_SUBDIR", "")
    base = resolve_project_root() / "include" / "sql"
    return base / subdir if subdir else base

SQL_ROOT = _resolve_sql_root()


def load_query(relative_path: str, from_dir: str | None = None) -> str:
    """
    Charge une requête SQL depuis un fichier.

    Args:
        relative_path: Chemin relatif depuis SQL_ROOT
                      Ex: "extract/customers.sql" ou "customers.sql" (si from_dir="extract")
        from_dir: Répertoire optionnel à préfixer (Ex: "extract", "dimensions", "facts")

    Returns:
        Contenu du fichier SQL en string

    Raises:
        FileNotFoundError: Si le fichier n'existe pas
    """
    full_path = f"{from_dir}/{relative_path}" if from_dir else relative_path
    path = SQL_ROOT / full_path

    if not path.exists():
        raise FileNotFoundError(
            f"Fichier SQL introuvable : {path}\n"
            f"SQL_ROOT configuré : {SQL_ROOT}"
        )

    content = path.read_text(encoding="utf-8")
    log.debug("Requête chargée : %s (%d chars)", full_path, len(content))
    return content


def load_query_template(relative_path: str, **kwargs: object) -> str:
    """
    Charge une requête SQL et remplace les placeholders {param}.

    Utile pour les requêtes paramétrées (cutoff_date, limit, etc.)

    Args:
        relative_path: Chemin relatif depuis SQL_ROOT
        **kwargs: Paramètres à substituer dans le template

    Returns:
        Requête avec placeholders remplacés

    Example:
        sql = load_query_template(
            "extract/incremental.sql",
            cutoff_date="2026-01-01"
        )
    """
    content = load_query(relative_path)
    for key, value in kwargs.items():
        placeholder = "{" + key + "}"
        safe_value = str(value).replace("'", "''")
        content = content.replace(placeholder, safe_value)
    log.debug("Template rempli : %s avec %d paramètres", relative_path, len(kwargs))
    return content


def load_extractions_config(config_file: str = "tables.yaml") -> dict:
    """
    Charge la configuration des extractions depuis un fichier YAML.

    Le fichier décrit pour chaque extraction :
    - type: incremental, full_load
    - target_table: table cible
    - columns: liste de colonnes
    - query: requête SQL ou chemin vers un .sql
    - conflict_col: colonne pour l'upsert
    - update_cols: colonnes à mettre à jour en cas de conflit
    - cutoff_days: fenêtre temporelle pour extraction incrémentale

    Args:
        config_file: Nom du fichier YAML (default: "tables.yaml")

    Returns:
        Dict avec la clé 'extractions'

    Raises:
        FileNotFoundError: Si le fichier est absent
        ValueError: Si le YAML n'a pas de clé 'extractions'
    """
    path = SQL_ROOT / "extract" / config_file

    if not path.exists():
        raise FileNotFoundError(
            f"Configuration introuvable : {path}\n"
            f"SQL_ROOT configuré : {SQL_ROOT}"
        )

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not config or "extractions" not in config:
        raise ValueError(f"Fichier YAML invalide (pas de clé 'extractions') : {path}")

    log.debug(
        "Configuration chargée : %s (%d extractions)",
        config_file,
        len(config["extractions"]),
    )
    return config
