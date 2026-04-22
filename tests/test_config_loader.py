from pathlib import Path

from orchestration.airflow.config_loader import load_pipeline_config


def test_load_pipeline_config_ingestion() -> None:
    """Vérifie la lecture de configuration pipeline depuis le YAML standard."""
    project_root = Path(__file__).resolve().parents[1]
    config = load_pipeline_config("ingestion", file_path=project_root / "config" / "pipelines.yaml")
    assert config.dag_id == "ingestion_dag"
    assert "dwh_postgres" in config.required_connections

