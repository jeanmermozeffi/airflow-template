import logging

from dags._bootstrap import bootstrap_project_paths

bootstrap_project_paths()

from orchestration.airflow.dag_factory import build_standard_dag

logger = logging.getLogger(__name__)


def run_ingestion() -> None:
    """Exécute la logique d'ingestion (placeholder à remplacer par le métier)."""
    logger.info("Pipeline ingestion: démarrage de l'extraction des données source.")


dag = build_standard_dag(
    pipeline_name="ingestion",
    run_callable=run_ingestion,
    description="Pipeline standard d'ingestion de données.",
)

