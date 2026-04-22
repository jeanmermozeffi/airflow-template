import logging

from dags._bootstrap import bootstrap_project_paths

bootstrap_project_paths()

from orchestration.airflow.dag_factory import build_standard_dag

logger = logging.getLogger(__name__)


def run_transformation() -> None:
    """Exécute la logique de transformation (placeholder à remplacer par SQL/DBT/Spark)."""
    logger.info("Pipeline transformation: application des règles de transformation.")


dag = build_standard_dag(
    pipeline_name="transformation",
    run_callable=run_transformation,
    description="Pipeline standard de transformation de données.",
)

