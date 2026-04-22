import logging

from dags._bootstrap import bootstrap_project_paths

bootstrap_project_paths()

from orchestration.airflow.dag_factory import build_standard_dag

logger = logging.getLogger(__name__)


def run_validation() -> None:
    """Exécute les contrôles qualité et de cohérence des données chargées."""
    logger.info("Pipeline validation: exécution des contrôles de qualité de données.")


dag = build_standard_dag(
    pipeline_name="validation",
    run_callable=run_validation,
    description="Pipeline standard de validation de qualité des données.",
)

