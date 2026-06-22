from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402


def test_dagbag_import_has_no_error() -> None:
    """Vérifie que tous les DAGs du template s'importent sans erreur."""
    project_root = Path(__file__).resolve().parents[1]
    dag_bag = DagBag(dag_folder=str(project_root / "dags"), include_examples=False)
    assert dag_bag.import_errors == {}


def test_standard_dags_exist() -> None:
    """Vérifie la présence des DAGs standard du template."""
    project_root = Path(__file__).resolve().parents[1]
    dag_bag = DagBag(dag_folder=str(project_root / "dags"), include_examples=False)

    expected = {"ingestion_dag", "transformation_dag", "validation_dag"}
    assert expected.issubset(set(dag_bag.dags.keys()))

