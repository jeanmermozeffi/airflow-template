import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.postgres.hooks.postgres")

from plugins.operators.sql_quality_operator import SqlQualityOperator  # noqa: E402


def test_sql_quality_operator_init() -> None:
    """Vérifie l'instanciation de l'operator custom de qualité SQL."""
    operator = SqlQualityOperator(
        task_id="quality_check",
        postgres_conn_id="dwh_postgres",
        sql="SELECT 1",
        min_rows=1,
    )
    assert operator.postgres_conn_id == "dwh_postgres"
    assert operator.min_rows == 1

