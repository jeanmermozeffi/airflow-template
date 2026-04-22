from airflow.providers.postgres.hooks.postgres import PostgresHook


class DbHealthHook(PostgresHook):
    """Expose un test de santé simple pour uniformiser les checks DB dans les DAGs."""

    def ping(self) -> bool:
        """Retourne `True` si la base répond à `SELECT 1`."""
        result = self.get_first("SELECT 1;")
        return bool(result and result[0] == 1)

