# Architecture du template

## Objectif

Industrialiser les orchestrations Airflow via:
- configuration centralisée,
- composants réutilisables,
- structure homogène et testable.

## Composants clés

- `dags/`:
  - définition légère des pipelines (ingestion, transformation, validation).
- `src/orchestration/`:
  - logique partagée (factory DAG, config loader, DB utilities, bootstrap env).
- `config/`:
  - connexions, variables et planning des DAGs.
- `plugins/`:
  - extensions Airflow standardisées.

## Convention multi-sources

- SQL multi-connecteurs:
  - `ORCH_DB__<SOURCE>__TYPE` (`postgres`, `mysql`, `mssql`, `oracle`, `sqlite`)
  - `ORCH_DB__<SOURCE>__HOST`, `PORT`, `DB`, `USER`, `PASSWORD`
- Kafka multi-clusters:
  - `ORCH_KAFKA__<CLUSTER>__BROKERS`
  - `ORCH_KAFKA__<CLUSTER>__SECURITY_PROTOCOL`

Exemple de plusieurs sources PostgreSQL:
- `ORCH_DB__DWH__TYPE=postgres`
- `ORCH_DB__RAW__TYPE=postgres`

## Réutilisation issue de `cic-bi-airflow`

Les briques suivantes ont été reprises et généralisées:
- résolution d'environnement dynamique (`env_paths.py`),
- gestion centralisée des chemins/config (`config.py`),
- utilitaires de connexion PostgreSQL (`postgres_connection.py`),
- bootstrap de chemin pour imports DAG (`dags/_bootstrap.py`).
