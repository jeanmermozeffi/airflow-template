# Flux DAG standard

Chaque DAG du template suit le flux suivant:

1. `start`
2. `check_connections`
3. `run_pipeline`
4. `end`

Ce flux est généré par `src/orchestration/airflow/dag_factory.py` afin d'assurer:
- une structure homogène entre projets,
- une validation systématique des dépendances techniques,
- un coût de maintenance réduit.

## Où changer le comportement

- Changer les paramètres d'un DAG: `config/pipelines.yaml`
- Ajouter de la logique métier: fonction `run_*` dans `dags/*.py`
- Ajouter des checks techniques: `src/orchestration/db/postgres_utils.py`

