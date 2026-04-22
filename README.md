# Airflow Orchestration Template

Template standard pour industrialiser les orchestrations Airflow avec:
- un minimum de code dupliqué,
- une configuration externalisée,
- des composants réutilisables (DAG factory, connexions DB, seed Airflow).

## Structure

```text
airflow-project/
├── config/                  # Configurations YAML et airflow.cfg de référence
├── dags/                    # DAGs déclaratifs (logique métier légère)
├── docker/                  # Environnement local Docker
├── docs/                    # Documentation d'exploitation
├── monitoring/              # SLA, alertes et politique d'observabilité
├── plugins/                 # Hooks, sensors et operators custom
├── scripts/                 # Scripts d'initialisation (seed Airflow)
├── src/orchestration/       # Librairie réutilisable
└── tests/                   # Tests automatiques
```

## Démarrage rapide

1. Copier `.env.example` vers `.env` et ajuster les valeurs.
2. Lancer le stack local:
   - `docker compose -f docker/docker-compose.yml up -d airflow-init`
   - `docker compose -f docker/docker-compose.yml up -d`
3. Vérifier les DAGs dans l'UI Airflow (`http://localhost:8080`).

## Standard de configuration

- `config/connections.yaml`: connexions Airflow à créer automatiquement.
- `config/variables.yaml`: variables Airflow à injecter automatiquement.
- `config/pipelines.yaml`: planning et paramètres des DAGs.
- `.env`:
  - format multi-sources SQL: `ORCH_DB__<SOURCE>__*`
  - format multi-clusters Kafka: `ORCH_KAFKA__<CLUSTER>__*`

Le script `scripts/bootstrap_airflow.py` applique les connexions et variables en mode idempotent.

## Connexion centrale (DB/Kafka)

- Factory SQL centrale: `src/orchestration/db/connection_factory.py`
- Config multi-sources: `src/orchestration/common/config.py`
- Helper Kafka: `src/orchestration/kafka/client_config.py`
