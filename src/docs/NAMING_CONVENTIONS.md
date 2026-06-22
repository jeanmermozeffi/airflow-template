# Conventions de nommage — Airflow Orchestration Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Table des matières

1. [Branches Git](#1-branches-git)
2. [DAGs Airflow](#2-dags-airflow)
3. [Tasks Airflow](#3-tasks-airflow)
4. [Fichiers Python](#4-fichiers-python)
5. [Variables Airflow](#5-variables-airflow)
6. [Connexions Airflow](#6-connexions-airflow)
7. [Variables d'environnement](#7-variables-denvironnement)
8. [Base de données](#8-base-de-données)
9. [Docker et infrastructure](#9-docker-et-infrastructure)
10. [Tests](#10-tests)

---

## 1. Branches Git

### Format

```
<type>/<NOM_PROJET>-<numero_ticket>-<description>
```

### Types autorisés

| Type | Usage |
|------|-------|
| `feature` | Nouvelle fonctionnalité |
| `fix` | Correction de bug |
| `hotfix` | Correction urgente en production |
| `release` | Préparation d'une release |
| `chore` | Tâches techniques (dépendances, config) |
| `docs` | Documentation uniquement |
| `refactor` | Refactoring sans nouvelle fonctionnalité |
| `test` | Ajout ou modification de tests uniquement |

### Exemples

```bash
feature/SYNELIA-19-dq-staging-vs-source-count-checks
fix/SYNELIA-24-correction-dag-refresh-dwh
hotfix/SYNELIA-31-fix-prod-airflow-scheduler
release/SYNELIA-v1.0.0
chore/SYNELIA-12-update-docker-compose
docs/SYNELIA-08-add-onboarding-guide
refactor/SYNELIA-45-split-extract-load-tasks
test/SYNELIA-52-add-dag-unit-tests
```

### Règles

- Description en **minuscules** avec des **tirets**
- Pas d'espaces, ni de caractères spéciaux (`/` séparatif uniquement)
- Numéro de ticket inclus quand disponible
- Longueur de la description : 3–7 mots

---

## 2. DAGs Airflow

### Format général

```
dag_<domaine>_<source>_<objectif>
```

### Composantes

| Composante | Description | Exemples |
|------------|-------------|---------|
| `dag_` | Préfixe obligatoire | — |
| `<domaine>` | Domaine métier ou technique | `bi`, `finance`, `rh`, `monitoring`, `dq` |
| `<source>` | Système source | `orangescrum`, `erp`, `crm`, `kafka` |
| `<objectif>` | Action principale | `extract_tasks`, `load_dwh`, `refresh_marts` |

### Exemples

```python
# Ingestion
dag_bi_orangescrum_extract_tasks
dag_bi_orangescrum_extract_projects
dag_finance_erp_extract_invoices
dag_rh_hrms_extract_employees

# Transformation / Chargement
dag_bi_orangescrum_load_dwh
dag_finance_erp_load_dwh
dag_bi_orangescrum_refresh_marts

# Monitoring / Qualité
dag_monitoring_check_data_quality
dag_dq_orangescrum_staging_vs_source
dag_monitoring_sla_check

# Orchestration
dag_orchestration_bi_full_pipeline
dag_orchestration_daily_refresh
```

### Règles

- Utiliser uniquement des **minuscules** et des **underscores**
- Pas de tirets, pas de majuscules, pas de caractères spéciaux
- Le `dag_id` doit être **unique** dans l'instance Airflow
- Longueur recommandée : < 60 caractères

---

## 3. Tasks Airflow

### Format général

```
<action>_<objet>_<cible>
```

### Composantes

| Composante | Description | Exemples |
|------------|-------------|---------|
| `<action>` | Verbe d'action | `extract`, `load`, `check`, `refresh`, `notify`, `validate`, `clean` |
| `<objet>` | Objet sur lequel porte l'action | `tasks`, `projects`, `count`, `fact_tasks` |
| `<cible>` | Destination ou contexte | `from_source`, `to_staging`, `to_dwh`, `on_failure` |

### Exemples

```python
# Extraction
extract_tasks_from_source
extract_projects_from_api
extract_invoices_from_erp

# Chargement
load_tasks_to_staging
load_tasks_to_dwh
load_fact_tasks

# Contrôle qualité
check_staging_vs_source_count
check_nulls_in_key_columns
check_duplicates_in_fact_table
validate_date_consistency

# Transformation
refresh_fact_tasks
refresh_dim_projects
transform_raw_to_staging

# Notification
notify_team_on_failure
notify_team_on_success
send_daily_report

# Utilitaires
start_pipeline
end_pipeline
skip_if_no_data
```

### Règles

- Utiliser uniquement des **minuscules** et des **underscores**
- Commencer par un **verbe** à l'infinitif
- Les `task_id` doivent être **uniques dans leur DAG**
- Longueur recommandée : < 50 caractères

---

## 4. Fichiers Python

### Format général

```
<domaine>_<fonction>.py
```

### Fichiers de DAGs

```
dag_<domaine>_<source>_<objectif>.py
```

**Exemples :**
```
dags/dag_bi_orangescrum_extract_tasks.py
dags/dag_bi_orangescrum_load_dwh.py
dags/dag_monitoring_check_data_quality.py
```

### Fichiers de la librairie `src/`

```
<domaine>_<fonction>.py
```

**Exemples :**
```
src/orchestration/db/connection_factory.py
src/orchestration/db/postgres_connection.py
src/orchestration/db/postgres_utils.py
src/orchestration/kafka/client_config.py
src/orchestration/airflow/dag_factory.py
src/orchestration/airflow/config_loader.py
src/orchestration/common/config.py
src/orchestration/common/env_paths.py
```

### Fichiers de tests

```
test_<module_testé>.py
```

**Exemples :**
```
tests/test_dags.py
tests/test_config_loader.py
tests/test_kafka_config.py
tests/test_multi_sources_config.py
tests/test_operators.py
```

### Règles

- **snake_case** obligatoire (minuscules + underscores)
- Un fichier = une responsabilité clairement identifiée
- Pas de caractères spéciaux, accents ou espaces
- Extension `.py` obligatoire pour les fichiers Python

---

## 5. Variables Airflow

### Format général

```
<NOM_PROJET>__<ENV>__<NOM_VARIABLE>
```

### Composantes

| Composante | Description | Exemples |
|------------|-------------|---------|
| `<NOM_PROJET>` | Identifiant du projet en majuscules | `SYNELIA`, `OSCRUMBI`, `FINOPS` |
| `<ENV>` | Environnement | `DEV`, `STAGING`, `PROD` |
| `<NOM_VARIABLE>` | Nom de la variable en majuscules | `DWH_SCHEMA`, `ALERT_EMAIL`, `SOURCE_DB_HOST` |

### Exemples

```bash
# Connexions bases de données
SYNELIA__DEV__SOURCE_DB_HOST=localhost
SYNELIA__STAGING__SOURCE_DB_HOST=staging-db.synelia.com
SYNELIA__PROD__SOURCE_DB_HOST=prod-db.synelia.com

# Schémas
SYNELIA__DEV__DWH_SCHEMA=public
SYNELIA__PROD__DWH_SCHEMA=analytics

# Alerting
SYNELIA__DEV__ALERT_EMAIL=data-dev@synelia.com
SYNELIA__PROD__ALERT_EMAIL=data-alerts@synelia.com

# Configuration métier
SYNELIA__PROD__MAX_RECORDS_PER_BATCH=5000
SYNELIA__PROD__ENABLE_SLACK_ALERTS=true
```

### Accès dans les DAGs

```python
from airflow.models import Variable

env = Variable.get("SYNELIA__PROD__DWH_SCHEMA", default_var="public")
email = Variable.get("SYNELIA__PROD__ALERT_EMAIL")
```

### Règles

- Séparateur double underscore `__` entre les composantes
- Tout en **MAJUSCULES**
- Pas d'espaces, pas de tirets, pas de caractères spéciaux
- Valeurs sensibles marquées `is_encrypted=True` dans Airflow

---

## 6. Connexions Airflow

### Format général

```
<source>_<type_connexion>
```

### Exemples

```bash
# Bases de données
dwh_postgres          # Data Warehouse PostgreSQL
staging_postgres      # Staging PostgreSQL
crm_mysql             # CRM MySQL
erp_mssql             # ERP SQL Server
raw_postgres          # Landing Zone PostgreSQL

# APIs
orangescrum_api       # API OrangeScrum
slack_data_alerts     # Webhook Slack
smtp_alerts           # SMTP pour emails

# Kafka
kafka_main_cluster    # Cluster Kafka principal
kafka_staging_cluster # Cluster Kafka staging

# Stockage
s3_data_lake          # AWS S3
minio_local           # MinIO local
```

### Règles

- **snake_case** obligatoire
- Format `<source>_<type>` pour les connexions DB
- Format `<service>_<usage>` pour les services externes

---

## 7. Variables d'environnement

### Format général des variables techniques Airflow

```
AIRFLOW__<SECTION>__<CLE>=<valeur>
```

**Exemples :**
```bash
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
AIRFLOW__CORE__FERNET_KEY=...
AIRFLOW__WEBSERVER__SECRET_KEY=...
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
```

### Format des variables multi-sources SQL

```
ORCH_DB__<SOURCE>__<PARAMETRE>=<valeur>
```

**Exemples :**
```bash
ORCH_DB__DWH__TYPE=postgres
ORCH_DB__DWH__HOST=postgres
ORCH_DB__DWH__PORT=5432
ORCH_DB__DWH__DB=analytics
ORCH_DB__DWH__USER=analytics_user
ORCH_DB__DWH__PASSWORD=analytics_password

ORCH_DB__CRM__TYPE=mysql
ORCH_DB__CRM__HOST=mysql
ORCH_DB__CRM__PORT=3306
```

### Format des variables multi-clusters Kafka

```
ORCH_KAFKA__<CLUSTER>__<PARAMETRE>=<valeur>
```

**Exemples :**
```bash
ORCH_KAFKA__MAIN__BROKERS=kafka1:9092,kafka2:9092
ORCH_KAFKA__MAIN__SECURITY_PROTOCOL=PLAINTEXT
ORCH_KAFKA__STAGING__BROKERS=kafka-staging:9092
ORCH_KAFKA__STAGING__SECURITY_PROTOCOL=SASL_SSL
```

---

## 8. Base de données

### Schémas

| Schéma | Usage |
|--------|-------|
| `landing` | Données brutes telles que reçues |
| `staging` | Données nettoyées et typées |
| `dw` ou `dwh` | Data Warehouse (faits et dimensions) |
| `marts` | Data Marts (agrégats BI) |
| `audit` | Logs de chargement et qualité |

### Tables

Format : `<schéma>.<prefixe>_<domaine>_<entité>`

| Préfixe | Usage |
|---------|-------|
| `raw_` | Table de landing brute |
| `stg_` | Table de staging |
| `dim_` | Table de dimension |
| `fact_` | Table de faits |
| `mart_` | Table de Data Mart |
| `audit_` | Table d'audit |

**Exemples :**
```sql
landing.raw_orangescrum_tasks
staging.stg_orangescrum_tasks
dwh.dim_projects
dwh.dim_users
dwh.fact_tasks
marts.mart_bi_task_summary
audit.audit_dag_runs
```

### Colonnes techniques standard

| Colonne | Type | Description |
|---------|------|-------------|
| `created_at` | `TIMESTAMP` | Date de création de la ligne |
| `updated_at` | `TIMESTAMP` | Date de dernière mise à jour |
| `loaded_at` | `TIMESTAMP` | Date de chargement par le DAG |
| `dag_run_id` | `VARCHAR` | Identifiant du DAG run qui a chargé |
| `is_deleted` | `BOOLEAN` | Soft delete |
| `valid_from` | `DATE` | Début de validité (SCD Type 2) |
| `valid_to` | `DATE` | Fin de validité (SCD Type 2) |

---

## 9. Docker et infrastructure

### Services Docker Compose

Format : `airflow-<role>`

```yaml
services:
  airflow-webserver
  airflow-scheduler
  airflow-worker
  airflow-init
  airflow-flower
  postgres
  redis
```

### Images Docker

Format : `synelia/<projet>:<version>`

```
synelia/airflow-template:1.0.0
synelia/airflow-template:latest
```

### Réseaux Docker

```
airflow-network       # Réseau interne Airflow
```

### Volumes Docker

```
airflow-postgres-data   # Données PostgreSQL
airflow-redis-data      # Données Redis
```

---

## 10. Tests

### Fichiers de test

```
tests/test_<module_testé>.py
```

### Fonctions de test

```
test_<comportement_attendu>_when_<condition>
```

**Exemples :**
```python
def test_dag_loads_without_import_errors():
def test_config_loader_returns_correct_schedule():
def test_connection_factory_raises_for_unknown_source():
def test_kafka_config_builds_from_env_variables():
def test_extract_task_retries_on_connection_error():
```

### Fixtures pytest

```python
# conftest.py
@pytest.fixture
def airflow_context():
    ...

@pytest.fixture
def mock_postgres_engine():
    ...
```

### Règles

- **Un test = un comportement attendu**
- Noms de test explicites (lire comme une phrase)
- Pas d'abréviation dans les noms de tests

---

*Document rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
