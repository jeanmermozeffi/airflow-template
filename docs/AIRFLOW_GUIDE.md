# Guide technique Apache Airflow

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Table des matières

1. [Rôle d'Apache Airflow dans le projet](#1-rôle-dapache-airflow-dans-le-projet)
2. [Structure des DAGs](#2-structure-des-dags)
3. [Structure des tasks](#3-structure-des-tasks)
4. [Utilisation des operators](#4-utilisation-des-operators)
5. [Utilisation des sensors](#5-utilisation-des-sensors)
6. [Gestion des variables Airflow](#6-gestion-des-variables-airflow)
7. [Gestion des connexions Airflow](#7-gestion-des-connexions-airflow)
8. [Gestion des pools](#8-gestion-des-pools)
9. [Gestion des retries](#9-gestion-des-retries)
10. [Gestion des SLA](#10-gestion-des-sla)
11. [Gestion des logs](#11-gestion-des-logs)
12. [Stratégie de monitoring](#12-stratégie-de-monitoring)
13. [Bonnes pratiques de planification](#13-bonnes-pratiques-de-planification)
14. [Bonnes pratiques de performance](#14-bonnes-pratiques-de-performance)
15. [Règles de sécurité](#15-règles-de-sécurité)

---

## 1. Rôle d'Apache Airflow dans le projet

Apache Airflow est l'**orchestrateur central** des pipelines de données du pôle Data SYNELIA. Il permet de :

- **Planifier** l'exécution des pipelines selon des schedules (cron, intervalles)
- **Orchestrer** les dépendances entre tâches (upstream / downstream)
- **Surveiller** l'état d'exécution des pipelines en temps réel
- **Relancer** automatiquement les tâches en échec
- **Notifier** les équipes en cas d'anomalie ou de dépassement de SLA
- **Centraliser** les logs d'exécution

Airflow **n'est pas** un moteur de calcul — il délègue l'exécution à des systèmes externes (bases de données, Spark, APIs) via ses operators.

---

## 2. Structure des DAGs

### 2.1 Anatomie d'un DAG

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Définition des arguments par défaut
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-alerts@synelia.com"],
}

# 2. Définition du DAG
with DAG(
    dag_id="dag_bi_orangescrum_extract_tasks",
    description="Extraction des tâches OrangeScrum vers la landing zone",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",       # 6h00 tous les jours
    catchup=False,               # Pas de backfill automatique
    max_active_runs=1,           # Un seul run actif à la fois
    tags=["ingestion", "bi", "orangescrum"],
    doc_md="""
    ## DAG : dag_bi_orangescrum_extract_tasks

    Extrait les tâches depuis l'API OrangeScrum et les charge dans la landing zone.

    **Schedule :** Quotidien à 6h00
    **Source :** OrangeScrum API
    **Destination :** PostgreSQL Landing Zone (schema `landing`)
    """,
) as dag:

    # 3. Définition des tâches
    extract = PythonOperator(
        task_id="extract_tasks_from_source",
        python_callable=extract_tasks,
    )

    load = PythonOperator(
        task_id="load_tasks_to_staging",
        python_callable=load_to_staging,
    )

    notify = PythonOperator(
        task_id="notify_team_on_success",
        python_callable=send_success_notification,
        trigger_rule="all_success",
    )

    # 4. Définition des dépendances
    extract >> load >> notify
```

### 2.2 Conventions de nommage des DAGs

```
dag_<domaine>_<source>_<objectif>
```

Exemples :
- `dag_bi_orangescrum_extract_tasks`
- `dag_bi_orangescrum_load_dwh`
- `dag_monitoring_check_data_quality`
- `dag_finance_erp_refresh_marts`

### 2.3 Tags recommandés

| Tag | Usage |
|-----|-------|
| `ingestion` | DAG d'extraction/chargement |
| `transformation` | DAG de transformation |
| `data-quality` | DAG de contrôle qualité |
| `monitoring` | DAG de surveillance |
| `notification` | DAG d'alerting |
| `bi` | Lié à la BI |
| `finance`, `rh`, etc. | Domaine métier |

---

## 3. Structure des tasks

### 3.1 Convention de nommage des tasks

```
<action>_<objet>_<cible>
```

Exemples :
- `extract_tasks_from_source`
- `load_tasks_to_staging`
- `check_staging_vs_source_count`
- `refresh_fact_tasks`
- `notify_team_on_failure`

### 3.2 Principes de design des tasks

- **Atomicité :** chaque task doit faire une seule chose
- **Idempotence :** une task peut être re-exécutée sans effet de bord
- **Stateless :** pas d'état partagé entre tasks (utiliser XCom si nécessaire)
- **Légèreté :** logique métier dans `src/orchestration/`, pas dans la task

### 3.3 Utilisation des XComs

```python
# Pousser une valeur
def extract_count(**context):
    count = 1500
    context["ti"].xcom_push(key="record_count", value=count)

# Récupérer une valeur
def validate_count(**context):
    count = context["ti"].xcom_pull(
        task_ids="extract_tasks_from_source",
        key="record_count"
    )
    assert count > 0, f"Aucun enregistrement extrait"
```

**Attention :** XComs sont stockés en base de données Airflow — éviter les gros volumes. Max recommandé : quelques Ko.

---

## 4. Utilisation des operators

### 4.1 PythonOperator

Pour exécuter une fonction Python :

```python
from airflow.operators.python import PythonOperator

def my_function(**context):
    logical_date = context["logical_date"]
    # ...

task = PythonOperator(
    task_id="extract_tasks_from_source",
    python_callable=my_function,
    op_kwargs={"source": "orangescrum"},
)
```

### 4.2 BashOperator

Pour exécuter des commandes shell :

```python
from airflow.operators.bash import BashOperator

task = BashOperator(
    task_id="run_dbt_transformation",
    bash_command="cd /opt/dbt && dbt run --models staging",
    env={"DBT_PROFILES_DIR": "/opt/dbt"},
)
```

### 4.3 PostgresOperator

Pour exécuter des requêtes SQL :

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

task = PostgresOperator(
    task_id="refresh_fact_tasks",
    postgres_conn_id="dwh_postgres",
    sql="sql/refresh_fact_tasks.sql",  # ou SQL inline
)
```

### 4.4 BranchPythonOperator

Pour les flux conditionnels :

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    count = context["ti"].xcom_pull(task_ids="extract", key="count")
    if count > 0:
        return "load_to_staging"
    return "skip_loading"

branch = BranchPythonOperator(
    task_id="check_data_availability",
    python_callable=choose_branch,
)
```

### 4.5 TriggerDagRunOperator

Pour déclencher un autre DAG :

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger = TriggerDagRunOperator(
    task_id="trigger_transformation_dag",
    trigger_dag_id="dag_bi_orangescrum_load_dwh",
    wait_for_completion=False,  # Ne pas bloquer le DAG courant
    conf={"source_date": "{{ ds }}"},
)
```

---

## 5. Utilisation des sensors

Les sensors attendent qu'une condition soit vraie avant de laisser passer le flux.

### 5.1 FileSensor

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_input_file",
    filepath="/opt/airflow/data/input/{{ ds }}/data.csv",
    poke_interval=60,    # Vérifie toutes les 60 secondes
    timeout=3600,        # Timeout après 1 heure
    mode="reschedule",   # Libère le worker pendant l'attente
)
```

### 5.2 ExternalTaskSensor

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_extraction_dag",
    external_dag_id="dag_bi_orangescrum_extract_tasks",
    external_task_id=None,  # Attend la fin du DAG complet
    allowed_states=["success"],
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
)
```

### 5.3 Bonnes pratiques sensors

- Toujours utiliser `mode="reschedule"` pour éviter de bloquer un worker
- Définir un `timeout` raisonnable (max 24h)
- Utiliser `poke_interval` adapté : 60s minimum pour éviter la surcharge

---

## 6. Gestion des variables Airflow

### 6.1 Définition

Les variables Airflow stockent des paramètres de configuration accessibles depuis les DAGs.

Convention de nommage : `<PROJET>__<ENV>__<NOM_VARIABLE>`

```bash
SYNELIA__DEV__DWH_SCHEMA=public
SYNELIA__PROD__ALERT_EMAIL=data-alerts@synelia.com
SYNELIA__STAGING__SOURCE_DB_HOST=staging-db.synelia.com
```

### 6.2 Utilisation dans les DAGs

```python
from airflow.models import Variable

# Lecture simple
schema = Variable.get("SYNELIA__PROD__DWH_SCHEMA")

# Lecture avec valeur par défaut
email = Variable.get("SYNELIA__PROD__ALERT_EMAIL", default_var="fallback@synelia.com")

# Lecture d'un JSON
config = Variable.get("SYNELIA__PROD__PIPELINE_CONFIG", deserialize_json=True)
```

### 6.3 Bootstrap automatique

Les variables sont chargées depuis `config/variables.yaml` via `scripts/bootstrap_airflow.py` :

```yaml
variables:
  - key: SYNELIA__DEV__DWH_SCHEMA
    value: public
  - key: SYNELIA__DEV__ALERT_EMAIL
    value: data-dev@synelia.com
```

---

## 7. Gestion des connexions Airflow

### 7.1 Définition

Les connexions Airflow encapsulent les credentials de connexion aux systèmes externes.

### 7.2 Utilisation dans les DAGs

```python
from airflow.hooks.base import BaseHook

# Récupérer une connexion
conn = BaseHook.get_connection("dwh_postgres")
host = conn.host
password = conn.password

# Avec le helper du projet
from src.orchestration.db.connection_factory import get_engine
engine = get_engine("DWH")
```

### 7.3 Bootstrap automatique

Les connexions sont chargées depuis `config/connections.yaml` :

```yaml
connections:
  - conn_id: dwh_postgres
    conn_type: postgres
    host: "{{ env('DWH_HOST') }}"
    port: 5432
    schema: "{{ env('DWH_DB') }}"
    login: "{{ env('DWH_USER') }}"
    password: "{{ env('DWH_PASSWORD') }}"
```

---

## 8. Gestion des pools

Les pools limitent le parallélisme des tâches pour éviter la surcharge des systèmes cibles.

### 8.1 Création d'un pool

Via l'UI Airflow : Admin > Pools > Ajouter

Via CLI :
```bash
airflow pools set dwh_pool 5 "Pool pour les opérations DWH"
airflow pools set api_pool 2 "Pool pour les appels API externes"
```

### 8.2 Utilisation dans les tasks

```python
task = PythonOperator(
    task_id="load_tasks_to_dwh",
    python_callable=load_function,
    pool="dwh_pool",         # Limite à 5 exécutions simultanées
    pool_slots=1,            # Utilise 1 slot du pool
)
```

### 8.3 Pools recommandés

| Pool | Slots | Usage |
|------|-------|-------|
| `dwh_pool` | 5 | Opérations Data Warehouse |
| `staging_pool` | 10 | Opérations Staging |
| `api_pool` | 2 | Appels APIs externes |
| `kafka_pool` | 3 | Consommation Kafka |

---

## 9. Gestion des retries

### 9.1 Configuration globale (default_args)

```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # Délai exponentiel
    "max_retry_delay": timedelta(hours=1),
}
```

### 9.2 Configuration par task

```python
task = PythonOperator(
    task_id="extract_tasks_from_source",
    python_callable=extract_function,
    retries=3,
    retry_delay=timedelta(minutes=10),
)
```

### 9.3 Bonnes pratiques

- `retries >= 1` toujours configuré en production
- `retry_delay` adapté à la nature de l'erreur (5–30 min pour les accès réseau)
- `retry_exponential_backoff=True` pour les APIs rate-limitées
- Logger l'exception avant de la lever pour tracer les erreurs

---

## 10. Gestion des SLA

### 10.1 Définition

Le SLA (Service Level Agreement) définit le délai maximal acceptable pour l'exécution d'un DAG.

```python
from datetime import timedelta

with DAG(
    dag_id="dag_bi_orangescrum_extract_tasks",
    sla_miss_callback=sla_miss_alert,     # Callback en cas de dépassement
    default_args={
        "sla": timedelta(hours=2),        # SLA par task
    },
    ...
) as dag:
    ...
```

### 10.2 Callback SLA

```python
def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    message = f"SLA breach pour le DAG {dag.dag_id}: tasks {task_list}"
    send_slack_alert(message)
    send_email_alert(message)
```

### 10.3 SLA par environnement (`monitoring/sla.yaml`)

```yaml
sla_policies:
  - dag_id: dag_bi_orangescrum_extract_tasks
    sla_hours: 2
    notify_email: data-alerts@synelia.com
  - dag_id: dag_monitoring_check_data_quality
    sla_hours: 1
    notify_email: data-alerts@synelia.com
```

---

## 11. Gestion des logs

### 11.1 Configuration

```ini
# config/airflow.cfg
[logging]
base_log_folder = /opt/airflow/logs
logging_level = INFO
log_format = [%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s
```

### 11.2 Logging dans les fonctions Python

```python
import logging

log = logging.getLogger(__name__)

def extract_tasks(**context):
    log.info("Démarrage de l'extraction — date : %s", context["ds"])
    try:
        records = fetch_from_api()
        log.info("Extraction terminée : %d enregistrements", len(records))
        return len(records)
    except Exception as e:
        log.error("Erreur lors de l'extraction : %s", str(e), exc_info=True)
        raise
```

### 11.3 Retention des logs

- Développement : 7 jours
- Staging : 30 jours
- Production : 90 jours (configurable dans `airflow.cfg`)

---

## 12. Stratégie de monitoring

### 12.1 Alertes par email

Configurées via `default_args` :

```python
default_args = {
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-alerts@synelia.com"],
}
```

### 12.2 Alertes Slack (optionnel)

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

notify_slack = SlackWebhookOperator(
    task_id="notify_team_on_failure",
    slack_webhook_conn_id="slack_data_alerts",
    message=":red_circle: DAG {{ dag.dag_id }} a échoué sur {{ ds }}",
    trigger_rule="one_failed",
)
```

### 12.3 Callbacks de DAG

```python
def on_failure_callback(context):
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    log.error("DAG %s failed — run_id: %s", dag_id, run_id)
    send_alert(dag_id, run_id)

with DAG(
    on_failure_callback=on_failure_callback,
    ...
) as dag:
    ...
```

---

## 13. Bonnes pratiques de planification

### Schedules recommandés

```python
# Expressions cron communes
"@daily"          # Quotidien à minuit
"@hourly"         # Toutes les heures
"0 6 * * *"       # Tous les jours à 6h00
"0 6 * * 1-5"     # Du lundi au vendredi à 6h00
"0 */4 * * *"     # Toutes les 4 heures
"0 6 * * 1"       # Tous les lundis à 6h00
"0 6 1 * *"       # Le 1er de chaque mois à 6h00
```

### Règles

- `catchup=False` par défaut pour éviter les backfills non désirés
- `max_active_runs=1` pour les DAGs avec dépendances sequentielles
- Éviter les schedules < 5 minutes sur des DAGs lourds
- Utiliser `depends_on_past=True` uniquement si les runs sont réellement séquentiels
- Espacer les schedules pour éviter la surcharge simultanée (stagger)

---

## 14. Bonnes pratiques de performance

- **Parallélisme :** configurer `max_active_tasks_per_dag` selon la capacité
- **Pools :** limiter la concurrence sur les systèmes sensibles
- **Lazy loading :** éviter les imports lourds au niveau du module DAG
- **SQL optimisé :** utiliser des requêtes paramétrées, éviter `SELECT *`
- **Batch processing :** préférer les insertions en batch (5000–10000 lignes)
- **Connexions :** utiliser le connection pooling (`pool_size`, `max_overflow`)
- **Idempotence :** `INSERT ... ON CONFLICT DO UPDATE` plutôt que `DELETE + INSERT`

---

## 15. Règles de sécurité

| Règle | Détail |
|-------|--------|
| Pas de secrets dans le code | Utiliser les connexions et variables Airflow |
| Chiffrement des métadonnées | Configurer `AIRFLOW__CORE__FERNET_KEY` |
| RBAC activé | Configurer des rôles (Viewer, User, Op, Admin) |
| HTTPS en production | Nginx reverse proxy avec TLS |
| Variables masquées | Utiliser `is_encrypted=True` pour les valeurs sensibles |
| Audit logs | Activer `AIRFLOW__WEBSERVER__AUDIT_LOG` |
| Network isolation | Services dans un réseau Docker dédié |

---

*Guide rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
