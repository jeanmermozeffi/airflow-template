# Bonnes pratiques Data Engineering

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Table des matières

1. [Structuration des DAGs](#1-structuration-des-dags)
2. [Idempotence des traitements](#2-idempotence-des-traitements)
3. [Gestion des erreurs](#3-gestion-des-erreurs)
4. [Gestion des reprises](#4-gestion-des-reprises)
5. [Logs structurés](#5-logs-structurés)
6. [Séparation des environnements](#6-séparation-des-environnements)
7. [Sécurité des secrets](#7-sécurité-des-secrets)
8. [Qualité des données](#8-qualité-des-données)
9. [Tests unitaires](#9-tests-unitaires)
10. [Tests d'intégration](#10-tests-dintégration)
11. [Contrôle des volumes](#11-contrôle-des-volumes)
12. [Monitoring des pipelines](#12-monitoring-des-pipelines)
13. [Alerting](#13-alerting)
14. [Documentation des flux](#14-documentation-des-flux)
15. [Revue de code](#15-revue-de-code)
16. [Gestion des dépendances](#16-gestion-des-dépendances)
17. [Performance des traitements](#17-performance-des-traitements)

---

## 1. Structuration des DAGs

### Principe de séparation des responsabilités

Un DAG bien structuré suit le principe de **séparation stricte** entre la définition du pipeline et la logique métier.

```
DAG (déclaratif)          →   src/orchestration/ (logique)
─────────────────              ──────────────────────────────
Définit QUI fait QUOI          Contient le COMMENT
Ordre d'exécution              Helpers réutilisables
Dépendances entre tasks        Connexions, transformations
Schedule                       Fonctions testables
```

### Structure d'un DAG

```python
# dags/dag_bi_orangescrum_extract_tasks.py
from src.orchestration.airflow.dag_factory import create_extraction_dag
from src.orchestration.bi.orangescrum import extract_tasks, load_to_staging

# Logique dans src/, DAG reste déclaratif
with create_dag(...) as dag:
    t1 = PythonOperator(task_id="extract_tasks_from_source", python_callable=extract_tasks)
    t2 = PythonOperator(task_id="load_tasks_to_staging", python_callable=load_to_staging)
    t1 >> t2
```

### Règles de structuration

- **Un DAG = un flux métier** cohérent et isolé
- **Pas de logique SQL complexe** dans le fichier DAG — la mettre dans `include/sql/`
- **Pas d'imports dynamiques** dans les DAGs (ralentit le parsing)
- **Définir les dépendances explicitement** : préférer `>>` à `set_downstream()`
- **Grouper les tasks** liées avec `TaskGroup`

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("extraction", tooltip="Extraction depuis la source") as tg_extract:
    t_extract = PythonOperator(task_id="extract_tasks_from_source", ...)
    t_validate = PythonOperator(task_id="validate_extraction_count", ...)
    t_extract >> t_validate
```

---

## 2. Idempotence des traitements

L'idempotence garantit qu'une tâche peut être re-exécutée plusieurs fois sans altérer le résultat final.

### Patron d'upsert (INSERT … ON CONFLICT)

```sql
-- Bon : idempotent
INSERT INTO staging.stg_orangescrum_tasks (task_id, name, updated_at)
VALUES (:task_id, :name, :updated_at)
ON CONFLICT (task_id) DO UPDATE SET
    name = EXCLUDED.name,
    updated_at = EXCLUDED.updated_at,
    loaded_at = NOW();

-- Mauvais : non idempotent
INSERT INTO staging.stg_orangescrum_tasks (task_id, name, updated_at)
VALUES (:task_id, :name, :updated_at);
```

### Patron DELETE + INSERT partitionné

```python
def load_partition(ds: str, engine):
    with engine.begin() as conn:
        # Supprimer uniquement la partition du jour
        conn.execute("DELETE FROM staging.stg_tasks WHERE loaded_date = :ds", {"ds": ds})
        # Réinsérer les données du jour
        conn.execute("INSERT INTO staging.stg_tasks SELECT * FROM landing.raw_tasks WHERE date = :ds", {"ds": ds})
```

### Règles d'idempotence

- Toujours filtrer par `logical_date` (ou `ds`) pour n'affecter que la partition courante
- Utiliser des transactions atomiques (`BEGIN` / `COMMIT`)
- Éviter les `AUTO_INCREMENT` sans clé naturelle (préférer des clés métier)
- Tester la re-exécution manuelle sur les tâches critiques

---

## 3. Gestion des erreurs

### Hiérarchie des exceptions

```python
from src.orchestration.common.exceptions import (
    DataExtractionError,
    DataValidationError,
    ConnectionError,
)

def extract_tasks(**context):
    try:
        records = api_client.fetch_tasks()
    except requests.Timeout as e:
        raise DataExtractionError(f"Timeout lors de l'extraction : {e}") from e
    except requests.HTTPError as e:
        raise DataExtractionError(f"Erreur HTTP {e.response.status_code}") from e
```

### Ne pas avaler les exceptions

```python
# Mauvais : exception avalée silencieusement
try:
    process_data()
except Exception:
    pass  # ← INTERDIT

# Bon : logger puis propager
try:
    process_data()
except Exception as e:
    log.error("Échec du traitement : %s", str(e), exc_info=True)
    raise
```

### Erreurs récupérables vs irrécupérables

| Type | Exemples | Action |
|------|----------|--------|
| **Récupérable** | Timeout réseau, rate limit API | Retry automatique |
| **Irrécupérable** | Données corrompues, schéma incorrect | Fail + alerte immédiate |
| **Partielle** | Certains enregistrements invalides | Logger + continuer + rapport |

---

## 4. Gestion des reprises

### Configuration des retries

```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,    # 5min, 10min, 20min…
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=2),
}
```

### Reprises manuelles

```bash
# Relancer une task spécifique
airflow tasks clear -d dag_bi_orangescrum_extract_tasks -t extract_tasks_from_source -s 2026-05-08

# Relancer un DAG complet
airflow dags clear -d dag_bi_orangescrum_extract_tasks -s 2026-05-08
```

### Stratégie de reprise pour les traitements longs

```python
def extract_with_checkpoint(**context):
    ds = context["ds"]
    checkpoint_key = f"checkpoint_{ds}"

    # Vérifier si déjà partiellement complété
    last_id = Variable.get(checkpoint_key, default_var=0)

    records = fetch_since_id(last_id)
    for batch in chunks(records, 1000):
        load_batch(batch)
        # Sauvegarder la progression
        Variable.set(checkpoint_key, batch[-1]["id"])

    # Nettoyer le checkpoint à la fin
    Variable.delete(checkpoint_key)
```

---

## 5. Logs structurés

### Format recommandé

```python
import logging
log = logging.getLogger(__name__)

# Contexte structuré dans chaque message
log.info(
    "Extraction terminée",
    extra={
        "dag_id": "dag_bi_orangescrum_extract_tasks",
        "source": "orangescrum",
        "record_count": 1500,
        "duration_seconds": 12.5,
    }
)
```

### Niveaux de logs

| Niveau | Usage |
|--------|-------|
| `DEBUG` | Informations de débogage détaillées (désactivé en prod) |
| `INFO` | Événements normaux (début/fin de task, compteurs) |
| `WARNING` | Anomalie non bloquante (données manquantes, valeur inattendue) |
| `ERROR` | Erreur bloquante (exception catchée et re-levée) |
| `CRITICAL` | Erreur système grave (perte de données, corruption) |

### Informations à toujours logger

```python
def extract_tasks(**context):
    log.info("Démarrage extraction — dag_run_id: %s, ds: %s", context["run_id"], context["ds"])

    start = time.time()
    records = fetch_data()
    duration = time.time() - start

    log.info("Extraction terminée — %d enregistrements en %.2fs", len(records), duration)
    return len(records)
```

---

## 6. Séparation des environnements

### Structure des fichiers d'environnement

```
.env.example    → Template de référence (versionné)
.env            → Valeurs locales de développement (non versionné)
.env.dev        → Surcharges spécifiques dev
.env.staging    → Surcharges spécifiques staging
.env.prod       → Surcharges spécifiques production
```

### Isolation par environnement

```python
# src/orchestration/common/config.py
import os

ENV = os.getenv("AIRFLOW_ENV", "dev")

def get_config(key: str, default=None):
    env_key = f"SYNELIA__{ENV.upper()}__{key}"
    return os.getenv(env_key, os.getenv(key, default))
```

### Règles de séparation

- Jamais de données de production en développement
- Jamais de credentials prod dans le code ou dans `.env.example`
- Variables d'environnement différentes par env (hosts, schémas, emails)
- Tester les changements en dev et staging **avant** la production

---

## 7. Sécurité des secrets

### Ce qui ne doit JAMAIS être dans le code

```python
# INTERDIT
password = "MonMotDePasse123"
api_key = "sk-1234567890abcdef"
conn_string = "postgresql://user:password@host:5432/db"
```

### Ce qu'il faut faire à la place

```python
# CORRECT — depuis les connexions Airflow
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("dwh_postgres")
password = conn.password

# CORRECT — depuis les variables d'environnement
import os
api_key = os.getenv("SYNELIA__PROD__API_KEY")

# CORRECT — depuis les variables Airflow
from airflow.models import Variable
api_key = Variable.get("SYNELIA__PROD__API_KEY")
```

### Détection automatique de secrets

```yaml
# .pre-commit-config.yaml
- repo: https://github.com/Yelp/detect-secrets
  rev: v1.4.0
  hooks:
    - id: detect-secrets
      args: ['--baseline', '.secrets.baseline']
```

### Rotation des secrets

- Changer les mots de passe tous les **90 jours** en production
- Utiliser un gestionnaire de secrets (Vault, AWS Secrets Manager) en production
- Ne jamais partager de credentials par email ou chat

---

## 8. Qualité des données

### Contrôles systématiques à implémenter

```python
def check_data_quality(**context):
    engine = get_engine("STAGING")
    ds = context["ds"]

    checks = [
        # Contrôle de volume
        f"SELECT COUNT(*) FROM stg_tasks WHERE loaded_date = '{ds}'",
        # Contrôle de nulls sur clés
        f"SELECT COUNT(*) FROM stg_tasks WHERE task_id IS NULL AND loaded_date = '{ds}'",
        # Contrôle de doublons
        f"SELECT COUNT(*) - COUNT(DISTINCT task_id) FROM stg_tasks WHERE loaded_date = '{ds}'",
        # Contrôle de cohérence avec la source
        f"SELECT ABS(source_count - staging_count) FROM dq_count_comparison WHERE ds = '{ds}'",
    ]

    for sql in checks:
        result = engine.execute(sql).scalar()
        if result > 0:
            raise DataValidationError(f"Contrôle qualité échoué : {sql} → {result}")
```

### Seuils d'alerte recommandés

| Contrôle | Seuil d'alerte | Seuil critique |
|----------|---------------|----------------|
| Variation de volume vs J-1 | > 20% | > 50% |
| Taux de nulls sur clés | > 0% | > 0% |
| Taux de doublons | > 0.1% | > 1% |
| Écart source vs staging | > 0.1% | > 1% |

---

## 9. Tests unitaires

### Couverture minimale

- **80%** sur `src/orchestration/`
- **100%** sur les fonctions de transformation critique
- **Syntaxe des DAGs** : tous les DAGs doivent charger sans erreur

### Structure des tests unitaires

```python
# tests/test_config_loader.py
import pytest
from src.orchestration.airflow.config_loader import load_pipeline_config

def test_load_pipeline_config_returns_dag_ids():
    config = load_pipeline_config("config/pipelines.yaml")
    assert "dag_bi_orangescrum_extract_tasks" in [p["dag_id"] for p in config["pipelines"]]

def test_load_pipeline_config_raises_for_missing_file():
    with pytest.raises(FileNotFoundError):
        load_pipeline_config("nonexistent.yaml")

def test_dag_loads_without_import_errors():
    from dags.dag_bi_orangescrum_extract_tasks import dag
    assert dag is not None
    assert dag.dag_id == "dag_bi_orangescrum_extract_tasks"
```

### Mocking des dépendances externes

```python
from unittest.mock import patch, MagicMock

def test_extract_tasks_returns_record_count(mocker):
    mock_engine = MagicMock()
    mock_engine.execute.return_value.fetchall.return_value = [{"id": 1}, {"id": 2}]

    with patch("src.orchestration.db.connection_factory.get_engine", return_value=mock_engine):
        count = extract_tasks(ds="2026-05-08", source="orangescrum")

    assert count == 2
```

---

## 10. Tests d'intégration

### Environnement de test

```python
# conftest.py
import pytest
from sqlalchemy import create_engine

@pytest.fixture(scope="session")
def test_postgres_engine():
    engine = create_engine("postgresql://test:test@localhost:5432/test_db")
    yield engine
    engine.dispose()
```

### Tests d'intégration recommandés

- Vérifier qu'un DAG complet s'exécute du début à la fin en local
- Tester les connexions aux bases de données cibles
- Tester les connexions Kafka
- Valider les transformations SQL sur des jeux de données de test

```bash
# Exécuter les tests d'intégration
pytest tests/ -m integration -v

# Exécuter uniquement les tests unitaires (sans dépendances externes)
pytest tests/ -m "not integration" -v
```

---

## 11. Contrôle des volumes

### Surveillance des volumes

```python
def check_volume_vs_yesterday(**context):
    ds = context["ds"]
    engine = get_engine("DWH")

    result = engine.execute("""
        SELECT
            today.cnt AS today_count,
            yesterday.cnt AS yesterday_count,
            ABS(today.cnt - yesterday.cnt)::FLOAT / NULLIF(yesterday.cnt, 0) AS variation_pct
        FROM
            (SELECT COUNT(*) AS cnt FROM fact_tasks WHERE load_date = :ds) today,
            (SELECT COUNT(*) AS cnt FROM fact_tasks WHERE load_date = :ds::date - 1) yesterday
    """, {"ds": ds}).fetchone()

    if result.variation_pct and result.variation_pct > 0.5:
        raise DataValidationError(
            f"Volume anormal : {result.today_count} vs {result.yesterday_count} hier "
            f"(variation : {result.variation_pct:.1%})"
        )
```

### Limites de traitement

```python
MAX_RECORDS_PER_BATCH = int(Variable.get("SYNELIA__PROD__MAX_RECORDS_PER_BATCH", default_var=5000))
MAX_TOTAL_RECORDS = int(Variable.get("SYNELIA__PROD__MAX_TOTAL_RECORDS", default_var=1_000_000))

def extract_with_limit(**context):
    records = fetch_records(limit=MAX_TOTAL_RECORDS)
    if len(records) == MAX_TOTAL_RECORDS:
        log.warning("Limite de %d enregistrements atteinte — données potentiellement tronquées", MAX_TOTAL_RECORDS)
```

---

## 12. Monitoring des pipelines

### Métriques clés à suivre

| Métrique | Description | Seuil d'alerte |
|----------|-------------|----------------|
| `dag_run_duration` | Durée d'exécution d'un DAG run | > SLA défini |
| `task_failure_rate` | Taux d'échec des tasks (7 jours) | > 5% |
| `queue_wait_time` | Temps d'attente en queue | > 10 min |
| `record_count` | Nombre d'enregistrements traités | ±20% vs J-1 |
| `zombie_tasks` | Tasks bloquées sans activité | > 0 |

### Dashboard de monitoring

Configurer un tableau de bord (Grafana ou Airflow UI) incluant :
- Statut des derniers DAG runs (succès/échec)
- Durée moyenne par DAG sur les 7 derniers jours
- Nombre de tasks en échec/retry
- Alertes actives

---

## 13. Alerting

### Niveaux d'alerte

| Niveau | Déclencheur | Canal | Délai de réponse |
|--------|-------------|-------|-----------------|
| **INFO** | Fin de run réussie | Log | — |
| **WARNING** | Retry d'une task | Email équipe | 4h ouvrables |
| **ERROR** | Échec d'un DAG | Email + Slack | 1h ouvrables |
| **CRITICAL** | Breach SLA en production | Email + Slack + SMS | 30 min |

### Configuration des alertes dans le DAG

```python
def alert_on_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    exception = context.get("exception", "Unknown error")

    message = (
        f"[AIRFLOW ERROR] DAG `{dag_id}` — Task `{task_id}` a échoué.\n"
        f"Run ID : {run_id}\n"
        f"Erreur : {exception}"
    )

    send_email_alert(message, to=["data-alerts@synelia.com"])
    send_slack_alert(message, channel="#data-alerts")

default_args = {
    "on_failure_callback": alert_on_failure,
}
```

---

## 14. Documentation des flux

### Documentation d'un DAG

Chaque DAG doit contenir :

```python
with DAG(
    dag_id="dag_bi_orangescrum_extract_tasks",
    doc_md="""
    ## dag_bi_orangescrum_extract_tasks

    **Objectif :** Extraction quotidienne des tâches OrangeScrum vers la landing zone.

    **Sources :** API OrangeScrum REST v2
    **Destinations :** `landing.raw_orangescrum_tasks`

    **Schedule :** Quotidien à 6h00 UTC

    **Dépendances upstream :** Aucune
    **Dépendances downstream :** `dag_bi_orangescrum_load_dwh`

    **SLA :** 2 heures
    **Contact :** data-team@synelia.com

    **Historique :**
    - 2026-05-08 : Création initiale (Jean Mermoz Effi)
    """,
    ...
) as dag:
```

### Documentation des flux de données

Maintenir un document `docs/dag_flow.md` avec les dépendances entre DAGs :

```
dag_bi_orangescrum_extract_tasks
    └── dag_bi_orangescrum_load_dwh
            └── dag_bi_orangescrum_refresh_marts
                    └── dag_monitoring_check_data_quality
```

---

## 15. Revue de code

### Checklist de revue pour les Data Engineers

```markdown
## Code
- [ ] La logique est dans src/, pas dans le DAG
- [ ] Les fonctions sont testées
- [ ] Pas de secrets dans le code
- [ ] Les connexions utilisent BaseHook.get_connection()

## DAG
- [ ] catchup=False défini
- [ ] retries >= 1 défini
- [ ] max_active_runs défini si pertinent
- [ ] Tags ajoutés
- [ ] doc_md présent

## Performance
- [ ] Pas de SELECT * en production
- [ ] Insertions en batch (pas ligne par ligne)
- [ ] Connexions avec pooling

## Qualité
- [ ] Contrôle de volume présent
- [ ] Idempotence vérifiée
- [ ] Tests passants
```

---

## 16. Gestion des dépendances

### Fichiers de dépendances

```
requirements.txt          → Dépendances de production (épinglées)
requirements-dev.txt      → Dépendances de développement
pyproject.toml            → Configuration des outils (ruff, pytest)
```

### Règles

- **Épingler les versions** en production (`apache-airflow==2.8.1`)
- Utiliser **dependabot** ou **renovate** pour les mises à jour automatiques
- Tester les montées de version sur un environnement de staging avant la prod
- Ne pas ajouter de dépendances non nécessaires (chaque dépendance = risque)

```bash
# Vérifier les vulnérabilités
pip-audit

# Générer un fichier de lock
pip freeze > requirements.lock.txt
```

---

## 17. Performance des traitements

### Optimisation SQL

```sql
-- Mauvais : SELECT * charge toutes les colonnes
SELECT * FROM raw_orangescrum_tasks;

-- Bon : sélectionner uniquement les colonnes nécessaires
SELECT task_id, name, status, updated_at FROM raw_orangescrum_tasks;

-- Mauvais : pas d'index sur la colonne filtrée
SELECT * FROM fact_tasks WHERE loaded_date = '2026-05-08';

-- Bon : index présent sur loaded_date
CREATE INDEX idx_fact_tasks_loaded_date ON fact_tasks(loaded_date);
```

### Insertions en batch

```python
from sqlalchemy import text

def load_in_batches(records: list, engine, batch_size: int = 5000):
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO staging.stg_tasks VALUES (:id, :name, :status)"),
                batch
            )
        log.info("Batch %d/%d chargé (%d lignes)", i // batch_size + 1, len(records) // batch_size + 1, len(batch))
```

### Parallélisation

```python
# Utiliser des pools pour contrôler la concurrence
task = PythonOperator(
    task_id="load_to_dwh",
    python_callable=load_function,
    pool="dwh_pool",     # Max 5 tâches simultanées sur le DWH
)

# Utiliser les TaskGroup pour paralléliser des branches indépendantes
with TaskGroup("parallel_extractions") as tg:
    for source in ["orangescrum", "erp", "crm"]:
        PythonOperator(
            task_id=f"extract_{source}",
            python_callable=extract_function,
            op_kwargs={"source": source},
        )
```

---

*Document rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
