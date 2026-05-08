# Guide de troubleshooting — Airflow Orchestration Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Table des matières

1. [DAG qui ne s'affiche pas dans Airflow](#1-dag-qui-ne-saffiche-pas-dans-airflow)
2. [Erreur d'import Python](#2-erreur-dimport-python)
3. [Problème de dépendances](#3-problème-de-dépendances)
4. [Problème de connexion à une base de données](#4-problème-de-connexion-à-une-base-de-données)
5. [Scheduler qui ne lance pas les tâches](#5-scheduler-qui-ne-lance-pas-les-tâches)
6. [Worker qui ne consomme pas](#6-worker-qui-ne-consomme-pas)
7. [Tâche bloquée en queued](#7-tâche-bloquée-en-queued)
8. [Problème de logs](#8-problème-de-logs)
9. [Problème de variables Airflow](#9-problème-de-variables-airflow)
10. [Problème de connexions Airflow](#10-problème-de-connexions-airflow)
11. [Problème de droits ou de permissions](#11-problème-de-droits-ou-de-permissions)
12. [Commandes de diagnostic](#12-commandes-de-diagnostic)

---

## 1. DAG qui ne s'affiche pas dans Airflow

### Symptômes

- Le DAG n'apparaît pas dans la liste de l'UI Airflow
- Le DAG est absent de `airflow dags list`

### Causes courantes et solutions

#### 1.1 Erreur d'import silencieuse

```bash
# Vérifier les erreurs d'import dans les logs du scheduler
docker compose logs airflow-scheduler | grep -i "broken dag\|import error\|syntax error"

# Tester le fichier directement
docker compose exec airflow-scheduler python /opt/airflow/dags/mon_dag.py
```

#### 1.2 Le fichier n'est pas dans le bon dossier

```bash
# Vérifier que le fichier est dans /opt/airflow/dags/
docker compose exec airflow-scheduler ls /opt/airflow/dags/

# Vérifier le volume monté
docker compose exec airflow-scheduler python -c "
import airflow
print(airflow.settings.DAGS_FOLDER)
"
```

#### 1.3 Le fichier ne contient pas d'objet DAG

Airflow cherche des objets de type `DAG` dans les fichiers. Vérifier que le DAG est bien instancié au niveau module :

```python
# Bon : DAG instancié au niveau module
with DAG(dag_id="mon_dag", ...) as dag:
    ...

# Mauvais : DAG dans une fonction, non visible par Airflow
def create_my_dag():
    with DAG(dag_id="mon_dag", ...) as dag:
        ...
    return dag  # ← Airflow ne voit pas ce DAG
```

#### 1.4 Le DAG est en pause

```bash
# Vérifier si le DAG est en pause
airflow dags list | grep mon_dag

# Réactiver le DAG
airflow dags unpause mon_dag
```

#### 1.5 Délai de parsing

Airflow parse les DAGs périodiquement. Attendre le prochain cycle ou forcer :

```bash
# Forcer le re-parsing des DAGs
docker compose restart airflow-scheduler
```

---

## 2. Erreur d'import Python

### Symptômes

```
Broken DAG: [/opt/airflow/dags/mon_dag.py] Traceback (most recent call last):
  ...
ImportError: No module named 'src.orchestration.db'
```

### Solutions

#### 2.1 Module non trouvé : vérifier `_bootstrap.py`

```bash
# Vérifier que _bootstrap.py existe et est importé
cat /opt/airflow/dags/_bootstrap.py

# Vérifier que le dag importe _bootstrap
head -5 /opt/airflow/dags/mon_dag.py
```

Le fichier DAG doit commencer par :
```python
import dags._bootstrap  # noqa — ajoute src/ au PYTHONPATH
from src.orchestration.db.connection_factory import get_engine
```

#### 2.2 Package non installé

```bash
# Vérifier les packages installés
docker compose exec airflow-scheduler pip list | grep <package>

# Vérifier requirements.txt
cat requirements.txt | grep <package>

# Installer le package manquant (sans modifier l'image)
docker compose exec airflow-scheduler pip install <package>

# Solution permanente : ajouter dans requirements.txt et reconstruire
docker compose build && docker compose up -d
```

#### 2.3 Erreur de syntaxe Python

```bash
# Vérifier la syntaxe
docker compose exec airflow-scheduler python -m py_compile /opt/airflow/dags/mon_dag.py
echo $?  # → 0 = OK, 1 = erreur

# Vérifier avec ruff
ruff check dags/mon_dag.py
```

---

## 3. Problème de dépendances

### Conflit de versions de packages

```bash
# Voir les conflits
docker compose exec airflow-scheduler pip check

# Voir toutes les dépendances d'un package
docker compose exec airflow-scheduler pip show apache-airflow
```

### Solution recommandée

```bash
# Utiliser un environment de test isolé
python3 -m venv .venv-test
source .venv-test/bin/activate
pip install -r requirements.txt
pip check
```

### Dépendances Airflow providers manquantes

```bash
# Vérifier les providers installés
docker compose exec airflow-scheduler airflow info

# Installer un provider manquant
# Exemple : provider PostgreSQL
pip install apache-airflow-providers-postgres

# Exemple : provider Slack
pip install apache-airflow-providers-slack
```

---

## 4. Problème de connexion à une base de données

### Symptômes

```
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not connect to server
```

### Diagnostic

```bash
# 1. Tester la connexion depuis le container
docker compose exec airflow-scheduler python -c "
from src.orchestration.db.connection_factory import get_engine
engine = get_engine('DWH')
with engine.connect() as conn:
    result = conn.execute('SELECT 1')
    print('Connexion OK :', result.scalar())
"

# 2. Vérifier les variables d'environnement
docker compose exec airflow-scheduler env | grep ORCH_DB__DWH

# 3. Tester la connectivité réseau
docker compose exec airflow-scheduler ping -c 3 <hôte_db>

# 4. Tester le port
docker compose exec airflow-scheduler nc -zv <hôte_db> 5432
```

### Causes courantes et solutions

| Cause | Solution |
|-------|----------|
| Hôte incorrect | Vérifier `ORCH_DB__DWH__HOST` dans `.env` |
| Port incorrect | Vérifier `ORCH_DB__DWH__PORT` |
| Mauvais mot de passe | Vérifier `ORCH_DB__DWH__PASSWORD` |
| Service DB non démarré | `docker compose up -d postgres` |
| Réseau Docker incorrect | Vérifier que le service est dans le bon network |
| SSL requis | Ajouter `ORCH_DB__DWH__PARAMS=sslmode=require` |

### Tester une connexion Airflow

```bash
# Tester via CLI
airflow connections test dwh_postgres

# Tester via Python
docker compose exec airflow-scheduler python -c "
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('dwh_postgres')
print(f'Host: {conn.host}:{conn.port}/{conn.schema}')
"
```

---

## 5. Scheduler qui ne lance pas les tâches

### Symptômes

- Les DAG runs sont créés mais les tâches restent en état `None` ou `scheduled`
- Le scheduler ne progresse pas

### Diagnostic

```bash
# Vérifier l'état du scheduler
curl http://localhost:8080/health
# → "scheduler": {"status": "healthy"}

# Voir les logs du scheduler
docker compose logs --tail=200 airflow-scheduler | grep -i "error\|warn\|critical"

# Vérifier que le scheduler tourne
docker compose ps airflow-scheduler
```

### Causes courantes et solutions

#### 5.1 Scheduler crashé

```bash
# Redémarrer le scheduler
docker compose restart airflow-scheduler

# Si le crash persiste : vérifier les logs
docker compose logs --tail=500 airflow-scheduler
```

#### 5.2 Problème de connexion à la metadata DB

```bash
# Tester la connexion
docker compose exec airflow-scheduler airflow db check
```

#### 5.3 DAGs en pause

```bash
# Vérifier si le DAG est actif
airflow dags list | grep mon_dag
airflow dags unpause mon_dag
```

#### 5.4 `start_date` dans le futur

```python
# Mauvais : start_date dans le futur → aucun run planifié
with DAG(start_date=datetime(2030, 1, 1), ...):

# Bon : start_date dans le passé
with DAG(start_date=datetime(2026, 1, 1), ...):
```

---

## 6. Worker qui ne consomme pas

### Symptômes (CeleryExecutor uniquement)

- Tâches en état `queued` depuis longtemps
- Worker actif mais aucune tâche n'est prise en charge

### Diagnostic

```bash
# Vérifier l'état des workers (Flower)
# http://localhost:5555

# Vérifier via CLI Celery
docker compose exec airflow-worker celery -A airflow.executors.celery_executor.app inspect active

# Vérifier la connexion Redis (broker)
docker compose exec redis redis-cli ping
# → PONG

# Voir les messages dans la queue
docker compose exec redis redis-cli llen airflow
```

### Solutions

```bash
# Redémarrer les workers
docker compose restart airflow-worker

# Scaler les workers si la queue est saturée
docker compose up -d --scale airflow-worker=4

# Vérifier les logs des workers
docker compose logs --tail=200 airflow-worker | grep -i error
```

---

## 7. Tâche bloquée en queued

### Symptômes

- Une tâche reste en état `queued` sans jamais passer à `running`
- Le délai d'attente dépasse 10-15 minutes

### Causes courantes

| Cause | Diagnostic | Solution |
|-------|-----------|----------|
| Tous les slots de pool utilisés | UI > Admin > Pools | Augmenter les slots du pool |
| Tous les workers occupés | Flower > Workers | Scaler les workers |
| `max_active_tasks_per_dag` atteint | Voir config Airflow | Augmenter la limite |
| Priorité trop basse | Voir `priority_weight` | Augmenter la priorité |
| Dépendance non résolue | Vérifier le DAG graph | Identifier la tâche bloquante |

### Diagnostic pool

```bash
# Voir l'utilisation des pools
airflow pools list

# Augmenter les slots d'un pool
airflow pools set dwh_pool 10 "Pool pour les opérations DWH"
```

### Forcer l'exécution (à utiliser avec précaution)

```bash
# Ignorer les dépendances et forcer l'exécution
airflow tasks run mon_dag ma_tache 2026-05-08 --ignore-all-dependencies
```

---

## 8. Problème de logs

### Symptômes

- Logs inaccessibles depuis l'UI Airflow
- Message : "Log file does not exist"
- Logs vides ou tronqués

### Diagnostic

```bash
# Vérifier l'emplacement des logs
docker compose exec airflow-scheduler airflow config get-value logging base_log_folder

# Vérifier les droits sur le dossier logs
docker compose exec airflow-scheduler ls -la /opt/airflow/logs/

# Vérifier l'espace disque
docker compose exec airflow-scheduler df -h /opt/airflow/logs/
```

### Solutions

```bash
# Corriger les droits
docker compose exec airflow-scheduler chmod -R 755 /opt/airflow/logs/

# Nettoyer les anciens logs (> 30 jours)
find ./logs -name "*.log" -mtime +30 -delete

# Configurer la rétention automatique dans airflow.cfg
# [logging]
# log_retention_days = 30
```

### Logs distants (S3)

Si les logs sont configurés sur S3 et inaccessibles :
```bash
# Vérifier la configuration
docker compose exec airflow-scheduler airflow config get-value logging remote_logging
docker compose exec airflow-scheduler airflow config get-value logging remote_base_log_folder

# Tester l'accès S3
docker compose exec airflow-scheduler aws s3 ls s3://mon-bucket/airflow-logs/
```

---

## 9. Problème de variables Airflow

### Symptômes

```
airflow.exceptions.AirflowException: Variable 'SYNELIA__PROD__DWH_SCHEMA' does not exist
```

### Solutions

```bash
# Vérifier si la variable existe
airflow variables list | grep SYNELIA__PROD__DWH_SCHEMA

# Créer la variable manuellement
airflow variables set SYNELIA__PROD__DWH_SCHEMA analytics

# Réexécuter le bootstrap
docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py

# Utiliser une valeur par défaut dans le code (tolérance aux erreurs)
value = Variable.get("SYNELIA__PROD__DWH_SCHEMA", default_var="public")
```

### Importer/exporter des variables

```bash
# Exporter toutes les variables
airflow variables export variables_backup.json

# Importer des variables depuis un fichier
airflow variables import variables_backup.json
```

---

## 10. Problème de connexions Airflow

### Symptômes

```
airflow.exceptions.AirflowNotFoundException: The conn_id 'dwh_postgres' isn't defined
```

### Solutions

```bash
# Vérifier si la connexion existe
airflow connections list | grep dwh_postgres

# Ajouter la connexion manuellement
airflow connections add dwh_postgres \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-schema analytics \
  --conn-login analytics_user \
  --conn-password analytics_password

# Réexécuter le bootstrap
docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py

# Tester la connexion
airflow connections test dwh_postgres
```

### Connexion chiffrée corrompue

Si la Fernet Key a changé, les connexions chiffrées deviennent illisibles :

```bash
# Générer une nouvelle Fernet Key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Mettre à jour dans .env
# AIRFLOW__CORE__FERNET_KEY=<nouvelle_clé>

# Recréer toutes les connexions depuis config/connections.yaml
docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py
```

---

## 11. Problème de droits ou de permissions

### Problème d'accès à l'UI

```bash
# Vérifier les rôles de l'utilisateur
airflow users list

# Modifier le rôle
airflow users add-role -u mon_utilisateur -r Admin

# Réinitialiser le mot de passe
airflow users reset-password -u mon_utilisateur -p <nouveau_mot_de_passe>
```

### Problème de droits sur les fichiers

```bash
# Vérifier l'UID utilisé
docker compose exec airflow-scheduler id

# Corriger les droits sur les dags
chown -R 50000:0 ./dags ./logs ./plugins

# Vérifier AIRFLOW_UID dans .env
grep AIRFLOW_UID .env
```

### Problème de permissions Docker

```bash
# Vérifier les volumes Docker
docker inspect airflow-template_airflow-data | grep -A5 Mounts

# Corriger les permissions
sudo chown -R $(id -u):$(id -g) ./logs
```

---

## 12. Commandes de diagnostic

### Diagnostic général

```bash
# État des services
docker compose ps

# Santé de l'instance Airflow
curl -s http://localhost:8080/health | python3 -m json.tool

# Version d'Airflow
docker compose exec airflow-scheduler airflow version

# Informations système
docker compose exec airflow-scheduler airflow info

# Vérification de la base de données
docker compose exec airflow-scheduler airflow db check

# Connexion Redis
docker compose exec redis redis-cli ping
docker compose exec redis redis-cli info server | head -10
```

### Diagnostic DAGs

```bash
# Lister tous les DAGs avec leur statut
airflow dags list

# Voir les erreurs d'import
airflow dags list-import-errors

# Vérifier un fichier DAG
airflow dags show mon_dag

# Voir le prochain run planifié
airflow dags next-execution mon_dag
```

### Diagnostic tasks

```bash
# Voir les tasks d'un DAG run
airflow tasks states-for-dag-run mon_dag <run_id>

# Voir les tâches en cours
airflow tasks list mon_dag

# Voir les tasks zombie (bloquées)
airflow tasks list -d mon_dag --tree
```

### Diagnostic réseau

```bash
# Lister les réseaux Docker
docker network ls

# Inspecter le réseau Airflow
docker network inspect airflow-network

# Tester la connectivité entre services
docker compose exec airflow-scheduler ping -c 3 postgres
docker compose exec airflow-scheduler nc -zv postgres 5432
docker compose exec airflow-scheduler nc -zv redis 6379
```

### Diagnostic ressources

```bash
# Utilisation des ressources
docker stats --no-stream

# Espace disque
docker system df

# Logs (taille)
du -sh ./logs/
```

### Nettoyage d'urgence

```bash
# Vider la queue Celery (attention : perd les tâches en attente)
docker compose exec redis redis-cli del airflow

# Marquer toutes les tâches en running comme failed (tâches zombies)
docker compose exec airflow-scheduler airflow tasks clear -d mon_dag --yes

# Redémarrer tous les services
docker compose restart

# Redémarrage complet (données préservées)
docker compose down && docker compose up -d
```

---

## Escalade

Si le problème persiste après avoir suivi ce guide :

1. **Collecter les logs** : `docker compose logs > debug_$(date +%Y%m%d_%H%M%S).log`
2. **Documenter la reproduction** : étapes exactes pour reproduire le problème
3. **Contacter le référent technique** : Jean Mermoz Effi (mangoua.effi@uvci.edu.ci)
4. **Consulter la documentation officielle** : https://airflow.apache.org/docs/
5. **Ouvrir un ticket** sur le projet avec les informations collectées

---

*Guide rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
