# Guide de déploiement — Airflow Orchestration Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Table des matières

1. [Environnements disponibles](#1-environnements-disponibles)
2. [Configuration par environnement](#2-configuration-par-environnement)
3. [Variables nécessaires](#3-variables-nécessaires)
4. [Secrets à configurer](#4-secrets-à-configurer)
5. [Commandes Docker](#5-commandes-docker)
6. [Commandes Airflow](#6-commandes-airflow)
7. [Déploiement en développement](#7-déploiement-en-développement)
8. [Déploiement en staging](#8-déploiement-en-staging)
9. [Déploiement en production](#9-déploiement-en-production)
10. [Procédure de rollback](#10-procédure-de-rollback)
11. [Contrôles post-déploiement](#11-contrôles-post-déploiement)

---

## 1. Environnements disponibles

| Environnement | Description | URL Airflow | Branche Git |
|---------------|-------------|-------------|-------------|
| **dev** | Développement local — données de test | `http://localhost:8080` | toute branche |
| **staging** | Pré-production — données réelles (copie) | `http://airflow-staging.synelia.com` | `main` |
| **prod** | Production — données réelles | `http://airflow.synelia.com` | tag `vX.Y.Z` |

---

## 2. Configuration par environnement

### Fichiers Docker Compose

| Fichier | Environnement | Description |
|---------|---------------|-------------|
| `deployment/docker-compose.yml` | Tous | Stack de base (services communs) |
| `deployment/docker-compose.dev.yml` | Dev | LocalExecutor, volumes de développement |
| `deployment/docker-compose.staging.yml` | Staging | CeleryExecutor, config staging |
| `deployment/docker-compose.prod.yml` | Production | CeleryExecutor, optimisations prod |

### Différences entre environnements

| Paramètre | Dev | Staging | Prod |
|-----------|-----|---------|------|
| Executor | LocalExecutor | CeleryExecutor | CeleryExecutor |
| Workers | 1 (scheduler) | 2 | 4+ |
| Redis | Local | Partagé | Cluster Redis |
| Postgres metadata | Local | Dédié staging | Dédié prod (HA) |
| Logs | Volume local | S3 staging | S3 prod |
| HTTPS | Non | Oui (auto-signé) | Oui (certificat valide) |
| Fernet Key | Dev key | Staging key | Prod key (rotation 90j) |

---

## 3. Variables nécessaires

### Variables obligatoires (tous environnements)

```bash
# Environnement
AIRFLOW_ENV=dev|staging|prod
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_HOME=/opt/airflow

# Base metadata Airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://<user>:<password>@<host>/<db>

# Sécurité
AIRFLOW__CORE__FERNET_KEY=<fernet_key_base64>
AIRFLOW__WEBSERVER__SECRET_KEY=<secret_key>

# Admin
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<mot_de_passe_fort>
```

### Variables SMTP (alerting par email)

```bash
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_USER=<email>
AIRFLOW__SMTP__SMTP_PASSWORD=<app_password>
AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow@synelia.com
```

### Variables de connexion métier

```bash
# Data Warehouse
DWH_HOST=<hôte>
DWH_PORT=5432
DWH_DB=analytics
DWH_USER=<utilisateur>
DWH_PASSWORD=<mot_de_passe>
DWH_SCHEMA=public

# Multi-sources SQL
ORCH_DB__DWH__TYPE=postgres
ORCH_DB__DWH__HOST=<hôte>
ORCH_DB__DWH__PORT=5432
ORCH_DB__DWH__DB=analytics
ORCH_DB__DWH__USER=<utilisateur>
ORCH_DB__DWH__PASSWORD=<mot_de_passe>
```

### Génération de la Fernet Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 4. Secrets à configurer

### Hiérarchie de gestion des secrets

| Environnement | Outil | Description |
|---------------|-------|-------------|
| Dev | Fichier `.env` local | Non versionné, partagé via canal sécurisé |
| Staging | Variables d'environnement CI/CD | GitHub Secrets ou GitLab CI Variables |
| Prod | Vault / AWS Secrets Manager | Rotation automatique recommandée |

### Secrets critiques en production

```
AIRFLOW__CORE__FERNET_KEY              → Chiffrement des connexions Airflow
AIRFLOW__WEBSERVER__SECRET_KEY          → Sessions Flask
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN     → Connexion metadata DB
<ENV>_DWH_PASSWORD                      → Mot de passe DWH
<ENV>_SMTP_PASSWORD                     → Mot de passe SMTP
```

### Rotation des secrets

- **Fernet Key :** tous les 90 jours en production (avec migration des connexions chiffrées)
- **Mots de passe DB :** tous les 90 jours
- **Tokens API :** selon la politique du fournisseur

---

## 5. Commandes Docker

### Construire les images

```bash
# Image de base
docker build -t synelia/airflow-template:1.0.0 .

# Vérifier l'image
docker run --rm synelia/airflow-template:1.0.0 python -c "import airflow; print(airflow.__version__)"
```

### Opérations de base

```bash
# Démarrer les services
docker compose -f deployment/docker-compose.yml up -d

# Arrêter les services
docker compose -f deployment/docker-compose.yml down

# Arrêter et supprimer les volumes (attention : perte de données)
docker compose -f deployment/docker-compose.yml down -v

# Voir les logs en temps réel
docker compose -f deployment/docker-compose.yml logs -f

# Voir les logs d'un service spécifique
docker compose -f deployment/docker-compose.yml logs -f airflow-scheduler

# Statut des services
docker compose -f deployment/docker-compose.yml ps

# Accéder au shell d'un service
docker compose exec airflow-scheduler bash
docker compose exec postgres psql -U airflow
```

### Nettoyage

```bash
# Supprimer les images inutilisées
docker image prune -f

# Supprimer tous les conteneurs arrêtés
docker container prune -f

# Nettoyage complet (images, conteneurs, réseaux, volumes sans utilisateur)
docker system prune -f
```

---

## 6. Commandes Airflow

### DAGs

```bash
# Lister tous les DAGs
airflow dags list

# Voir les détails d'un DAG
airflow dags details -d dag_bi_orangescrum_extract_tasks

# Déclencher un DAG
airflow dags trigger dag_bi_orangescrum_extract_tasks

# Déclencher avec une configuration
airflow dags trigger dag_bi_orangescrum_extract_tasks --conf '{"source_date": "2026-05-08"}'

# Lister les runs d'un DAG
airflow dags list-runs -d dag_bi_orangescrum_extract_tasks

# Pauser un DAG
airflow dags pause dag_bi_orangescrum_extract_tasks

# Réactiver un DAG
airflow dags unpause dag_bi_orangescrum_extract_tasks

# Effacer des runs (pour re-exécuter)
airflow dags clear -d dag_bi_orangescrum_extract_tasks -s 2026-05-08 -e 2026-05-08
```

### Tasks

```bash
# Tester une task (sans affecter la metadata DB)
airflow tasks test dag_bi_orangescrum_extract_tasks extract_tasks_from_source 2026-05-08

# Voir les logs d'une task
airflow tasks logs dag_bi_orangescrum_extract_tasks extract_tasks_from_source 2026-05-08

# Lister les tasks d'un DAG
airflow tasks list dag_bi_orangescrum_extract_tasks
```

### Connexions et variables

```bash
# Lister les connexions
airflow connections list

# Ajouter une connexion
airflow connections add dwh_postgres --conn-type postgres --conn-host localhost --conn-port 5432

# Lister les variables
airflow variables list

# Lire une variable
airflow variables get SYNELIA__PROD__DWH_SCHEMA

# Définir une variable
airflow variables set SYNELIA__PROD__DWH_SCHEMA analytics
```

### Base de données

```bash
# Initialiser la base de données Airflow
airflow db init

# Migrer la base de données (après mise à jour Airflow)
airflow db migrate

# Créer un utilisateur admin
airflow users create --username admin --role Admin --email admin@synelia.com \
  --firstname Admin --lastname Admin --password Admin@123
```

### Bootstrap

```bash
# Seeder les connexions et variables depuis config/
python scripts/bootstrap_airflow.py

# Ou via Docker
docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py
```

---

## 7. Déploiement en développement

### Procédure complète

```bash
# 1. Cloner et configurer
git clone <url-repo>
cd airflow-template
cp .env.example .env
# Éditer .env avec les valeurs locales

# 2. Installer les dépendances Python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Initialiser Airflow
docker compose -f deployment/docker-compose.dev.yml up -d airflow-init
# Attendre la fin (~ 2-3 minutes)

# 4. Démarrer les services
docker compose -f deployment/docker-compose.dev.yml up -d

# 5. Bootstrapper la configuration
docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py

# 6. Vérifier
docker compose -f deployment/docker-compose.dev.yml ps
# → Tous les services doivent être "healthy"

# 7. Accéder à l'UI
# http://localhost:8080 (admin / Admin@123)
```

### Mise à jour du code en développement

```bash
# Les volumes Docker reflètent les changements de code en temps réel
# Pas besoin de redémarrer pour les DAGs

# Pour les changements de requirements.txt
docker compose -f deployment/docker-compose.dev.yml build
docker compose -f deployment/docker-compose.dev.yml up -d
```

---

## 8. Déploiement en staging

### Prérequis

- Accès au serveur staging
- Variables CI/CD configurées dans GitHub/GitLab
- Merger la branche sur `main`

### Procédure

```bash
# 1. Se connecter au serveur staging
ssh deploy@airflow-staging.synelia.com

# 2. Mettre à jour le code
cd /opt/airflow-template
git pull origin main

# 3. Construire les nouvelles images (si Dockerfile modifié)
docker compose -f deployment/docker-compose.staging.yml build

# 4. Démarrer les migrations de base de données
docker compose -f deployment/docker-compose.staging.yml run --rm airflow-scheduler airflow db migrate

# 5. Redémarrer les services
docker compose -f deployment/docker-compose.staging.yml up -d --no-deps airflow-scheduler
docker compose -f deployment/docker-compose.staging.yml up -d --no-deps airflow-webserver
docker compose -f deployment/docker-compose.staging.yml up -d --no-deps airflow-worker

# 6. Bootstrapper la configuration (si connexions/variables modifiées)
docker compose -f deployment/docker-compose.staging.yml exec airflow-scheduler \
  python scripts/bootstrap_airflow.py

# 7. Vérifier les contrôles post-déploiement
```

### Via CI/CD (GitHub Actions)

```yaml
# .github/workflows/deploy-staging.yml
name: Deploy to Staging
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to staging server
        run: |
          ssh deploy@staging "cd /opt/airflow-template && \
            git pull && \
            docker compose -f deployment/docker-compose.staging.yml up -d && \
            docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py"
```

---

## 9. Déploiement en production

> **ATTENTION :** Le déploiement en production doit toujours être précédé d'une validation en staging.

### Prérequis

- Validation complète en staging (tests fonctionnels, monitoring 24h)
- Tag Git créé : `git tag -a v1.1.0 -m "Release v1.1.0"`
- Revue et approbation du Lead Data Engineer
- Fenêtre de maintenance planifiée (si impact sur les SLA)
- Backup de la base de données metadata Airflow

### Procédure de déploiement production

```bash
# 1. Backup de la base de données metadata
docker compose exec postgres pg_dump -U airflow airflow > backup_airflow_$(date +%Y%m%d_%H%M%S).sql

# 2. Notifier l'équipe du début de la fenêtre de maintenance
# (email, Slack, etc.)

# 3. Pauser les DAGs critiques (si nécessaire)
docker compose exec airflow-scheduler airflow dags pause dag_bi_orangescrum_extract_tasks

# 4. Attendre la fin des runs en cours
docker compose exec airflow-scheduler airflow dags list-runs -d dag_bi_orangescrum_extract_tasks

# 5. Mettre à jour le code
git checkout v1.1.0
git pull

# 6. Migrer la base de données
docker compose -f deployment/docker-compose.prod.yml run --rm airflow-scheduler airflow db migrate

# 7. Déployer les services (rolling update)
docker compose -f deployment/docker-compose.prod.yml up -d --no-deps --scale airflow-worker=4 airflow-worker
docker compose -f deployment/docker-compose.prod.yml up -d --no-deps airflow-scheduler
docker compose -f deployment/docker-compose.prod.yml up -d --no-deps airflow-webserver

# 8. Bootstrapper la configuration
docker compose -f deployment/docker-compose.prod.yml exec airflow-scheduler \
  python scripts/bootstrap_airflow.py

# 9. Réactiver les DAGs
docker compose -f deployment/docker-compose.prod.yml exec airflow-scheduler \
  airflow dags unpause dag_bi_orangescrum_extract_tasks

# 10. Effectuer les contrôles post-déploiement
```

---

## 10. Procédure de rollback

### Rollback rapide (< 30 minutes après déploiement)

```bash
# 1. Identifier le tag précédent
git tag --sort=-version:refname | head -5

# 2. Checkout de la version précédente
git checkout v1.0.0

# 3. Reconstruire et redémarrer
docker compose -f deployment/docker-compose.prod.yml build
docker compose -f deployment/docker-compose.prod.yml up -d

# 4. Rollback de la base de données si migration effectuée
# (Airflow ne supporte pas le downgrade automatique — restaurer depuis backup)
docker compose exec postgres psql -U airflow -c "SELECT version FROM alembic_version;"
# Restaurer depuis le backup si nécessaire :
docker compose exec -T postgres psql -U airflow airflow < backup_airflow_20260508_120000.sql
```

### Rollback de migration de base de données

```bash
# Vérifier la version actuelle de la migration
docker compose exec airflow-scheduler airflow db check

# Rollback manuel (si la version le supporte)
docker compose exec airflow-scheduler airflow db downgrade <revision>

# En cas d'impossibilité de downgrade : restaurer le backup
docker compose exec postgres dropdb -U airflow airflow
docker compose exec postgres createdb -U airflow airflow
docker compose exec -T postgres psql -U airflow airflow < backup_airflow.sql
```

---

## 11. Contrôles post-déploiement

### Checklist immédiate (dans les 15 minutes)

```bash
# 1. Vérifier que tous les services sont healthy
docker compose -f deployment/docker-compose.prod.yml ps

# 2. Vérifier les logs pour des erreurs
docker compose -f deployment/docker-compose.prod.yml logs --tail=100 airflow-scheduler | grep -i error
docker compose -f deployment/docker-compose.prod.yml logs --tail=100 airflow-webserver | grep -i error

# 3. Vérifier la connectivité de l'UI
curl -f http://localhost:8080/health
# → Doit retourner {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}

# 4. Vérifier que les DAGs se chargent
docker compose exec airflow-scheduler airflow dags list 2>&1 | grep -i error
# → Aucune erreur d'import

# 5. Vérifier les connexions
docker compose exec airflow-scheduler airflow connections test dwh_postgres
```

### Checklist dans les 2 heures

```bash
# 6. Déclencher un DAG de test
docker compose exec airflow-scheduler \
  airflow dags trigger dag_monitoring_check_data_quality

# 7. Vérifier l'exécution
docker compose exec airflow-scheduler \
  airflow dags list-runs -d dag_monitoring_check_data_quality

# 8. Vérifier les métriques de monitoring (Grafana / Airflow UI)
#    - Durée des runs
#    - Statut des tasks
#    - Pas de tâches en état "zombie"

# 9. Confirmer la fin de la fenêtre de maintenance (si applicable)
# → Notifier l'équipe via Slack / email
```

### En cas d'anomalie post-déploiement

1. **Ne pas paniquer** — lire les logs pour identifier la cause
2. **Évaluer l'impact** : production impactée ? SLA breachée ?
3. **Décider** : corriger rapidement (hotfix) ou rollback ?
4. **Communiquer** avec l'équipe
5. Si rollback : suivre la [procédure de rollback](#10-procédure-de-rollback)

---

*Guide rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
