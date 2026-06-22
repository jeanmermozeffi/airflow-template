# Guide d'onboarding — Pôle Data / Projet Airflow Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Bienvenue dans l'équipe Data SYNELIA !

Félicitations et bienvenue ! Tu rejoins le **pôle Data de SYNELIA**, une équipe passionnée qui construit et maintient l'infrastructure de données au cœur de nos produits et décisions métier. Ce guide a été conçu pour t'aider à te mettre à l'aise rapidement et à contribuer dès les premiers jours.

Prends le temps de lire ce document en entier — il te donnera tous les repères essentiels pour démarrer dans de bonnes conditions.

---

## Table des matières

1. [Présentation du pôle Data](#1-présentation-du-pôle-data)
2. [Présentation du projet Airflow](#2-présentation-du-projet-airflow)
3. [Rôle d'Airflow dans l'architecture Data](#3-rôle-dairflow-dans-larchitecture-data)
4. [Objectifs du projet](#4-objectifs-du-projet)
5. [Principaux cas d'usage](#5-principaux-cas-dusage)
6. [Organisation des dossiers](#6-organisation-des-dossiers)
7. [Outils nécessaires pour démarrer](#7-outils-nécessaires-pour-démarrer)
8. [Prérequis techniques](#8-prérequis-techniques)
9. [Étapes d'installation locale](#9-étapes-dinstallation-locale)
10. [Accès nécessaires](#10-accès-nécessaires)
11. [Commandes essentielles](#11-commandes-essentielles)
12. [Bonnes pratiques à respecter](#12-bonnes-pratiques-à-respecter)
13. [Erreurs fréquentes à éviter](#13-erreurs-fréquentes-à-éviter)
14. [Contacts et référents techniques](#14-contacts-et-référents-techniques)

---

## 1. Présentation du pôle Data

Le **pôle Data de SYNELIA** est responsable de :

- La collecte, la transformation et le chargement des données provenant de sources multiples (bases de données transactionnelles, APIs, fichiers plats, flux Kafka)
- La construction et la maintenance du Data Warehouse (DWH) et des Data Marts
- La mise en place des pipelines de données (ETL/ELT)
- Le contrôle qualité des données
- L'exposition des données aux équipes BI/Analytiques
- La surveillance et le monitoring des flux de données

L'équipe Data travaille en étroite collaboration avec les équipes BI, les développeurs applicatifs et les équipes métier pour garantir la disponibilité et la fiabilité de la donnée.

---

## 2. Présentation du projet Airflow

**Apache Airflow** est l'orchestrateur central des pipelines de données du pôle Data. Ce repository — `airflow-template` — constitue le **template standard** à partir duquel tous les projets Airflow de l'organisation sont créés.

Il embarque :

- Une structure de projet standardisée et prête à l'emploi
- Des composants réutilisables (DAG factory, helpers de connexion DB, bootstrap)
- Une configuration externalisée et centralisée (YAML + variables d'environnement)
- Des exemples de DAGs pour les cas d'usage courants
- Une documentation complète couvrant tous les aspects du projet

---

## 3. Rôle d'Airflow dans l'architecture Data

Apache Airflow joue le rôle de **chef d'orchestre** : il planifie, déclenche et surveille l'exécution des pipelines de données.

```
Sources de données         Airflow (Orchestrateur)         Destinations
─────────────────          ──────────────────────          ────────────
PostgreSQL (CRM)    ──►    DAG Ingestion          ──►    Landing Zone
MySQL (ERP)         ──►    DAG Transformation     ──►    Data Warehouse
Kafka (streaming)   ──►    DAG Quality Check      ──►    Data Marts
APIs REST           ──►    DAG Notification       ──►    Alerting / BI
Fichiers CSV/JSON   ──►    DAG Validation
```

Airflow **ne déplace pas les données** directement — il orchestre l'exécution des scripts et opérateurs qui le font.

---

## 4. Objectifs du projet

| Objectif | Description |
|----------|-------------|
| **Standardisation** | Fournir une base commune à tous les projets Airflow |
| **Réutilisabilité** | Partager des composants communs (helpers, factories) |
| **Maintenabilité** | Structure claire, code documenté, tests automatisés |
| **Onboarding rapide** | Permettre à un nouveau collaborateur d'être opérationnel en 1 jour |
| **Fiabilité** | Gestion des retries, SLA, monitoring et alerting intégrés |
| **Sécurité** | Secrets externalisés, accès contrôlés par rôle |

---

## 5. Principaux cas d'usage

### 5.1 Ingestion de données

Extraction depuis des sources SQL (PostgreSQL, MySQL, MSSQL) ou des APIs REST et chargement dans une zone Landing.

```
dag_bi_<source>_extract_<objet>
```

### 5.2 Transformation et chargement DWH

Transformation des données brutes et chargement dans les tables du Data Warehouse.

```
dag_bi_<source>_load_dwh
```

### 5.3 Rafraîchissement des Data Marts

Calcul et mise à jour des agrégats et indicateurs des marts BI.

```
dag_bi_<source>_refresh_marts
```

### 5.4 Contrôle qualité des données

Vérification des volumes, des doublons, des valeurs nulles et de la cohérence des données.

```
dag_monitoring_check_data_quality
```

### 5.5 Notifications et alerting

Envoi de rapports de statut et d'alertes en cas d'anomalie.

---

## 6. Organisation des dossiers

```
airflow-template/
├── config/            # Fichiers YAML de configuration (connexions, variables, pipelines)
├── dags/              # Définition des DAGs Airflow
├── src/orchestration/ # Librairie interne réutilisable
│   ├── airflow/       # DAG factory, config loader, seed
│   ├── common/        # Config centralisée, résolution de chemins
│   ├── db/            # Helpers de connexion SQL (Postgres, MySQL, MSSQL)
│   └── kafka/         # Helpers Kafka
├── plugins/           # Extensions Airflow (hooks, operators, sensors)
├── tests/             # Tests unitaires et d'intégration
├── deployment/        # Fichiers Docker Compose par environnement
├── monitoring/        # SLA et règles d'alerting
├── scripts/           # Scripts d'initialisation (bootstrap)
├── docs/              # Documentation technique
├── data/              # Données locales de test
├── include/           # Fichiers SQL, Jinja, macros
└── logs/              # Logs locaux (non versionné)
```

**Règle d'or :** les DAGs doivent rester **légers** (logique métier minimale). Toute logique réutilisable va dans `src/orchestration/`.

---

## 7. Outils nécessaires pour démarrer

| Outil | Rôle | Installation |
|-------|------|-------------|
| **Docker Desktop** | Exécution de la stack Airflow | https://www.docker.com/products/docker-desktop |
| **Python 3.11+** | Développement des DAGs | https://www.python.org/downloads/ |
| **Git** | Versioning du code | https://git-scm.com/ |
| **PyCharm / VS Code** | IDE | Choix personnel |
| **DBeaver / TablePlus** | Client SQL | Optionnel mais recommandé |
| **Postman / Insomnia** | Test d'APIs | Optionnel |
| **make** | Automatisation des commandes | Préinstallé sur Linux/Mac |

---

## 8. Prérequis techniques

- **Docker** >= 24.0 et **Docker Compose** >= 2.20
- **Python** >= 3.11
- **Git** >= 2.40
- 8 Go de RAM disponibles (recommandé : 16 Go pour les environnements Docker)
- Accès au dépôt Git du projet
- Accès VPN si nécessaire pour les environnements staging/prod

### Connaissances recommandées

- Bases Python (fonctions, classes, gestion d'exceptions)
- Notions SQL (SELECT, JOIN, INSERT, UPDATE)
- Bases Docker et Docker Compose
- Notions de base Git (branches, commits, pull requests)
- Connaissance minimale d'Apache Airflow (DAGs, tasks, operators)

---

## 9. Étapes d'installation locale

### Étape 1 — Cloner le repository

```bash
git clone <url-du-repo>
cd airflow-plateforme-template
```

### Étape 2 — Configurer les variables d'environnement

```bash
cp .env.example .env
```

Éditer `.env` avec les valeurs fournies par le référent technique (mots de passe, hôtes, etc.).

### Étape 3 — Créer le virtualenv Python

```bash
python3 -m venv .venv
source .venv/bin/activate       # Linux/macOS
# ou
.venv\Scripts\activate          # Windows

pip install -r requirements.txt
```

### Étape 4 — Initialiser Airflow

```bash
docker compose -f deployment/docker-compose.yml up -d airflow-init
```

Attendre la fin de l'initialisation (quelques minutes).

### Étape 5 — Démarrer la stack

```bash
docker compose -f deployment/docker-compose.yml up -d
```

### Étape 6 — Seeder les connexions et variables

```bash
docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py
```

### Étape 7 — Vérifier l'installation

Ouvrir `http://localhost:8080` dans un navigateur.
- Login : `admin` / `Admin@123` (modifiable dans `.env`)
- Les DAGs doivent apparaître dans l'interface

### Étape 8 — Lancer les tests

```bash
pytest tests/ -v
```

---

## 10. Accès nécessaires

Contacter le référent technique pour obtenir les accès suivants :

| Accès | Responsable | Délai |
|-------|-------------|-------|
| Dépôt Git (lecture/écriture) | Lead Data Engineer | J+1 |
| Airflow UI (env staging) | Administrateur Airflow | J+2 |
| Base de données staging | DBA / Data Engineer | J+2 |
| VPN entreprise | IT / DevOps | J+1 |
| Espace de stockage (S3/MinIO) | Data Engineer Senior | J+3 |
| Accès Kafka (staging) | Data Engineer Senior | J+3 |
| Dashboard monitoring | Data Engineer Senior | J+5 |

---

## 11. Commandes essentielles

### Docker Compose

```bash
# Démarrer la stack
docker compose -f deployment/docker-compose.yml up -d

# Arrêter la stack
docker compose -f deployment/docker-compose.yml down

# Voir les logs d'un service
docker compose -f deployment/docker-compose.yml logs -f airflow-scheduler

# Accéder au shell du scheduler
docker compose exec airflow-scheduler bash
```

### Airflow CLI

```bash
# Lister les DAGs
airflow dags list

# Déclencher un DAG
airflow dags trigger dag_bi_orangescrum_extract_tasks

# Voir le statut d'un DAG run
airflow dags list-runs -d dag_bi_orangescrum_extract_tasks

# Tester une task spécifique
airflow tasks test dag_bi_orangescrum_extract_tasks extract_tasks_from_source 2026-01-01

# Voir les connexions
airflow connections list

# Voir les variables
airflow variables list
```

### Python / Tests

```bash
# Lancer les tests
pytest tests/ -v

# Lancer les tests avec couverture
pytest tests/ --cov=src --cov-report=term-missing

# Vérifier le style de code
ruff check .
ruff format --check .
```

### Git

```bash
# Créer une branche de travail
git checkout -b feature/PROJET-123-ma-fonctionnalite

# Pousser la branche
git push -u origin feature/PROJET-123-ma-fonctionnalite

# Mettre à jour depuis main
git fetch origin && git rebase origin/main
```

---

## 12. Bonnes pratiques à respecter

### Code

- **Un DAG = un flux métier** clairement identifié
- Garder les DAGs **déclaratifs** — la logique complexe va dans `src/orchestration/`
- Utiliser le **DAG factory** pour les DAGs similaires
- Nommer les tâches selon la convention : `<action>_<objet>_<cible>`
- Toujours définir `default_args` avec `retries` et `retry_delay`
- Tester chaque nouveau DAG localement avant de pousser

### Sécurité

- Ne **jamais** committer de secrets (mots de passe, tokens, clés) dans le code
- Utiliser les **connexions Airflow** pour les credentials
- Utiliser les **variables Airflow** pour les paramètres sensibles
- Le fichier `.env` est local et **non versionné**

### Git

- Travailler toujours sur une **branche dédiée**, jamais directement sur `main`
- Commits atomiques et bien décrits : `feat(airflow): add extraction dag`
- Ouvrir une **Pull Request** avec description et tests avant de merger
- Reviewer le code des collègues

### Documentation

- Documenter tout nouveau DAG avec un docstring
- Mettre à jour le `CHANGELOG.md` lors de chaque release
- Signaler les problèmes rencontrés dans la documentation

---

## 13. Erreurs fréquentes à éviter

| Erreur | Impact | Solution |
|--------|--------|----------|
| Committer `.env` avec des secrets | Fuite de credentials | Vérifier `.gitignore` avant chaque commit |
| Mettre de la logique métier dans un DAG | Code non testable et non réutilisable | Déplacer dans `src/orchestration/` |
| Utiliser des imports relatifs dans les DAGs | `ImportError` en production | Utiliser `_bootstrap.py` et imports absolus |
| Modifier `main` directement | Régression en production | Toujours passer par une PR |
| Oublier `catchup=False` | Backfill indésirable | Définir `catchup=False` par défaut |
| Créer des dépendances circulaires entre DAGs | Deadlock | Utiliser `TriggerDagRunOperator` avec `wait_for_completion=False` |
| Hardcoder des connexions dans les DAGs | Non portable | Utiliser `BaseHook.get_connection()` |
| Négliger les retries | Instabilité en production | Toujours configurer `retries >= 1` |

---

## 14. Contacts et référents techniques

| Rôle | Nom | Contact |
|------|-----|---------|
| Lead Data Engineer / Auteur du template | Jean Mermoz Effi | jean.effi@synelia.tech |
| Administrateur Airflow | *(à compléter)* | *(à compléter)* |
| DBA / Administrateur base de données | *(à compléter)* | *(à compléter)* |
| DevOps / Infrastructure | *(à compléter)* | *(à compléter)* |
| Product Owner Data | *(à compléter)* | *(à compléter)* |

---

> En cas de question, consulte d'abord la documentation dans le dossier `docs/`, puis sollicite ton référent technique.
> **Bienvenue dans l'équipe !**

---

*Guide rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
