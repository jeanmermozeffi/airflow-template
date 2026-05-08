# Architecture — Airflow Orchestration Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Composants Airflow](#2-composants-airflow)
3. [Architecture des données](#3-architecture-des-données)
4. [Architecture applicative](#4-architecture-applicative)
5. [Infrastructure Docker](#5-infrastructure-docker)
6. [Gestion des connexions](#6-gestion-des-connexions)
7. [Gestion des configurations](#7-gestion-des-configurations)
8. [Sécurité](#8-sécurité)
9. [Monitoring et observabilité](#9-monitoring-et-observabilité)
10. [Décisions architecturales](#10-décisions-architecturales)

---

## 1. Vue d'ensemble

Ce template implémente une architecture **modulaire et extensible** pour l'orchestration de pipelines Data avec Apache Airflow.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SYNELIA Data Platform                        │
│                                                                      │
│  Sources             Orchestration             Destinations          │
│  ───────             ─────────────             ────────────          │
│                                                                      │
│  PostgreSQL ──►┐                          ┌──► Landing Zone          │
│  MySQL      ──►│  ┌────────────────────┐  │                          │
│  MSSQL      ──►├─►│  Apache Airflow    ├──┼──► Data Warehouse        │
│  Oracle     ──►│  │  (Orchestrateur)   │  │                          │
│  Kafka      ──►│  └────────────────────┘  ├──► Data Marts            │
│  APIs REST  ──►│          │               │                          │
│  CSV/JSON   ──►┘          │               └──► Notifications         │
│                           │                                          │
│                    ┌──────▼───────┐                                  │
│                    │  Metadata DB │                                  │
│                    │ (PostgreSQL) │                                  │
│                    └──────────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Composants Airflow

### 2.1 Architecture Airflow (mode CeleryExecutor)

```
┌─────────────────────────────────────────────┐
│              Airflow Cluster                 │
│                                              │
│  ┌──────────────┐    ┌──────────────────┐   │
│  │  Web Server  │    │    Scheduler     │   │
│  │  (UI / API)  │    │ (Planification   │   │
│  └──────────────┘    │  des tâches)     │   │
│                      └──────┬───────────┘   │
│  ┌──────────────┐           │               │
│  │ Metadata DB  │◄──────────┘               │
│  │ (PostgreSQL) │                           │
│  └──────────────┘                           │
│                      ┌──────────────────┐   │
│  ┌──────────────┐    │     Worker 1     │   │
│  │    Redis     │◄──►│  (Celery)        │   │
│  │  (Broker)    │    └──────────────────┘   │
│  └──────────────┘    ┌──────────────────┐   │
│                      │     Worker 2     │   │
│                      │  (Celery)        │   │
│                      └──────────────────┘   │
└─────────────────────────────────────────────┘
```

### 2.2 Rôles des composants

| Composant | Rôle |
|-----------|------|
| **Web Server** | Interface utilisateur et API REST Airflow |
| **Scheduler** | Planification et soumission des tâches à la queue |
| **Worker** | Exécution effective des tâches (Celery) |
| **Metadata DB** | Stockage de l'état des DAGs, tasks, logs |
| **Redis** | Broker de messages entre Scheduler et Workers |
| **Flower** | Monitoring du cluster Celery (optionnel) |

---

## 3. Architecture des données

### 3.1 Flux de données standard

```
Source System
    │
    ▼
[Extraction Task]      ─── Lit depuis la source
    │                      via connection_factory.py
    ▼
Landing Zone (Staging)
    │
    ▼
[Transformation Task]  ─── Applique les règles métier
    │                      et nettoyage
    ▼
Data Warehouse
    │
    ▼
[Data Quality Task]    ─── Contrôle de volume, doublons,
    │                      nulls, cohérence
    ▼
[Notification Task]    ─── Envoi d'alertes si anomalie
```

### 3.2 Zones de données

| Zone | Description | Rétention |
|------|-------------|-----------|
| **Landing Zone** | Données brutes telles que reçues de la source | 7 jours |
| **Staging** | Données nettoyées et typées | 30 jours |
| **Data Warehouse** | Tables historisées (faits et dimensions) | Permanent |
| **Data Marts** | Agrégats et KPIs pour la BI | Permanent |

---

## 4. Architecture applicative

### 4.1 Couches du template

```
┌─────────────────────────────────────────────────┐
│                    DAGs Layer                    │
│         (dags/  — déclaratif, léger)             │
├─────────────────────────────────────────────────┤
│                  Plugins Layer                   │
│  (plugins/  — hooks, operators, sensors custom)  │
├─────────────────────────────────────────────────┤
│               Orchestration Library              │
│    (src/orchestration/ — logique réutilisable)   │
│                                                  │
│   ┌───────────┐ ┌──────────┐ ┌───────────────┐  │
│   │  airflow/ │ │   db/    │ │    kafka/     │  │
│   │ factory   │ │ helpers  │ │    helpers    │  │
│   └───────────┘ └──────────┘ └───────────────┘  │
│             ┌───────────────────┐               │
│             │    common/        │               │
│             │  config, env_path │               │
│             └───────────────────┘               │
├─────────────────────────────────────────────────┤
│                 Config Layer                     │
│    (config/ — YAML + variables d'env)            │
└─────────────────────────────────────────────────┘
```

### 4.2 DAG Factory

Le `dag_factory.py` permet de générer des DAGs à partir d'une configuration YAML (`config/pipelines.yaml`), évitant la duplication de code :

```python
# config/pipelines.yaml
pipelines:
  - dag_id: dag_bi_orangescrum_extract_tasks
    schedule: "0 6 * * *"
    source: orangescrum
    tasks:
      - extract_tasks_from_source
      - load_tasks_to_staging
```

### 4.3 Connection Factory

La `connection_factory.py` fournit des connexions SQL unifiées depuis les variables d'environnement :

```python
# Support multi-sources via ORCH_DB__<SOURCE>__*
engine = get_engine("DWH")    # PostgreSQL DWH
engine = get_engine("CRM")    # MySQL CRM
engine = get_engine("RAW")    # PostgreSQL Landing
```

---

## 5. Infrastructure Docker

### 5.1 Services Docker Compose

| Service | Image | Port | Rôle |
|---------|-------|------|------|
| `airflow-webserver` | apache/airflow:2.8 | 8080 | Interface Web |
| `airflow-scheduler` | apache/airflow:2.8 | — | Planificateur |
| `airflow-worker` | apache/airflow:2.8 | — | Exécuteur Celery |
| `airflow-init` | apache/airflow:2.8 | — | Initialisation |
| `postgres` | postgres:14 | 5432 | Metadata DB + DWH |
| `redis` | redis:7 | 6379 | Broker Celery |
| `flower` | apache/airflow:2.8 | 5555 | Monitoring Celery |

### 5.2 Volumes persistants

```
./dags         → /opt/airflow/dags
./logs         → /opt/airflow/logs
./plugins      → /opt/airflow/plugins
./config       → /opt/airflow/config
./src          → /opt/airflow/src
```

---

## 6. Gestion des connexions

Les connexions sont définies dans `config/connections.yaml` et bootstrappées automatiquement :

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

Le script `scripts/bootstrap_airflow.py` applique ces connexions en mode **idempotent** (création ou mise à jour sans doublons).

---

## 7. Gestion des configurations

### Variables d'environnement (`.env`)

Structure hiérarchique :
- `.env.example` — template de référence (versionné)
- `.env` — valeurs locales (non versionné)
- `.env.dev` / `.env.staging` / `.env.prod` — par environnement

### Variables Airflow (`config/variables.yaml`)

Convention de nommage : `<PROJET>__<ENV>__<NOM_VARIABLE>`

```yaml
variables:
  - key: SYNELIA__DEV__DWH_SCHEMA
    value: public
  - key: SYNELIA__PROD__ALERT_EMAIL
    value: data-team@synelia.com
```

---

## 8. Sécurité

| Principe | Implémentation |
|----------|---------------|
| **Pas de secrets dans le code** | Variables d'env + Connexions Airflow |
| **Chiffrement des métadonnées** | `AIRFLOW__CORE__FERNET_KEY` |
| **RBAC** | Rôles Airflow (Admin, Op, User, Viewer) |
| **TLS** | Nginx reverse proxy avec HTTPS (prod) |
| **Réseau isolé** | Network Docker dédié |
| **Scan de secrets** | Pre-commit hook `detect-secrets` |

---

## 9. Monitoring et observabilité

### Logs

- Logs Airflow centralisés dans `/opt/airflow/logs/`
- Format structuré JSON recommandé
- Rotation des logs configurée dans `airflow.cfg`

### Alerting (`monitoring/alerts.yaml`)

- Alertes par email sur échec de DAG
- Alertes Slack sur SLA breach
- Webhooks configurables

### SLA (`monitoring/sla.yaml`)

- Définition des délais maximaux par DAG
- Notification automatique sur dépassement

### Métriques (optionnel)

- Export vers Prometheus via `airflow-exporter`
- Dashboard Grafana pré-configuré

---

## 10. Décisions architecturales

### ADR-001 : CeleryExecutor vs LocalExecutor

**Décision :** CeleryExecutor en production, LocalExecutor en développement.

**Raison :** Le CeleryExecutor permet la scalabilité horizontale et la tolérance aux pannes. Le LocalExecutor simplifie le développement local sans Redis.

### ADR-002 : Configuration via YAML + variables d'env

**Décision :** Externalisation totale de la configuration dans des fichiers YAML et des variables d'environnement.

**Raison :** Portabilité entre environnements sans modification du code. Séparation claire entre code et configuration.

### ADR-003 : Librairie `src/orchestration/` partagée

**Décision :** Toute la logique réutilisable est dans un package Python installable (`src/orchestration/`), pas dans les DAGs.

**Raison :** Testabilité, réutilisabilité, séparation des responsabilités.

### ADR-004 : Bootstrap idempotent

**Décision :** Le script `bootstrap_airflow.py` est conçu pour être exécuté plusieurs fois sans effet de bord.

**Raison :** Fiabilité lors des déploiements répétés et des redémarrages.

---

*Document rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
