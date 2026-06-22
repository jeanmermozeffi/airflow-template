# Airflow Orchestration Template — SYNELIA Data Pole

[![CI](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAXYAAACHCAMAAAA1OYJfAAAAllBMVEX///9nWaKM0PRlV6FkVaBhUp/Lx91dTp2jmsiKgLdcTJ3i3+1fUJ6AdLFZR5uposp3aqu14PnAu9m6tNT4+PtwYajq6PLX1Obe2+r08/h0Zqzz+v61r9HRzeLn5fBsXqfGwduakcDn9v5+crGOhbqb1/imnsiSibzR7Pym2/jf8v3r9/7D5/yxqs5VQ5qx3/iCzfXJ6ftgxYSeAAAOrUlEQVR4nO1df3uqPA9W24ogKFNRcf5iOj1nZz7zfP8v98KcSpO04EPR6/Xp/deuCaXcpGmapGmjYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhUQ0jBOKi178vv39/vfy6e++eFh2Hy+i14SWvL60Tjq2/j+jiM6IjmjI8SPtH64rj++t9urWcLNrrdbu9mCyp4fd/D0S7A2j/dWxJqJv30WK+b/o5CB7sBt2an3pvFNH+pwVRZ2/G7WDl+7zZZJf+pH8x4fDIXYzrfPKdUUT7O2T9+FlbX6bJyuFNEoxxZ7Wf1Pboe6OA9g8k7K1WTcp2uRWCkZyfqeeOG9fz7LujgPYvgvZazJm4X0B6Bu7W8ehHoIB2gvXWSw3d6IZOEecpRL+GRz8EetpfKdrfzfdizRU6XYb3NMpdT/sbRbt5W2boFeqXDCx6FtVeQDs2H+ugfe6VIf2ZVPu/0e2/DXdhXUatZ/CfRrUX0Y7M9hSGDfcx7IES3tTskx+IAtr/YtaPb2Z7EJaaTTPw51mnFtBOmDKGdcygpGJPWd+bffIjUeQc+HuEwv7HbAcYbcRwLr7dYDnL8nms9hIeSLBOPRpeow6I+ZRxJwqS/mF+2O3dsOn4J+qfx2ovQTvg3bQjjNDsXASTqxYfLaeHzcpJBwV/Is97Me35afXddFxv2sOsz7BUjweh7z+P1V6K9sbr53s9pDcaBx+xvqGFeuo+kWovRXuKPx8fH3XElVyoY/jsiVSJGiVprwkxsh6dxR0f/zg8lvYlpF2lYp4Nj6V9gWh/oiWRDo+lHVntonPHp9MYTdu7bRAEbjKY3OaNGHUXw8TNbt321/p7jdIedyfrQ7I9PbkzX0yL/OMH+HSRlHnOr68XiC9qwv9El/0sOxZJR8K1n90k8Jx0ccxYukz2ov2irNKLF0nge+m6mqW3cuF40XaoZr6I9kMHIKHbGo3byTb0PMf/7nP2ZOF54fag/eiI9nIu9Q/osUhXz4Rxi6MF5zX20BN5eOdOTtyU8lx3GPfCdZkejfuRAyJkjPs8UflMi2jf+EJGj2qp2w9E2mHkXkkf7bua4TOHZjuLSo1rwh9NRHh/Ydp/HEoD+bne8vu/8d4j1szetrBLo51Dx9+Fk9ADvoj2ADbnoQSt0Tzo+ZjyC/NeoHSm4AhHuVAG4Y8mIryfyovAc0/enolDe/7FqsDPv1YnPTDBSamrTPt45/kFHnPe6yg0ZBd7fUUZh9cbpv2IlTseE2ePUlum3Z+n/xv8o+o/EzreR3vs4Mjf7FGCVJX2bq84vSV9r5Aea2PcYybKLJh+l1HuSh3TWMi0ZxP5XEMe89V6ZhwVRcc8wiiuSvu4VMy/yVck76MNHijMKaFnYEJsiwi/EBlt558m8ijj28ZaG21hkaoj3VVxcMzDVnFlJYO8KjQ4nWzRJ0SFOWFh0PQVKxCUA44/zcVrPQW075cFIuvvFKxHZV7fGRqnfV4yAi0CqttTUsi4v18W8I6zBFGMFyuij/NP0CkRFgZ0e6SaiWflhM6Bdkhl2olZkYaPPnmGkFRSTLBEn9JOmO4wAqPWMY0x7HShqhSk1yIoKXM8ADZFZdpHNHHUsyk1o9SpQuy19gPWMkC54+j79bsg2ovhEeKOowXNLATMcXzYAzJX3W5PpBayFSqjRx7tb1F+tSyvuq1emmOjHGw0war9omMasdbmI+EfUBe6qBV/5fYHwyRAKZ0MmBTVab8sPZhw/GgTpNisyJWHR4k7Eda7IF2aD1XEEyt/Wbmj75JbUGloTyWV1nsz1AVohnF+doVMt3AcgK9WnfbYyS7hPpvtFme323ixY5h4Z0Dxt9Pl4jGHkVNCg5oxX/S/f11/Gylo5z5fRUyQGhtNqlA/8iinFQ/gRyavXAw4BwLeFE44AL2KE0SnwvotmJec5pq8DWXwAP8ASvC56hgF7ayXfC/VuvMeoSa/l7J5zGRq2EpioAPkXd4BaYD2g/dPhzI7Doj3Hr1mKrDCmBdSzRMJa/nMqTf0UXKqn6SdX30v8QaLAgc+aSjsDvBqgLeSfasGaJ+qdhQlsG3ahkyt3wI7jBPzGWW65/0DaEb9yt9L0C7NejGe6VkoTzOBTCsyGBZA3KXBYIB2JZbQkFLFjsYBtsRArwL8abHpnmf2Bf74kb+X8AXJLzZFLlXmS10AKy7CbQMakILzddKOHAd8o7gw3hcZ0nyFn4uk/V3zo+w7wLTDDMst0nyetHLeycQRYTEQTJCUVK2070HXmae0ww9FjkzGkEcYCXQrZ0LCoSAvpghpB8MJRddlZqATz8N+U6BleJj7rVbad7Bxaqn3g0no64nnHPL+hoyVq3KHGugo6RhMO5LWcQS7I81M0CdCJGiOgdBFuUtqoX0Ux+PuZLFAA1WXtDvaiQKLZgX9Y0jaryKNFkvyEhbRjiP3qPPSvD4EokzFgMGHywdwTNMeLwedfRCJnud5eCe7s9bd23X1As+BLdH4RJPq5Sdo5nzJd0LaGUOvhe2wvPYH+pPMvQfWUP7ljdLePey9LN2BKfYKKNapV0xCIoice3Hg9cam+0WVwB9A7AnRPkOWEtKQeWpjoNpJ03gjs+DnXt4c7eP+zCtQEyrDPYeJq4sSQiWF3JBnJyPy2IAYCKI9bEAMoVGbp30MBSvCmGnuN0X7QkvXGcW0ZzFxR6lroAWKTPezCQkXSzDiV4L2gY52HGVgGLDzuVnbDO3djaoiiYwytGcZIJFqW7Yv22lYy/z8AGxLFN+uSnvpvbQ55KddI7TvypFelvaU+HXokMRDiwHOnD92IgyCoGyOqrRTMeAi5IeqAdrHCooIlKU9k/gV+WZMNiJRdsBJucNRgLIKqtKelN5Me4VZ2qfN8l0oT3v6NffUQIb2NZpUya+BthdWpR2uv8vAKO0TZRoegVtoJ0OVKAEeLotO6gSqdpQx9gjaWS53oirty5WCddYkrPfbaE8nLqJx+RJkKX5PnkDl443jD5F2c1MqldaVPcF32GoWhvD/N9JOhubBJTB2l0X2YPIS3sL8CNqFOQOyT+hfIVb74aIbZ/ELcPettDfQWGICeNNQNnUD+ciIrNRH0J736VSjfUnlgyeXpWR12ts4ExssVKHRknEMF0u43UdYMuacA1tk5Hn5CF912mOkZXzo2IZFEX6hfxFlEozb7VwUwe+tr/dXoh2n+3pS0LM67Q1kJ/moBjEQ7Rek74maIKZXqdw99AuRW3JUoh1ttPPlYKkB2lfwwyJph/PnO/oP0axpn8ytRVcq0Q5/ZCu5cQO0o/GEd1ZA0x3malBFQarSPgaj8Nb9tFVoH6FAAHCnV6d9hPaU4QRWwPLxA+xsoopsVaUdJ3Tctnu8Cu1dcC/jQBKr0z5BUyrDFwFV/glmVKrdqrSjkB8qe69HFdrbsGNAx+Aw8M20o4AmkQEK9+29y6qdLDxUmXa4tfPGGpVVaIczKspxXGqD76dLDrrROcGpzER+2CveYpDDB77BAO0oyfo2ca9CO/ziiPYusnQQ7YOef1BukSd2iiBDJgNVd/si+2TLlWkfBTAHqHnLiQpVaMceE7BwL5EEmV4ieps1mT+zJpKIVtTgIPbt6XVMddpxhJsV7RrOw6S0Q1EcY6Mb0T47JceL8LCQhX603hAhVUUWpUba6WKh1WnH+3A4K8iLyMGkbs/2d+bhItcBon107jz3/Sh0D4vJCYcgovZ2M06PZJyWp9cxBminvGH+hky/mq5DqPiNWjJyjjdqmqBdcnUx7jvO9+kzjqKGAb1fjkjLu0BRttIA7dTuH+5s+tPxRQ+O4uVkvt8Ix4HbWqvQPkYpGoxdM/OxrBO0o8omeuDcrR8opV1Ro9UA7Y091XcmvChw90mS7LduELJUgFIJQovYSs4BLJGM95eZo73bJyPQaCPKbX5rdfkNnJZ3gqogsQnacX25n7bY+cS2S3wNpRFWoh1ZKlnnnHDrzjxFfRVAe3yTsKNNtVfQ9f1RDp5R2hvD8rtbYapzJdrhPpGfV+DKqLYAix1UGU8HxjQ74qlC8y31mWhGaCdsBhVg+nu1MEfJLfcXwDUmejVt13V2MW26Kw/NMUP7qFShh+97wYtXo52qR619Oug5paUUYD1tfR+i5Iauwr8Z2htxibIm34BzasUQNp04kHsd0HPZkBqh1pUQCov9AspBoK7wb4j2RhyWExy+Mbr5vaulnUXALQ22qowL9s3knlp4rBtRtEdTa94U7Y14W2p6ggkPVdOTdLM5513QPHCPEk4XEoJBw5MAoWXUxaCN0d5ozEsd1QVoq5yMd1AeVcXZBObmgy21y71ftBEhExShqD0pA2sZzSl0BmlvdF3tFpQf2uSbq6eezhW8i6zyAbQQ4TbEZSf0iPqRV3AnSooqKZ2AK51oThExSXtqSAeKdcpP49xpurIFaSDRuh1RKaLePmM4BtrbQSp6NOmkS2hOlL5J7X+nGcASEkogadcdmfMPOAFcELR74BrkWMlhkkTUwM1q6/j+zB1Ayen4oHEQJdkIeEQ5tijiDpc/dvqszenrxjP5frrkVneRhDPmZIVq2XelWiF8R0ThflFO0DOgjR1HMqz0g8AFwD62NrpGG5GMJ0m48p1Tkd/vYru+4/AodAdUpeM5bBuUKE3Q7xQTy91M/HztrGpTtL000te2nke3PZjv9kEYhhs3OQzWk9vOLcS+35tuN4J4sR7uttkrBG6yG67b09qr0U8GSZiVHtokg0ccOPCGhN38QSIWCNh6fHSP/gvAFayssNePF+wIe3SX/gMgWLfCXjdevzDrNZzXbSHhg3L56mx2i+p4JXM1TJ8bbSGDKDmbwfCZrhZ5vL0c6YQB06eLWlxBxDXsfFo//igzwayKqROKHA3rjKkX9HR61MSULAwAHyKWwdqOdYNS7nY6rR2Ucq/j5GgLCfhkPXT2koV5oM0EVq/fBcCGsYvT+0B2g1mv452Qy662on4/XHJjjl/WIXBHfJuQx5Yl/b74PB5bv39Zq/HOePt8s5xbWFhYWPyn8T8EGxfiFLIVegAAAABJRU5ErkJggg==)](https://synelia.tech/)
[![Security](https://github.com/)](https://veza.com/blog/how-to-secure-access-to-your-source-code-in-github/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8%2B-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![Docker](https://img.shields.io/badge/Docker-24%2B-2496ED?logo=docker&logoColor=white)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![LinkedIn](https://custom-icon-badges.demolab.com/badge/LinkedIn-0A66C2?logo=linkedin-white&logoColor=fff)](https://ci.linkedin.com/company/syneliaofficiel)

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Navigation rapide

| Je veux… | Lire… |
|----------|-------|
| Rejoindre le projet (nouveau collaborateur) | [ONBOARDING.md](ONBOARDING.md) |
| Comprendre l'architecture technique | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Contribuer (branches, commits, PR) | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Maîtriser Airflow (operators, sensors, SLA…) | [docs/AIRFLOW_GUIDE.md](docs/AIRFLOW_GUIDE.md) |
| Consulter les conventions de nommage | [docs/NAMING_CONVENTIONS.md](docs/NAMING_CONVENTIONS.md) |
| Suivre le workflow Git du projet | [docs/GIT_WORKFLOW.md](docs/GIT_WORKFLOW.md) |
| Appliquer les bonnes pratiques Data Eng. | [docs/DATA_ENGINEERING_BEST_PRACTICES.md](docs/DATA_ENGINEERING_BEST_PRACTICES.md) |
| Déployer en dev / staging / prod | [docs/DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md) |
| Résoudre un problème courant | [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) |
| Voir l'historique des versions | [CHANGELOG.md](CHANGELOG.md) |

---

## Table des matières

1. [Contexte](#1-contexte)
2. [Objectifs](#2-objectifs)
3. [Architecture globale](#3-architecture-globale)
4. [Composants utilisés](#4-composants-utilisés)
5. [Structure du repository](#5-structure-du-repository)
6. [Environnements](#6-environnements)
7. [Installation](#7-installation)
8. [Configuration](#8-configuration)
9. [Commandes de démarrage](#9-commandes-de-démarrage)
10. [Tests](#10-tests)
11. [Déploiement](#11-déploiement)
12. [Liens utiles](#12-liens-utiles)
13. [Règles de contribution](#13-règles-de-contribution)

---

## 1. Contexte

Ce repository constitue le **template standard Apache Airflow** du pôle Data de SYNELIA. Il a été conçu pour servir de socle de référence pour tous les nouveaux projets d'orchestration Data, garantissant une structure homogène, une documentation claire et le respect des bonnes pratiques techniques.

Ce template est issu de l'expérience accumulée sur les projets Data du pôle (`cic-bi-airflow`, etc.) et a été industrialisé pour accélérer l'intégration des nouveaux collaborateurs et la mise en production des pipelines.

---

## 2. Objectifs

- Fournir une structure projet prête à l'emploi pour Apache Airflow
- Centraliser la configuration (connexions, variables, planification)
- Proposer des composants réutilisables (DAG factory, helpers DB/Kafka)
- Garantir la maintenabilité et la testabilité des pipelines
- Standardiser les conventions de nommage, de développement et de déploiement
- Faciliter l'intégration des nouveaux collaborateurs

---

## 3. Architecture globale

```
                        ┌─────────────────────────────────┐
                        │         Apache Airflow           │
                        │  ┌──────────┐  ┌─────────────┐  │
                        │  │ Scheduler│  │   Web UI    │  │
                        │  └──────────┘  └─────────────┘  │
                        │  ┌──────────┐  ┌─────────────┐  │
                        │  │  Worker  │  │   Metadata  │  │
                        │  └──────────┘  │   Database  │  │
                        │                └─────────────┘  │
                        └────────────┬────────────────────┘
                                     │ orchestrate
              ┌──────────────────────┼──────────────────────┐
              ▼                      ▼                       ▼
     ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐
     │  Source Systems  │  │  Data Warehouse  │  │   Notification   │
     │  (PostgreSQL,    │  │  (DWH / Marts)   │  │  (Email, Slack)  │
     │  MySQL, Kafka…)  │  └──────────────────┘  └──────────────────┘
     └─────────────────┘
```

Les DAGs orchestrent les flux : **extraction** → **transformation** → **chargement** → **validation** → **notification**.

---

## 4. Composants utilisés

| Composant | Version | Rôle |
|-----------|---------|------|
| Apache Airflow | 2.8+ | Orchestrateur de pipelines |
| PostgreSQL | 14+ | Base metadata Airflow + DWH |
| Redis | 7+ | Broker Celery (mode distribué) |
| Docker / Docker Compose | 24+ | Environnement d'exécution |
| Python | 3.11+ | Langage des DAGs et opérateurs |
| Kafka | 3+ | Messaging / streaming (optionnel) |

---

## 5. Structure du repository

```
airflow-template/
├── .env.example                   # Template de variables d'environnement
├── .env.dev / .env.staging / .env.prod
├── README.md                      # Ce fichier
├── ONBOARDING.md                  # Guide d'intégration nouveaux collaborateurs
├── ARCHITECTURE.md                # Documentation architecture détaillée
├── CONTRIBUTING.md                # Règles de contribution
├── CHANGELOG.md                   # Historique des versions
├── LICENSE
│
├── config/
│   ├── airflow.cfg                # Configuration Airflow de référence
│   ├── connections.yaml           # Connexions Airflow (bootstrap automatique)
│   ├── variables.yaml             # Variables Airflow (bootstrap automatique)
│   └── pipelines.yaml             # Planning et paramètres des DAGs
│
├── dags/
│   ├── _bootstrap.py              # Résolution dynamique des chemins
│   ├── ingestion_dag.py           # DAG d'ingestion
│   ├── transformation_dag.py      # DAG de transformation
│   ├── data_quality_dag.py        # DAG de contrôle qualité
│   ├── data_cleaning_dag.py       # DAG de nettoyage
│   ├── validation_dag.py          # DAG de validation
│   └── orchestration/             # DAGs d'orchestration complexe
│
├── src/orchestration/
│   ├── airflow/
│   │   ├── dag_factory.py         # Factory de génération de DAGs
│   │   ├── config_loader.py       # Chargeur de configuration pipelines
│   │   └── seed_airflow.py        # Bootstrap connexions et variables
│   ├── common/
│   │   ├── config.py              # Config centralisée multi-sources
│   │   └── env_paths.py           # Résolution des chemins d'environnement
│   ├── db/
│   │   ├── connection_factory.py  # Factory de connexions SQL
│   │   ├── postgres_connection.py
│   │   ├── mysql_connection.py
│   │   └── mssql_connection.py
│   └── kafka/
│       └── client_config.py       # Helper configuration Kafka
│
├── plugins/
│   ├── hooks/                     # Hooks Airflow custom
│   ├── operators/                 # Operators Airflow custom
│   └── sensors/                   # Sensors Airflow custom
│
├── tests/
│   ├── conftest.py
│   ├── test_dags.py
│   ├── test_config_loader.py
│   ├── test_kafka_config.py
│   ├── test_multi_sources_config.py
│   └── test_operators.py
│
├── deployment/
│   ├── docker-compose.yml         # Stack complète
│   ├── docker-compose.dev.yml
│   ├── docker-compose.staging.yml
│   └── docker-compose.prod.yml
│
├── monitoring/
│   ├── alerts.yaml                # Règles d'alerting
│   └── sla.yaml                   # Définition des SLA
│
├── scripts/
│   └── bootstrap_airflow.py       # Script d'initialisation Airflow
│
├── docs/
│   ├── AIRFLOW_GUIDE.md           # Guide technique Airflow
│   ├── GIT_WORKFLOW.md            # Workflow Git
│   ├── NAMING_CONVENTIONS.md      # Conventions de nommage
│   ├── DATA_ENGINEERING_BEST_PRACTICES.md
│   ├── DEPLOYMENT_GUIDE.md        # Guide de déploiement
│   └── TROUBLESHOOTING.md         # Résolution des problèmes courants
│
├── data/                          # Données locales de test
├── include/                       # Fichiers SQL, Jinja, macros
└── logs/                          # Logs locaux (gitignored)
```

---

## 6. Environnements

| Environnement | Fichier `.env` | URL Airflow | Base de données |
|---------------|----------------|-------------|-----------------|
| Développement | `.env.dev` | `http://localhost:8080` | PostgreSQL local |
| Staging | `.env.staging` | `http://airflow-staging:8080` | PostgreSQL staging |
| Production | `.env.prod` | `http://airflow-prod:8080` | PostgreSQL prod |

---

## 7. Installation

### Prérequis

- Docker >= 24.0 et Docker Compose >= 2.20
- Python >= 3.11
- Git >= 2.40

### Étapes

```bash
# 1. Cloner le repository
git clone <url-du-repo>
cd airflow-plateforme-template

# 2. Configurer l'environnement
cp .env.example .env
# Éditer .env avec vos valeurs

# 3. Créer le virtualenv Python (pour développement local)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. Initialiser Airflow
docker compose -f deployment/docker-compose.yml up -d airflow-init

# 5. Démarrer la stack complète
docker compose -f deployment/docker-compose.yml up -d

# 6. Seeder les connexions et variables
docker compose exec airflow-scheduler python scripts/bootstrap_airflow.py
```

---

## 8. Configuration

### Variables d'environnement

Le fichier `.env` doit contenir au minimum :

```bash
AIRFLOW_ENV=dev
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=Admin@123
```

### Multi-sources SQL

```bash
ORCH_DB__<SOURCE>__TYPE=postgres|mysql|mssql|oracle|sqlite
ORCH_DB__<SOURCE>__HOST=...
ORCH_DB__<SOURCE>__PORT=...
ORCH_DB__<SOURCE>__DB=...
ORCH_DB__<SOURCE>__USER=...
ORCH_DB__<SOURCE>__PASSWORD=...
```

### Multi-clusters Kafka

```bash
ORCH_KAFKA__<CLUSTER>__BROKERS=host1:9092,host2:9092
ORCH_KAFKA__<CLUSTER>__SECURITY_PROTOCOL=PLAINTEXT|SASL_SSL
```

Voir le fichier [`.env.example`](.env.example) pour la liste complète des variables.

---

## 9. Commandes de démarrage

```bash
# Démarrer toute la stack
docker compose -f deployment/docker-compose.yml up -d

# Arrêter la stack
docker compose -f deployment/docker-compose.yml down

# Voir les logs
docker compose -f deployment/docker-compose.yml logs -f airflow-scheduler

# Accéder au shell du scheduler
docker compose exec airflow-scheduler bash

# Lister les DAGs
docker compose exec airflow-scheduler airflow dags list

# Déclencher un DAG manuellement
docker compose exec airflow-scheduler airflow dags trigger dag_bi_orangescrum_extract_tasks
```

---

## 10. Tests

```bash
# Activer le virtualenv
source .venv/bin/activate

# Lancer tous les tests
pytest tests/ -v

# Lancer les tests d'un module
pytest tests/test_dags.py -v

# Vérifier la couverture
pytest tests/ --cov=src --cov-report=html

# Vérifier la qualité du code
ruff check .
ruff format --check .
```

---

## 11. Déploiement

Consulter le guide complet : [docs/DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md)

```bash
# Déploiement en dev
docker compose -f deployment/docker-compose.dev.yml up -d

# Déploiement en staging
docker compose -f deployment/docker-compose.staging.yml up -d

# Déploiement en production
docker compose -f deployment/docker-compose.prod.yml up -d
```

---

## 12. Liens utiles

| Ressource | URL |
|-----------|-----|
| UI Airflow (local) | http://localhost:8080 |
| Documentation Apache Airflow | https://airflow.apache.org/docs/ |
| Guide d'onboarding | [ONBOARDING.md](ONBOARDING.md) |
| Guide technique Airflow | [docs/AIRFLOW_GUIDE.md](docs/AIRFLOW_GUIDE.md) |
| Guide de déploiement | [docs/DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md) |
| Conventions de nommage | [docs/NAMING_CONVENTIONS.md](docs/NAMING_CONVENTIONS.md) |
| Résolution de problèmes | [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) |

---

## 13. Règles de contribution

Consulter le guide complet : [CONTRIBUTING.md](CONTRIBUTING.md)

**En résumé :**
- Travailler sur une branche dédiée : `feature/<PROJET>-<ticket>-<description>`
- Respecter les conventions de commit : `feat(scope): description`
- Ouvrir une Pull Request vers `main` avec au moins 1 reviewer
- Les tests et le lint doivent passer avant merge

---

*Template maintenu par le Pôle Data — SYNELIA | v1.0.0 | 08 mai 2026*
