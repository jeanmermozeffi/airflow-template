# Changelog — Airflow Orchestration Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026

Toutes les modifications notables de ce projet sont documentées dans ce fichier.

Le format suit [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/),
et ce projet adhère au [Versioning Sémantique](https://semver.org/lang/fr/).

---

## [Non publié]

### En cours
- Amélioration de la documentation des cas d'usage avancés

---

## [1.0.0] — 2026-05-08

> Auteur : Jean Mermoz Effi

### Ajouté
- Structure complète du template Airflow (dags, src, config, plugins, tests, deployment, monitoring, docs)
- DAG factory (`src/orchestration/airflow/dag_factory.py`) pour génération de DAGs depuis YAML
- Config loader (`src/orchestration/airflow/config_loader.py`) pour la lecture de `pipelines.yaml`
- Connection factory (`src/orchestration/db/connection_factory.py`) multi-sources SQL (PostgreSQL, MySQL, MSSQL, Oracle, SQLite)
- Helper Kafka (`src/orchestration/kafka/client_config.py`) multi-clusters
- Script de bootstrap idempotent (`scripts/bootstrap_airflow.py`) pour connexions et variables Airflow
- DAGs d'exemple : ingestion, transformation, data quality, data cleaning, validation
- Fichiers Docker Compose par environnement : dev, staging, prod
- Configuration centralisée : `config/connections.yaml`, `config/variables.yaml`, `config/pipelines.yaml`
- SLA et alerting : `monitoring/sla.yaml`, `monitoring/alerts.yaml`
- Tests unitaires : dags, config_loader, kafka_config, multi_sources_config, operators
- Documentation complète :
  - `README.md` — Documentation générale du projet
  - `ONBOARDING.md` — Guide d'intégration nouveaux collaborateurs
  - `ARCHITECTURE.md` — Architecture technique détaillée
  - `CONTRIBUTING.md` — Règles de contribution
  - `docs/AIRFLOW_GUIDE.md` — Guide technique Airflow
  - `docs/GIT_WORKFLOW.md` — Workflow Git
  - `docs/NAMING_CONVENTIONS.md` — Conventions de nommage
  - `docs/DATA_ENGINEERING_BEST_PRACTICES.md` — Bonnes pratiques Data Engineering
  - `docs/DEPLOYMENT_GUIDE.md` — Guide de déploiement
  - `docs/TROUBLESHOOTING.md` — Résolution des problèmes courants

### Infrastructure
- Support multi-sources SQL via `ORCH_DB__<SOURCE>__*`
- Support multi-clusters Kafka via `ORCH_KAFKA__<CLUSTER>__*`
- Bootstrap dynamique des chemins Python dans les DAGs (`dags/_bootstrap.py`)
- Gestion des environnements dev/staging/prod via fichiers `.env`

---

## Conventions de versioning

Ce projet utilise le [Versioning Sémantique](https://semver.org/lang/fr/) :

```
MAJEUR.MINEUR.PATCH
```

| Type de changement | Version incrémentée |
|--------------------|---------------------|
| Changement incompatible | MAJEUR (ex : 1.0.0 → 2.0.0) |
| Nouvelle fonctionnalité rétrocompatible | MINEUR (ex : 1.0.0 → 1.1.0) |
| Correction de bug rétrocompatible | PATCH (ex : 1.0.0 → 1.0.1) |

---

## Format d'entrée

```markdown
## [X.Y.Z] — AAAA-MM-JJ

> Auteur : Prénom Nom

### Ajouté
- Description de ce qui a été ajouté

### Modifié
- Description de ce qui a été modifié

### Déprécié
- Description de ce qui sera supprimé dans une prochaine version

### Supprimé
- Description de ce qui a été supprimé

### Corrigé
- Description des bugs corrigés

### Sécurité
- Description des correctifs de sécurité
```

---

*Changelog maintenu par le Pôle Data — SYNELIA | Initié par Jean Mermoz Effi | 08 mai 2026*
