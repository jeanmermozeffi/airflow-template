# Guide de contribution — Airflow Orchestration Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

Merci de contribuer à ce projet ! Ce guide définit les règles et conventions à respecter pour maintenir la qualité du code et faciliter la collaboration au sein du pôle Data SYNELIA.

---

## Table des matières

1. [Prérequis](#1-prérequis)
2. [Workflow de contribution](#2-workflow-de-contribution)
3. [Convention de branches](#3-convention-de-branches)
4. [Convention de commits](#4-convention-de-commits)
5. [Pull Requests](#5-pull-requests)
6. [Standards de code](#6-standards-de-code)
7. [Tests](#7-tests)
8. [Documentation](#8-documentation)
9. [Revue de code](#9-revue-de-code)

---

## 1. Prérequis

Avant de contribuer, assure-toi d'avoir :

- Suivi le [guide d'onboarding](ONBOARDING.md)
- Installé le projet localement et vérifié que les tests passent
- Accès en écriture au dépôt Git
- Pris connaissance des [conventions de nommage](docs/NAMING_CONVENTIONS.md)

---

## 2. Workflow de contribution

```
1. Créer une branche depuis main
        │
        ▼
2. Développer la fonctionnalité / corriger le bug
        │
        ▼
3. Écrire ou mettre à jour les tests
        │
        ▼
4. Vérifier le code (lint, format, tests)
        │
        ▼
5. Pousser la branche et ouvrir une Pull Request
        │
        ▼
6. Revue de code par au moins 1 reviewer
        │
        ▼
7. Merge vers main après approbation et CI verte
```

---

## 3. Convention de branches

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
| `test` | Ajout ou modification de tests |

### Exemples

```bash
feature/SYNELIA-19-dq-staging-vs-source-count-checks
fix/SYNELIA-24-correction-dag-refresh-dwh
hotfix/SYNELIA-31-fix-prod-airflow-scheduler
release/SYNELIA-v1.1.0
chore/SYNELIA-12-update-docker-compose
docs/SYNELIA-08-add-onboarding-guide
refactor/SYNELIA-45-split-extract-load-tasks
test/SYNELIA-52-add-dag-unit-tests
```

### Règles

- La description doit être en minuscules avec des tirets
- Inclure toujours le numéro de ticket si applicable
- Ne jamais travailler directement sur `main`

---

## 4. Convention de commits

### Format

```
<type>(<scope>): <description courte>

[corps optionnel — explication du pourquoi]

[pied de page optionnel — références tickets, breaking changes]
```

### Types autorisés

| Type | Usage |
|------|-------|
| `feat` | Nouvelle fonctionnalité |
| `fix` | Correction de bug |
| `docs` | Documentation |
| `style` | Formatage, espaces (pas de changement fonctionnel) |
| `refactor` | Refactoring |
| `test` | Ajout ou modification de tests |
| `chore` | Maintenance, dépendances |
| `ci` | CI/CD |
| `perf` | Amélioration des performances |

### Scopes courants

```
airflow    — DAGs, operators, hooks
dags       — fichiers de définition de DAGs
db         — helpers de connexion base de données
kafka      — helpers Kafka
config     — fichiers de configuration
docker     — Docker Compose, Dockerfile
docs       — documentation
tests      — fichiers de tests
ci         — GitHub Actions, pipelines CI
```

### Exemples

```bash
feat(airflow): add orangescrum extraction dag
fix(dq): correct staging vs source count check logic
docs(onboarding): add setup guide for new contributors
refactor(dags): split extract and load tasks into separate modules
chore(docker): update airflow image to 2.8.1
test(dag_factory): add unit tests for schedule parsing
ci(github-actions): add ruff lint step to pipeline
perf(db): add connection pooling to postgres factory
```

### Règles

- Description en anglais, minuscule, sans point final, moins de 72 caractères
- Utiliser l'impératif présent : "add", "fix", "update" (pas "added", "fixed")
- Le corps du commit explique le **pourquoi**, pas le quoi
- Référencer le ticket en pied de page : `Refs: SYNELIA-123`

---

## 5. Pull Requests

### Avant d'ouvrir une PR

```bash
# Vérifier que les tests passent
pytest tests/ -v

# Vérifier le lint
ruff check .
ruff format --check .

# Mettre à jour depuis main
git fetch origin
git rebase origin/main
```

### Template de description PR

```markdown
## Description
<!-- Brève description de ce que fait cette PR -->

## Motivation et contexte
<!-- Pourquoi ce changement est-il nécessaire ? -->
<!-- Ticket : SYNELIA-XXX -->

## Type de changement
- [ ] Nouvelle fonctionnalité
- [ ] Correction de bug
- [ ] Refactoring
- [ ] Documentation
- [ ] Infrastructure / Configuration

## Tests effectués
- [ ] Tests unitaires ajoutés / mis à jour
- [ ] Tests d'intégration validés
- [ ] DAG testé manuellement sur environnement local

## Checklist
- [ ] Code lint et formaté (`ruff check .`)
- [ ] Tous les tests passent (`pytest tests/ -v`)
- [ ] Documentation mise à jour si nécessaire
- [ ] CHANGELOG.md mis à jour
- [ ] Pas de secrets dans le code
- [ ] Revue effectuée sur sa propre PR avant soumission
```

### Règles

- **1 reviewer minimum** requis avant merge
- La CI doit être **verte** (lint + tests)
- Résoudre tous les commentaires de revue avant merge
- **Squash and merge** préféré pour les features
- **Merge commit** pour les releases
- Délai de review : 48h ouvrables maximum

---

## 6. Standards de code

### Python

- Suivre **PEP 8** (appliqué automatiquement par `ruff`)
- Type hints sur les fonctions publiques
- Docstrings sur les fonctions et classes publiques (format Google)
- Longueur de ligne max : **120 caractères**

```python
# Bon
def get_engine(source_name: str) -> Engine:
    """Retourne un engine SQLAlchemy pour la source spécifiée.

    Args:
        source_name: Nom de la source (ex : "DWH", "CRM").

    Returns:
        SQLAlchemy Engine configuré.

    Raises:
        ValueError: Si la source n'est pas configurée.
    """
    ...

# Mauvais
def get_engine(source):
    # retourne l'engine
    ...
```

### DAGs Airflow

```python
# Toujours définir default_args
default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@synelia.com"],
}

# catchup=False par défaut
with DAG(
    dag_id="dag_bi_source_extract_objects",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    tags=["ingestion", "bi"],
) as dag:
    ...
```

### YAML

- Indentation : 2 espaces
- Clés en minuscule avec underscores
- Valeurs sensibles via variables d'environnement

---

## 7. Tests

### Structure des tests

```
tests/
├── conftest.py              # Fixtures partagées
├── test_dags.py             # Validation syntaxique des DAGs
├── test_config_loader.py    # Tests du chargeur de configuration
├── test_kafka_config.py     # Tests de la config Kafka
├── test_multi_sources_config.py
└── test_operators.py        # Tests des operators custom
```

### Règles

- Tout nouveau code doit avoir des tests correspondants
- Coverage minimum : **80%** sur `src/orchestration/`
- Les tests ne doivent pas dépendre d'une infrastructure externe (mocker les connexions)
- Utiliser `pytest` et les fixtures `conftest.py`

```bash
# Lancer les tests
pytest tests/ -v

# Vérifier la couverture
pytest tests/ --cov=src --cov-report=term-missing --cov-fail-under=80
```

---

## 8. Documentation

- Mettre à jour le `CHANGELOG.md` pour chaque PR significative
- Documenter les nouveaux DAGs avec un `doc_md` ou docstring
- Mettre à jour `README.md` si la structure du projet change
- Tout nouveau composant doit être décrit dans le guide approprié (`docs/`)

---

## 9. Revue de code

### En tant que reviewer

- Vérifier la logique métier et la correction technique
- Contrôler le respect des conventions de nommage
- Vérifier l'absence de secrets ou de données sensibles
- Tester la PR localement si le changement est critique
- Approuver ou demander des modifications dans les **48h ouvrables**

### En tant qu'auteur

- Répondre à tous les commentaires avant de merger
- Ne pas dismisser les reviews sans accord du reviewer
- Préférer la discussion constructive aux changements non justifiés

---

*Guide rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
