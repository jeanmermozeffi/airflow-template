# Git Workflow — Airflow Orchestration Template

> **Rédigé par :** Jean Mermoz Effi
> **Date :** 08 mai 2026
> **Version :** 1.0.0

---

## Table des matières

1. [Stratégie de branches](#1-stratégie-de-branches)
2. [Règles de commit](#2-règles-de-commit)
3. [Règles de Pull Request / Merge Request](#3-règles-de-pull-request--merge-request)
4. [Règles de revue de code](#4-règles-de-revue-de-code)
5. [Règles de merge](#5-règles-de-merge)
6. [Gestion des conflits](#6-gestion-des-conflits)
7. [Stratégie de release](#7-stratégie-de-release)
8. [Règles de versioning](#8-règles-de-versioning)

---

## 1. Stratégie de branches

### 1.1 Modèle de branches

Ce projet utilise un modèle de branches simplifié inspiré de **GitHub Flow** :

```
main
  │
  ├── feature/SYNELIA-42-add-kafka-ingestion-dag
  │
  ├── fix/SYNELIA-51-correct-retry-logic
  │
  ├── hotfix/SYNELIA-67-fix-prod-scheduler-crash
  │
  └── release/v1.2.0
```

### 1.2 Branches permanentes

| Branche | Description | Protection |
|---------|-------------|-----------|
| `main` | Code en production — toujours stable | Push direct interdit, PR obligatoire |

### 1.3 Branches temporaires

| Type | Format | Usage | Durée de vie |
|------|--------|-------|--------------|
| `feature` | `feature/<PROJET>-<ticket>-<description>` | Nouvelle fonctionnalité | Jusqu'au merge |
| `fix` | `fix/<PROJET>-<ticket>-<description>` | Correction de bug non urgent | Jusqu'au merge |
| `hotfix` | `hotfix/<PROJET>-<ticket>-<description>` | Correction urgente en production | Jusqu'au merge (prioritaire) |
| `release` | `release/<PROJET>-v<X.Y.Z>` | Préparation d'une version | Jusqu'à la release |
| `chore` | `chore/<PROJET>-<ticket>-<description>` | Maintenance, dépendances, CI | Jusqu'au merge |
| `docs` | `docs/<PROJET>-<ticket>-<description>` | Documentation uniquement | Jusqu'au merge |
| `refactor` | `refactor/<PROJET>-<ticket>-<description>` | Refactoring | Jusqu'au merge |
| `test` | `test/<PROJET>-<ticket>-<description>` | Tests uniquement | Jusqu'au merge |

### 1.4 Règles de nommage des branches

```
<type>/<NOM_PROJET>-<numero_ticket>-<description-en-minuscules-avec-tirets>
```

**Exemples valides :**
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

**Exemples invalides :**
```bash
jean/my-feature          # Pas de nom personnel
new-dag                  # Pas de type ni de ticket
feature/add dag          # Pas d'espaces
FEATURE/SYNELIA-19       # Pas de majuscules
```

### 1.5 Cycle de vie d'une branche

```
1. Créer depuis main (toujours à jour)
   git checkout main && git pull
   git checkout -b feature/SYNELIA-42-add-kafka-ingestion-dag

2. Développer et committer
   git add src/orchestration/kafka/...
   git commit -m "feat(kafka): add kafka ingestion helper"

3. Maintenir à jour avec main (rebase recommandé)
   git fetch origin
   git rebase origin/main

4. Pousser et ouvrir une PR
   git push -u origin feature/SYNELIA-42-add-kafka-ingestion-dag

5. Après merge, supprimer la branche
   git branch -d feature/SYNELIA-42-add-kafka-ingestion-dag
   git push origin --delete feature/SYNELIA-42-add-kafka-ingestion-dag
```

---

## 2. Règles de commit

### 2.1 Format du message de commit

```
<type>(<scope>): <description courte>

[corps optionnel]

[pied de page optionnel]
```

### 2.2 Types de commits autorisés

| Type | Usage | Exemple |
|------|-------|---------|
| `feat` | Nouvelle fonctionnalité | `feat(airflow): add orangescrum extraction dag` |
| `fix` | Correction de bug | `fix(dq): correct staging vs source count check` |
| `docs` | Documentation | `docs(onboarding): add setup guide` |
| `style` | Formatage (sans changement fonctionnel) | `style(dags): fix indentation` |
| `refactor` | Refactoring | `refactor(dags): split extract and load tasks` |
| `test` | Tests | `test(dag_factory): add schedule parsing tests` |
| `chore` | Maintenance | `chore(docker): update airflow image to 2.8.1` |
| `ci` | CI/CD | `ci(github-actions): add lint step` |
| `perf` | Performance | `perf(db): add connection pooling` |

### 2.3 Scopes courants

```
airflow, dags, db, kafka, config, docker, docs, tests, ci, plugins, monitoring
```

### 2.4 Règles de rédaction

- **Description :** impératif présent, minuscule, sans point final, < 72 caractères
  - Bon : `feat(airflow): add extraction dag`
  - Mauvais : `Added the new extraction DAG.`

- **Corps (optionnel) :** explication du POURQUOI (pas du quoi)
  ```
  feat(db): add connection pooling to postgres factory

  Les tests de charge ont montré que les connexions éphémères surchargent
  le serveur PostgreSQL en production au-delà de 50 DAGs simultanés.
  SQLAlchemy connection pooling réduit le nombre de connexions de ~70%.

  Refs: SYNELIA-89
  ```

- **Pied de page :** références tickets, breaking changes
  ```
  Refs: SYNELIA-123
  Closes: SYNELIA-124
  BREAKING CHANGE: la variable ORCH_DB__HOST est renommée ORCH_DB__<SOURCE>__HOST
  ```

### 2.5 Commits atomiques

- Un commit = une modification logique cohérente
- Ne pas mélanger plusieurs fonctionnalités dans un seul commit
- Éviter les commits "WIP" ou "fix" sans description

### 2.6 Pre-commit hooks recommandés

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.0
    hooks:
      - id: ruff
      - id: ruff-format
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.4.0
    hooks:
      - id: detect-secrets
  - repo: https://github.com/commitizen-tools/commitizen
    rev: v3.0.0
    hooks:
      - id: commitizen
```

---

## 3. Règles de Pull Request / Merge Request

### 3.1 Quand ouvrir une PR

- Dès que la fonctionnalité/correction est prête à être relue
- La CI doit être **verte** avant de demander une review
- La branche doit être à jour avec `main`

### 3.2 Checklist avant ouverture

```markdown
- [ ] Tests écrits et passants (`pytest tests/ -v`)
- [ ] Lint passant (`ruff check .`)
- [ ] Branche rebasée sur main (`git rebase origin/main`)
- [ ] Pas de fichiers secrets dans les changements
- [ ] Description de la PR complète
- [ ] Reviewer(s) assigné(s)
- [ ] Label approprié ajouté (feature, fix, docs, etc.)
- [ ] Ticket lié dans la description
```

### 3.3 Template de description PR

```markdown
## Description
Brève description de ce que fait cette PR.

## Motivation
Pourquoi ce changement est nécessaire ? Ticket : SYNELIA-XXX

## Type de changement
- [ ] feat — Nouvelle fonctionnalité
- [ ] fix — Correction de bug
- [ ] refactor — Refactoring
- [ ] docs — Documentation
- [ ] chore — Maintenance

## Tests
- [ ] Tests unitaires ajoutés/mis à jour
- [ ] DAG testé manuellement en local
- [ ] Comportement en cas d'erreur validé

## Captures d'écran (si applicable)
```

### 3.4 Taille recommandée des PRs

| Taille | Lignes modifiées | Durée de review estimée |
|--------|-----------------|------------------------|
| XS | < 50 | < 30 min |
| S | 50–200 | < 1h |
| M | 200–500 | 1–2h |
| L | 500–1000 | 2–4h |
| XL | > 1000 | > 4h — à découper |

**Recommandation :** viser des PRs de taille S à M. Les PRs XL sont difficiles à reviewer efficacement.

---

## 4. Règles de revue de code

### 4.1 Pour le reviewer

- Délai de review : **48h ouvrables** maximum
- Approuver ou demander des modifications (pas de "looks good" sans approbation formelle)
- Se concentrer sur :
  - Correctness (logique métier correcte ?)
  - Security (secrets, injections SQL ?)
  - Performance (requêtes, connexions ?)
  - Conventions (nommage, structure ?)
  - Tests (couverture suffisante ?)
- Formuler des commentaires constructifs et précis
- Distinguer les suggestions des blockers : `[suggestion]` vs `[blocker]`

### 4.2 Pour l'auteur

- Répondre à **tous** les commentaires (même pour accepter sans changer)
- Ne pas dismisser les reviews sans accord
- Notifier le reviewer une fois les modifications faites

### 4.3 Règle du pouce

- **1 reviewer minimum** pour les features et fixes
- **2 reviewers minimum** pour les changements d'infrastructure ou de sécurité
- **Lead Data Engineer** requis pour les changes `main` critiques

---

## 5. Règles de merge

### 5.1 Stratégies de merge

| Stratégie | Quand l'utiliser |
|-----------|-----------------|
| **Squash and merge** | Features et fixes (historique propre sur main) |
| **Merge commit** | Releases (pour conserver l'historique de release) |
| **Rebase and merge** | Petits commits propres (déconseillé sur features complexes) |

### 5.2 Conditions de merge

- Au moins **1 approval** requis (2 pour l'infrastructure)
- Toutes les **discussions résolues**
- CI **verte** (lint + tests)
- Branche à jour avec `main`
- Pas de merge si le PR est marqué `WIP` ou `Draft`

### 5.3 Après le merge

```bash
# Supprimer la branche locale
git branch -d feature/SYNELIA-42-add-kafka-ingestion-dag

# Mettre à jour main localement
git checkout main && git pull
```

---

## 6. Gestion des conflits

### 6.1 Prévention

- Mettre à jour sa branche régulièrement (au moins avant chaque push)
- Travailler sur des fichiers différents dans des branches parallèles
- Limiter la durée de vie des branches (fusionner rapidement)

### 6.2 Résolution

```bash
# Mettre à jour main
git fetch origin
git checkout main && git pull

# Rebaser sa branche
git checkout feature/SYNELIA-42-...
git rebase origin/main

# En cas de conflit :
# 1. Ouvrir les fichiers en conflit et résoudre manuellement
# 2. Marquer comme résolu
git add <fichier_resolu>
# 3. Continuer le rebase
git rebase --continue

# En cas de problème grave : annuler le rebase
git rebase --abort
```

### 6.3 Principes de résolution

- Ne jamais accepter aveuglément "les deux versions" (`both`)
- Comprendre **pourquoi** les deux versions divergent
- Tester après résolution : `pytest tests/ -v`
- Demander l'aide du collègue en cas de doute

---

## 7. Stratégie de release

### 7.1 Processus de release

```
1. Créer une branche release depuis main
   git checkout -b release/SYNELIA-v1.1.0

2. Mettre à jour CHANGELOG.md avec les changements de la version

3. Mettre à jour le numéro de version dans pyproject.toml

4. Ouvrir une PR release → main (review par le Lead Data Engineer)

5. Merger avec "Merge commit" (pour conserver l'historique)

6. Tagger le commit de release
   git tag -a v1.1.0 -m "Release v1.1.0 — Description"
   git push origin v1.1.0

7. Créer une GitHub Release à partir du tag
```

### 7.2 Hotfix en production

```
1. Créer depuis main (qui est en production)
   git checkout main && git pull
   git checkout -b hotfix/SYNELIA-99-fix-scheduler-crash

2. Corriger, tester, committer

3. PR → main avec revue urgente

4. Merger et tagger immédiatement
   git tag -a v1.0.1 -m "Hotfix v1.0.1"
```

---

## 8. Règles de versioning

Ce projet suit le [Versioning Sémantique 2.0.0](https://semver.org/lang/fr/) :

```
MAJEUR.MINEUR.PATCH
```

| Incrémentation | Quand |
|---------------|-------|
| **MAJEUR** | Changement incompatible (breaking change dans l'API ou la structure) |
| **MINEUR** | Nouvelle fonctionnalité rétrocompatible |
| **PATCH** | Correction de bug rétrocompatible |

### Exemples

```
1.0.0  → 1.0.1  : Correction de bug (hotfix)
1.0.1  → 1.1.0  : Nouvelle fonctionnalité (feature)
1.1.0  → 2.0.0  : Changement de structure DAG incompatible (breaking change)
```

### Pre-releases

```
1.1.0-alpha.1   : Version alpha (développement)
1.1.0-beta.1    : Version beta (tests)
1.1.0-rc.1      : Release Candidate
```

---

*Guide rédigé par Jean Mermoz Effi — Pôle Data SYNELIA | 08 mai 2026*
