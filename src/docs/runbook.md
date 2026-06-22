# Runbook Exploitation

## 1. Initialisation

1. Préparer l'environnement:
   - `cp .env.example .env`
2. Démarrer Airflow:
   - `docker compose -f docker/docker-compose.yml up -d airflow-init`
   - `docker compose -f docker/docker-compose.yml up -d`

## 2. Mise à jour des connexions et variables

Après toute modification de `config/connections.yaml` ou `config/variables.yaml`:

1. Exécuter:
   - `docker compose -f docker/docker-compose.yml run --rm airflow-cli python scripts/bootstrap_airflow.py`

## 3. Dépannage rapide

- DAG absent dans l'UI:
  - vérifier les logs scheduler.
  - vérifier la validité YAML de `config/pipelines.yaml`.
- Erreur de connexion DB:
  - vérifier `config/connections.yaml` + `.env`.
  - relancer `bootstrap_airflow.py`.
- Alerte SMTP:
  - vérifier `SMTP_*` dans `.env`.

## 4. Standards de changement

- Ne pas coder de secret en dur.
- Toute nouvelle dépendance doit être déclarée dans `requirements.txt`.
- Tout nouveau DAG doit passer par la factory `build_standard_dag`.

## 5. Multi-sources du même connecteur

Pour déclarer plusieurs sources PostgreSQL, utiliser des noms distincts:
- `ORCH_DB__DWH__TYPE=postgres`
- `ORCH_DB__RAW__TYPE=postgres`

Puis mapper ces sources dans `config/connections.yaml` avec des `conn_id` différents:
- `dwh_postgres`
- `raw_postgres`
