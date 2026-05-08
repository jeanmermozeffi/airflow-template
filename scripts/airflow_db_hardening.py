#!/usr/bin/env python3
"""
Hardening de la base de données Airflow (PostgreSQL).

- Crée le rôle applicatif non-superuser (AIRFLOW_DB_USER)
- Transfère la propriété de la DB au rôle applicatif
- Supprime les artifacts malveillants connus (event triggers, rôles suspects)

À lancer avec user root (0:0) pendant airflow-init, avant airflow db migrate.

Variables d'environnement :
  POSTGRES_USER      — superuser PostgreSQL (défaut: admin_airflow)
  POSTGRES_PASSWORD  — mot de passe du superuser
  AIRFLOW_DB_USER    — rôle applicatif Airflow (défaut: airflow)
  AIRFLOW_DB_PASSWORD — mot de passe du rôle applicatif
  POSTGRES_DB        — nom de la base de données (défaut: airflow)
"""
import os
import sys

import psycopg2
from psycopg2 import sql


def ensure_login_role(cur, role_name: str, role_password: str) -> None:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role_name,))
    exists = cur.fetchone() is not None
    if role_password:
        stmt = "ALTER ROLE {} LOGIN PASSWORD %s" if exists else "CREATE ROLE {} LOGIN PASSWORD %s"
        cur.execute(sql.SQL(stmt).format(sql.Identifier(role_name)), (role_password,))
    else:
        stmt = "ALTER ROLE {} LOGIN" if exists else "CREATE ROLE {} LOGIN"
        cur.execute(sql.SQL(stmt).format(sql.Identifier(role_name)))


def main() -> None:
    admin_user = os.getenv("POSTGRES_USER", "admin_airflow")
    admin_password = os.getenv("POSTGRES_PASSWORD", "")
    app_user = os.getenv("AIRFLOW_DB_USER", "airflow")
    app_password = os.getenv("AIRFLOW_DB_PASSWORD", "")
    db_name = os.getenv("POSTGRES_DB", "airflow")
    conn = psycopg2.connect(
        host="postgres", port=5432,
        user=admin_user, password=admin_password, dbname=db_name,
    )
    conn.autocommit = True

    with conn, conn.cursor() as cur:
        cur.execute("SELECT usesuper FROM pg_user WHERE usename = current_user")
        row = cur.fetchone()
        if not row or not row[0]:
            print(f"ERREUR: '{admin_user}' n'est pas superuser — hardening annulé.", file=sys.stderr)
            sys.exit(1)

        # Suppression des artifacts malveillants connus
        cur.execute("DROP EVENT TRIGGER IF EXISTS log_start")
        cur.execute("DROP EVENT TRIGGER IF EXISTS log_end")
        cur.execute("DROP FUNCTION IF EXISTS public.escalate_priv()")
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'priv_esc'")
        cur.execute("DROP ROLE IF EXISTS priv_esc")

        ensure_login_role(cur, app_user, app_password)

        # Restreindre le rôle applicatif seulement s'il est distinct du compte admin connecté.
        # Si app_user == admin_user, retirer SUPERUSER/CREATEROLE sur la session active
        # ferait échouer les opérations suivantes qui nécessitent ces privilèges.
        if app_user != admin_user:
            cur.execute(sql.SQL(
                "ALTER ROLE {} NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION"
            ).format(sql.Identifier(app_user)))

        # Propriété DB → rôle applicatif
        cur.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
            sql.Identifier(db_name), sql.Identifier(app_user)))
        cur.execute(sql.SQL("ALTER SCHEMA public OWNER TO {}").format(sql.Identifier(app_user)))
        cur.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
            sql.Identifier(db_name), sql.Identifier(app_user)))

        print(
            f"DB hardening OK : app='{app_user}' (non-superuser), admin='{admin_user}' conservé."
        )


if __name__ == "__main__":
    main()
