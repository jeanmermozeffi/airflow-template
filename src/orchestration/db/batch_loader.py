"""
Chargement en batch avec upsert — Pôle Data SYNELIA.

Fournit des utilitaires de chargement bulk performants et idempotents :
  - upsert_records   : INSERT … ON CONFLICT DO UPDATE (PostgreSQL)
  - insert_records   : INSERT simple par batch
  - truncate_and_load: Vide la table et recharge (full refresh)

Ces fonctions s'appuient sur SQLAlchemy Core pour rester indépendantes
du dialecte (les helpers PostgreSQL-spécifiques sont marqués explicitement).

Exemple :

    from orchestration.db.batch_loader import upsert_records
    from orchestration.db.connection_factory import get_engine

    engine = get_engine(source_name="STAGING")
    upsert_records(
        engine=engine,
        table="staging.stg_orangescrum_tasks",
        records=extracted_rows,
        key_columns=["task_id"],
        update_columns=["name", "status", "updated_at", "loaded_at"],
        batch_size=5_000,
    )
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 5_000


# ── Upsert (PostgreSQL) ───────────────────────────────────────────────────────

def upsert_records(
    engine: Engine,
    table: str,
    records: list[dict],
    key_columns: list[str],
    update_columns: Optional[list[str]] = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> int:
    """Insère ou met à jour des enregistrements (INSERT … ON CONFLICT DO UPDATE).

    Compatible PostgreSQL. Pour les autres dialectes, utilisez insert_records()
    après un DELETE préalable.

    Args:
        engine:         Connexion SQLAlchemy.
        table:          Nom complet de la table (ex. "staging.stg_tasks").
        records:        Liste de dicts — les clés correspondent aux colonnes.
        key_columns:    Colonnes formant la clé de conflit (constraint UNIQUE).
        update_columns: Colonnes à mettre à jour en cas de conflit.
                        None = toutes les colonnes sauf les clés.
        batch_size:     Taille des lots pour les insertions.

    Returns:
        Nombre total d'enregistrements traités.
    """
    if not records:
        logger.info("[BATCH] upsert_records : aucun enregistrement à charger dans %s", table)
        return 0

    columns = list(records[0].keys())
    if update_columns is None:
        update_columns = [c for c in columns if c not in key_columns]

    conflict_target = ", ".join(key_columns)
    col_list        = ", ".join(columns)
    param_list      = ", ".join(f":{c}" for c in columns)
    update_set      = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    sql = text(f"""
        INSERT INTO {table} ({col_list})
        VALUES ({param_list})
        ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}
    """)

    total = _execute_in_batches(engine, sql, records, batch_size, operation="upsert", table=table)
    return total


# ── Insert simple ─────────────────────────────────────────────────────────────

def insert_records(
    engine: Engine,
    table: str,
    records: list[dict],
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> int:
    """Insère des enregistrements sans gestion de conflit.

    À utiliser après un DELETE/TRUNCATE préalable pour garantir l'idempotence.

    Returns:
        Nombre total d'enregistrements insérés.
    """
    if not records:
        logger.info("[BATCH] insert_records : aucun enregistrement à insérer dans %s", table)
        return 0

    columns   = list(records[0].keys())
    col_list  = ", ".join(columns)
    param_list = ", ".join(f":{c}" for c in columns)

    sql = text(f"INSERT INTO {table} ({col_list}) VALUES ({param_list})")
    return _execute_in_batches(engine, sql, records, batch_size, operation="insert", table=table)


# ── Truncate & load ───────────────────────────────────────────────────────────

def truncate_and_load(
    engine: Engine,
    table: str,
    records: list[dict],
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> int:
    """Vide la table (TRUNCATE) puis recharge les enregistrements.

    Idempotent sur l'ensemble de la table — à n'utiliser que pour les
    tables de petite taille ou lors des full refreshes.

    Returns:
        Nombre d'enregistrements chargés.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table}"))
        logger.info("[BATCH] Table tronquée : %s", table)

    return insert_records(engine, table, records, batch_size=batch_size)


# ── Delete + insert partitionné ───────────────────────────────────────────────

def delete_and_insert(
    engine: Engine,
    table: str,
    records: list[dict],
    delete_filter: str,
    delete_params: Optional[dict] = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> int:
    """Supprime les lignes matchant delete_filter puis insère les nouveaux records.

    Pattern idempotent recommandé pour les chargements partitionnés par date.

    Args:
        delete_filter: Clause WHERE sans le mot WHERE, ex. "loaded_date = :ds".
        delete_params: Paramètres de la clause WHERE, ex. {"ds": "2026-05-08"}.

    Example:
        delete_and_insert(
            engine, "staging.stg_tasks", records,
            delete_filter="loaded_date = :ds",
            delete_params={"ds": "2026-05-08"},
        )
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {table} WHERE {delete_filter}"),
            delete_params or {},
        )
        deleted = result.rowcount
        logger.info("[BATCH] %d ligne(s) supprimée(s) dans %s (filtre: %s)", deleted, table, delete_filter)

    return insert_records(engine, table, records, batch_size=batch_size)


# ── Utilitaire interne ────────────────────────────────────────────────────────

def _execute_in_batches(
    engine: Engine,
    sql: object,
    records: list[dict],
    batch_size: int,
    operation: str,
    table: str,
) -> int:
    """Exécute une instruction SQL paramétrée sur des batches de records."""
    total = len(records)
    n_batches = (total + batch_size - 1) // batch_size

    for batch_idx in range(n_batches):
        start = batch_idx * batch_size
        batch = records[start : start + batch_size]
        with engine.begin() as conn:
            conn.execute(sql, batch)
        logger.info(
            "[BATCH] %s %s — batch %d/%d (%d lignes)",
            operation, table, batch_idx + 1, n_batches, len(batch),
        )

    logger.info("[BATCH] %s terminé : %d enregistrement(s) dans %s", operation, total, table)
    return total
