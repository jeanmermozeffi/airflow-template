"""
Framework de contrôle qualité des données — Pôle Data SYNELIA.

Fournit des fonctions de vérification programmatiques utilisables dans des
PythonOperator ou dans le SqlQualityOperator existant.

Chaque check retourne un CheckResult : résultat + métadonnées + levée d'exception
si le seuil configuré est dépassé.

Exemple dans un DAG :

    from orchestration.data_quality.checks import check_count, check_no_nulls, run_checks
    from orchestration.db.connection_factory import get_engine

    def run_dq_checks(**context):
        engine = get_engine(source_name="STAGING")
        ds     = context["ds"]
        filter = f"loaded_date = '{ds}'"

        results = run_checks(engine, [
            check_count("stg_orangescrum_tasks",       min_rows=1,    date_filter=filter),
            check_no_nulls("stg_orangescrum_tasks",    columns=["task_id", "project_id"], date_filter=filter),
            check_no_duplicates("stg_orangescrum_tasks", key_columns=["task_id"], date_filter=filter),
            check_freshness("stg_orangescrum_tasks",   ts_column="loaded_at", max_age_hours=25),
        ])

        failed = [r for r in results if not r.passed]
        if failed:
            raise ValueError(f"{len(failed)} contrôle(s) qualité échoué(s) : {[r.name for r in failed]}")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# ── Résultat d'un check ───────────────────────────────────────────────────────

@dataclass
class CheckResult:
    """Résultat d'un contrôle qualité."""

    name: str
    passed: bool
    value: Any = None
    threshold: Any = None
    message: str = ""
    metadata: dict = field(default_factory=dict)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name} — {self.message}"


# ── Type alias ────────────────────────────────────────────────────────────────

CheckSpec = Callable[[Engine], CheckResult]


# ── Factories de checks ───────────────────────────────────────────────────────

def check_count(
    table: str,
    min_rows: int = 1,
    max_rows: Optional[int] = None,
    date_filter: Optional[str] = None,
    schema: Optional[str] = None,
) -> CheckSpec:
    """Contrôle que le nombre de lignes est dans la plage [min_rows, max_rows].

    Args:
        table:       Nom de la table (sans schéma).
        min_rows:    Minimum attendu (défaut 1 — vérifie que la table n'est pas vide).
        max_rows:    Maximum acceptable (None = pas de limite supérieure).
        date_filter: Clause WHERE pour filtrer par partition, ex. "loaded_date = '2026-05-08'".
        schema:      Schéma SQL (None = schéma courant de la connexion).
    """
    full_table = f"{schema}.{table}" if schema else table
    check_name = f"count({full_table})"

    def _run(engine: Engine) -> CheckResult:
        where = f"WHERE {date_filter}" if date_filter else ""
        sql = f"SELECT COUNT(*) FROM {full_table} {where}"
        with engine.connect() as conn:
            count = conn.execute(text(sql)).scalar() or 0

        passed = count >= min_rows and (max_rows is None or count <= max_rows)
        threshold_desc = f">= {min_rows}" + (f" et <= {max_rows}" if max_rows else "")
        msg = (
            f"count={count:,} {threshold_desc}"
            + (f" | filtre : {date_filter}" if date_filter else "")
        )
        logger.info("[DQ] %s : %s", check_name, msg)
        return CheckResult(
            name=check_name,
            passed=passed,
            value=count,
            threshold={"min": min_rows, "max": max_rows},
            message=msg,
        )

    return _run


def check_no_nulls(
    table: str,
    columns: list[str],
    date_filter: Optional[str] = None,
    schema: Optional[str] = None,
) -> CheckSpec:
    """Contrôle l'absence de valeurs NULL sur les colonnes critiques."""
    full_table = f"{schema}.{table}" if schema else table
    check_name = f"no_nulls({full_table}[{', '.join(columns)}])"

    def _run(engine: Engine) -> CheckResult:
        null_conditions = " OR ".join(f"{col} IS NULL" for col in columns)
        where_parts = [f"({null_conditions})"]
        if date_filter:
            where_parts.append(date_filter)
        where = "WHERE " + " AND ".join(where_parts)

        sql = f"SELECT COUNT(*) FROM {full_table} {where}"
        with engine.connect() as conn:
            null_count = conn.execute(text(sql)).scalar() or 0

        passed = null_count == 0
        msg = f"null_count={null_count:,} sur colonnes {columns}"
        if date_filter:
            msg += f" | filtre : {date_filter}"
        logger.info("[DQ] %s : %s", check_name, msg)
        return CheckResult(
            name=check_name,
            passed=passed,
            value=null_count,
            threshold=0,
            message=msg,
            metadata={"columns": columns},
        )

    return _run


def check_no_duplicates(
    table: str,
    key_columns: list[str],
    date_filter: Optional[str] = None,
    schema: Optional[str] = None,
    max_duplicate_rate: float = 0.0,
) -> CheckSpec:
    """Contrôle l'unicité sur les colonnes clés.

    Args:
        key_columns:         Colonnes qui forment la clé unique.
        max_duplicate_rate:  Taux de doublons toléré (0.0 = aucun doublon).
    """
    full_table = f"{schema}.{table}" if schema else table
    check_name = f"no_duplicates({full_table}[{', '.join(key_columns)}])"
    key_expr = ", ".join(key_columns)

    def _run(engine: Engine) -> CheckResult:
        where = f"WHERE {date_filter}" if date_filter else ""
        sql = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT ({key_expr})) AS distinct_rows
            FROM {full_table} {where}
        """
        with engine.connect() as conn:
            row = conn.execute(text(sql)).fetchone()

        total    = row[0] if row else 0
        distinct = row[1] if row else 0
        dup_count = total - distinct
        dup_rate = dup_count / total if total > 0 else 0.0

        passed = dup_rate <= max_duplicate_rate
        msg = f"total={total:,} distinct={distinct:,} doublons={dup_count:,} taux={dup_rate:.2%}"
        logger.info("[DQ] %s : %s", check_name, msg)
        return CheckResult(
            name=check_name,
            passed=passed,
            value=dup_rate,
            threshold=max_duplicate_rate,
            message=msg,
            metadata={"key_columns": key_columns, "duplicate_count": dup_count},
        )

    return _run


def check_freshness(
    table: str,
    ts_column: str,
    max_age_hours: float = 25.0,
    schema: Optional[str] = None,
) -> CheckSpec:
    """Contrôle que la table contient des données récentes.

    Vérifie que MAX(ts_column) est plus récent que NOW() - max_age_hours.
    """
    full_table = f"{schema}.{table}" if schema else table
    check_name = f"freshness({full_table}.{ts_column})"

    def _run(engine: Engine) -> CheckResult:
        sql = f"SELECT MAX({ts_column}) FROM {full_table}"
        with engine.connect() as conn:
            latest: Optional[datetime] = conn.execute(text(sql)).scalar()

        if latest is None:
            return CheckResult(
                name=check_name, passed=False, value=None,
                threshold=max_age_hours,
                message=f"Table vide — aucune valeur dans {ts_column}",
            )

        if isinstance(latest, str):
            latest = datetime.fromisoformat(latest)

        age_hours = (datetime.utcnow() - latest).total_seconds() / 3600
        passed = age_hours <= max_age_hours
        msg = f"latest={latest.isoformat()} age={age_hours:.1f}h (max={max_age_hours}h)"
        logger.info("[DQ] %s : %s", check_name, msg)
        return CheckResult(
            name=check_name,
            passed=passed,
            value=age_hours,
            threshold=max_age_hours,
            message=msg,
            metadata={"latest_ts": str(latest)},
        )

    return _run


def check_volume_vs_reference(
    table: str,
    reference_table: str,
    max_variation_pct: float = 0.20,
    date_filter: Optional[str] = None,
    ref_date_filter: Optional[str] = None,
    schema: Optional[str] = None,
) -> CheckSpec:
    """Contrôle que le volume de la table ne s'écarte pas trop de la référence.

    Typiquement utilisé pour comparer staging vs source, ou J vs J-1.

    Args:
        table:              Table cible.
        reference_table:    Table de référence (peut être la même table sur J-1).
        max_variation_pct:  Variation maximale tolérée (0.20 = 20%).
        date_filter:        Filtre pour la table cible.
        ref_date_filter:    Filtre pour la table de référence.
    """
    full_table = f"{schema}.{table}" if schema else table
    full_ref   = f"{schema}.{reference_table}" if schema else reference_table
    check_name = f"volume_check({full_table} vs {full_ref})"

    def _run(engine: Engine) -> CheckResult:
        where     = f"WHERE {date_filter}" if date_filter else ""
        ref_where = f"WHERE {ref_date_filter}" if ref_date_filter else ""

        with engine.connect() as conn:
            target_count = conn.execute(text(f"SELECT COUNT(*) FROM {full_table} {where}")).scalar() or 0
            ref_count    = conn.execute(text(f"SELECT COUNT(*) FROM {full_ref} {ref_where}")).scalar() or 0

        if ref_count == 0:
            return CheckResult(
                name=check_name, passed=False, value=None, threshold=max_variation_pct,
                message=f"Table de référence vide ({full_ref})",
            )

        variation = abs(target_count - ref_count) / ref_count
        passed = variation <= max_variation_pct
        msg = (
            f"target={target_count:,} ref={ref_count:,} "
            f"variation={variation:.1%} (max={max_variation_pct:.0%})"
        )
        logger.info("[DQ] %s : %s", check_name, msg)
        return CheckResult(
            name=check_name,
            passed=passed,
            value=variation,
            threshold=max_variation_pct,
            message=msg,
            metadata={"target_count": target_count, "ref_count": ref_count},
        )

    return _run


# ── Exécuteur ─────────────────────────────────────────────────────────────────

def run_checks(
    engine: Engine,
    checks: list[CheckSpec],
    raise_on_failure: bool = True,
) -> list[CheckResult]:
    """Exécute une liste de checks et retourne les résultats.

    Args:
        engine:           Connexion SQLAlchemy.
        checks:           Liste de fonctions retournées par les factories check_*().
        raise_on_failure: Lève une ValueError si au moins un check échoue.

    Returns:
        Liste de CheckResult dans l'ordre des checks fournis.
    """
    results: list[CheckResult] = []
    for check_fn in checks:
        try:
            result = check_fn(engine)
        except Exception as exc:
            result = CheckResult(
                name=getattr(check_fn, "__name__", "unknown"),
                passed=False,
                message=f"Exception lors du check : {exc}",
            )
            logger.error("[DQ] Erreur inattendue : %s", exc, exc_info=True)
        results.append(result)

    failed = [r for r in results if not r.passed]
    if failed and raise_on_failure:
        summary = "; ".join(str(r) for r in failed)
        raise ValueError(f"{len(failed)}/{len(results)} contrôle(s) qualité échoué(s) : {summary}")

    return results
