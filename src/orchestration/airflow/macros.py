"""
Macros Jinja personnalisées pour Airflow — Pôle Data SYNELIA.

Enregistre des fonctions utilitaires comme macros Jinja pour les DAGs.
À injecter via user_defined_macros dans la définition du DAG.

Utilisation dans un DAG :

    from orchestration.airflow.macros import AIRFLOW_MACROS

    with DAG(
        dag_id="mon_dag",
        user_defined_macros=AIRFLOW_MACROS,
        ...
    ) as dag:
        task = BashOperator(
            task_id="run_sql",
            bash_command="psql -c 'SELECT * FROM t WHERE dt = {{ ds_nodash(ds) }}'",
        )

Les macros sont aussi disponibles dans les champs template_fields des operators.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta


# ── Fonctions de macros ───────────────────────────────────────────────────────

def ds_nodash(ds: str) -> str:
    """Convertit YYYY-MM-DD en YYYYMMDD (format partition Hive/S3).

    Example:
        {{ ds_nodash(ds) }}  →  "20260508"
    """
    return ds.replace("-", "")


def ds_add(ds: str, days: int) -> str:
    """Ajoute ou soustrait des jours à une date ISO.

    Example:
        {{ ds_add(ds, -1) }}  →  date de la veille au format YYYY-MM-DD
    """
    return (date.fromisoformat(ds) + timedelta(days=days)).isoformat()


def ds_format(ds: str, fmt: str = "%d/%m/%Y") -> str:
    """Reformate une date ISO selon un format strftime.

    Example:
        {{ ds_format(ds, "%d/%m/%Y") }}  →  "08/05/2026"
    """
    return date.fromisoformat(ds).strftime(fmt)


def yesterday_ds(ds: str) -> str:
    """Retourne la date de la veille au format YYYY-MM-DD."""
    return ds_add(ds, -1)


def yesterday_nodash(ds: str) -> str:
    """Retourne la date de la veille au format YYYYMMDD."""
    return ds_nodash(ds_add(ds, -1))


def first_day_of_month(ds: str) -> str:
    """Retourne le premier jour du mois au format YYYY-MM-DD."""
    d = date.fromisoformat(ds)
    return d.replace(day=1).isoformat()


def last_day_of_month(ds: str) -> str:
    """Retourne le dernier jour du mois au format YYYY-MM-DD."""
    d = date.fromisoformat(ds)
    next_month = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    return (next_month - timedelta(days=1)).isoformat()


def week_label(ds: str) -> str:
    """Retourne le label de semaine ISO (ex. "2026-W19").

    Example:
        {{ week_label(ds) }}  →  "2026-W19"
    """
    d = date.fromisoformat(ds)
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def month_label(ds: str) -> str:
    """Retourne le label de mois (ex. "2026-05")."""
    return ds[:7]


def quarter_label(ds: str) -> str:
    """Retourne le label de trimestre (ex. "2026-Q2")."""
    d = date.fromisoformat(ds)
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


def year_label(ds: str) -> str:
    """Retourne l'année (ex. "2026")."""
    return ds[:4]


def is_first_day_of_month(ds: str) -> bool:
    """Retourne True si ds est le premier jour du mois."""
    return date.fromisoformat(ds).day == 1


def is_monday(ds: str) -> bool:
    """Retourne True si ds est un lundi."""
    return date.fromisoformat(ds).weekday() == 0


def now_utc() -> str:
    """Retourne la date-heure UTC courante au format ISO 8601."""
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


# ── Dictionnaire des macros à injecter dans user_defined_macros ───────────────

AIRFLOW_MACROS: dict[str, object] = {
    "ds_nodash":            ds_nodash,
    "ds_add":               ds_add,
    "ds_format":            ds_format,
    "yesterday_ds":         yesterday_ds,
    "yesterday_nodash":     yesterday_nodash,
    "first_day_of_month":   first_day_of_month,
    "last_day_of_month":    last_day_of_month,
    "week_label":           week_label,
    "month_label":          month_label,
    "quarter_label":        quarter_label,
    "year_label":           year_label,
    "is_first_day_of_month": is_first_day_of_month,
    "is_monday":            is_monday,
    "now_utc":              now_utc,
}
