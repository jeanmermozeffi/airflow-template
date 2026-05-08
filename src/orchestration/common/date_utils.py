"""
Utilitaires de manipulation de dates — Pôle Data SYNELIA.

Fournit des helpers communs pour les DAGs Airflow : partitions temporelles,
labels de période, jours ouvrés, plages de dates.

Exemple :

    from orchestration.common.date_utils import (
        to_nodash, get_period_label, iter_date_range, get_business_days_ago,
    )

    # Convertir ds en partition yyyymmdd
    partition = to_nodash("2026-05-08")  # → "20260508"

    # Label lisible pour les rapports
    label = get_period_label("2026-05-08", "week")  # → "2026-W19"

    # Derniers N jours ouvrés
    cutoff = get_business_days_ago(5)  # → date 5 jours ouvrés en arrière
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterator


# ── Conversion ────────────────────────────────────────────────────────────────

def to_date(ds: str | date | datetime) -> date:
    """Normalise une valeur en objet `date`.

    Accepte une chaîne ISO 8601 (YYYY-MM-DD), un objet `date` ou `datetime`.
    """
    if isinstance(ds, datetime):
        return ds.date()
    if isinstance(ds, date):
        return ds
    return date.fromisoformat(ds)


def to_nodash(ds: str | date | datetime) -> str:
    """Retourne la date au format YYYYMMDD (partition Hive/S3).

    Example:
        to_nodash("2026-05-08")  → "20260508"
    """
    return to_date(ds).strftime("%Y%m%d")


def to_iso(ds: str | date | datetime) -> str:
    """Retourne la date au format ISO YYYY-MM-DD."""
    return to_date(ds).isoformat()


# ── Labels de période ─────────────────────────────────────────────────────────

def get_period_label(ds: str | date | datetime, period: str = "day") -> str:
    """Retourne un label lisible pour une période.

    Args:
        ds:     Date de référence.
        period: "day", "week", "month", "quarter", "year".

    Returns:
        Exemples : "2026-05-08", "2026-W19", "2026-05", "2026-Q2", "2026".

    Raises:
        ValueError: Si `period` n'est pas reconnu.
    """
    d = to_date(ds)
    if period == "day":
        return d.isoformat()
    if period == "week":
        return f"{d.isocalendar().year}-W{d.isocalendar().week:02d}"
    if period == "month":
        return d.strftime("%Y-%m")
    if period == "quarter":
        q = (d.month - 1) // 3 + 1
        return f"{d.year}-Q{q}"
    if period == "year":
        return str(d.year)
    raise ValueError(f"Période non reconnue : '{period}'. Valeurs acceptées : day, week, month, quarter, year.")


def get_first_day_of_month(ds: str | date | datetime) -> date:
    """Retourne le premier jour du mois de la date donnée."""
    d = to_date(ds)
    return d.replace(day=1)


def get_last_day_of_month(ds: str | date | datetime) -> date:
    """Retourne le dernier jour du mois de la date donnée."""
    d = to_date(ds)
    next_month = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    return next_month - timedelta(days=1)


def get_first_day_of_week(ds: str | date | datetime, start_monday: bool = True) -> date:
    """Retourne le premier jour de la semaine (lundi par défaut)."""
    d = to_date(ds)
    offset = d.weekday() if start_monday else (d.weekday() + 1) % 7
    return d - timedelta(days=offset)


# ── Navigation de dates ───────────────────────────────────────────────────────

def add_days(ds: str | date | datetime, days: int) -> date:
    """Ajoute (ou soustrait si négatif) un nombre de jours à une date."""
    return to_date(ds) + timedelta(days=days)


def yesterday(ds: str | date | datetime) -> date:
    """Retourne la veille de la date donnée."""
    return add_days(ds, -1)


def tomorrow(ds: str | date | datetime) -> date:
    """Retourne le lendemain de la date donnée."""
    return add_days(ds, 1)


# ── Jours ouvrés ─────────────────────────────────────────────────────────────

_WEEKDAY_NAMES = {0: "lun", 1: "mar", 2: "mer", 3: "jeu", 4: "ven", 5: "sam", 6: "dim"}


def is_business_day(ds: str | date | datetime) -> bool:
    """Retourne True si la date est un jour ouvré (lundi–vendredi)."""
    return to_date(ds).weekday() < 5


def next_business_day(ds: str | date | datetime) -> date:
    """Retourne le prochain jour ouvré après la date donnée."""
    d = to_date(ds) + timedelta(days=1)
    while not is_business_day(d):
        d += timedelta(days=1)
    return d


def previous_business_day(ds: str | date | datetime) -> date:
    """Retourne le dernier jour ouvré avant la date donnée."""
    d = to_date(ds) - timedelta(days=1)
    while not is_business_day(d):
        d -= timedelta(days=1)
    return d


def get_business_days_ago(n: int, reference: date | None = None) -> date:
    """Retourne la date N jours ouvrés en arrière depuis la date de référence.

    Args:
        n:         Nombre de jours ouvrés à soustraire (doit être > 0).
        reference: Date de départ (aujourd'hui si None).

    Example:
        get_business_days_ago(3)  # 3 jours ouvrés avant aujourd'hui
    """
    if n <= 0:
        raise ValueError(f"n doit être un entier positif, reçu : {n}")
    d = reference or date.today()
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if is_business_day(d):
            count += 1
    return d


# ── Plages de dates ───────────────────────────────────────────────────────────

def iter_date_range(
    start: str | date | datetime,
    end: str | date | datetime,
    step_days: int = 1,
    business_only: bool = False,
) -> Iterator[date]:
    """Génère les dates entre start et end (inclusifs).

    Args:
        start:         Date de début (incluse).
        end:           Date de fin (incluse).
        step_days:     Pas en jours (défaut = 1).
        business_only: Si True, ne retourne que les jours ouvrés.

    Yields:
        Objets `date` entre start et end.
    """
    current = to_date(start)
    end_d = to_date(end)
    while current <= end_d:
        if not business_only or is_business_day(current):
            yield current
        current += timedelta(days=step_days)


def list_date_range(
    start: str | date | datetime,
    end: str | date | datetime,
    step_days: int = 1,
    business_only: bool = False,
) -> list[date]:
    """Version list de iter_date_range."""
    return list(iter_date_range(start, end, step_days=step_days, business_only=business_only))


def get_partition_range(
    start: str | date | datetime,
    end: str | date | datetime,
) -> list[str]:
    """Retourne la liste des partitions YYYYMMDD entre start et end."""
    return [to_nodash(d) for d in iter_date_range(start, end)]
