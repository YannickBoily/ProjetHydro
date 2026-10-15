"""Human-readable formatting helpers for the public Streamlit dashboard.

These helpers only affect presentation. Raw values and database/model schemas remain
unchanged so analytics and ML code can continue to use technical column names.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

QUEBEC_TIMEZONE = "America/Toronto"

_MONTHS_FR = {
    1: "janv.",
    2: "févr.",
    3: "mars",
    4: "avr.",
    5: "mai",
    6: "juin",
    7: "juil.",
    8: "août",
    9: "sept.",
    10: "oct.",
    11: "nov.",
    12: "déc.",
}

PUBLIC_COLUMN_LABELS = {
    "outage_id": "Identifiant de la panne",
    "customers_affected": "Clients affectés",
    "municipality_label": "Municipalité",
    "municipality_name": "Municipalité",
    "municipality_id": "Identifiant de la municipalité",
    "mrc_name": "MRC",
    "region_name": "Région",
    "status_fr": "État",
    "analysis_cause_label_fr": "Cause",
    "history_cause_label_fr": "Cause",
    "latest_raw_cause_label_fr": "Cause",
    "active_capture_at": "Dernière mise à jour",
    "captured_at": "Moment du relevé",
    "first_capture_at": "Première observation",
    "last_capture_at": "Dernière observation",
    "latest_row_captured_at": "Dernière observation",
    "start_time": "Début de la panne",
    "estimated_restore": "Rétablissement prévu par Hydro-Québec",
    "latest_estimated_restore": "Rétablissement prévu par Hydro-Québec",
    "first_hydro_eta": "Première estimation Hydro-Québec",
    "predicted_end_at": "Rétablissement prévu",
    "initial_no_eta_predicted_restore_time": "Prévision initiale du modèle, sans estimation Hydro-Québec",
    "initial_first_eta_predicted_restore_time": "Prévision initiale du modèle, avec estimation Hydro-Québec",
    "live_predicted_restore_time": "Rétablissement prévu par le modèle",
    "observed_duration_hours": "Durée observée",
    "predicted_duration_hours": "Durée prévue",
    "outage_age_hours_at_capture": "Panne active depuis",
    "live_predicted_remaining_hours": "Temps restant prévu par le modèle",
    "live_vs_hydro_minutes": "Écart avec Hydro-Québec",
    "ml_data_as_of": "Données utilisées jusqu'à",
    "live_prediction_available": "Prévision actuelle disponible",
    "live_probability_remaining_over_1h": "Probabilité de durer encore plus de 1 h",
    "live_probability_remaining_over_2h": "Probabilité de durer encore plus de 2 h",
    "live_probability_remaining_over_4h": "Probabilité de durer encore plus de 4 h",
    "live_probability_remaining_over_8h": "Probabilité de durer encore plus de 8 h",
    "lat": "Latitude",
    "lon": "Longitude",
    "is_geocoded_fr": "Géolocalisée",
    "is_major_outage_fr": "Panne majeure",
    "has_known_cause_fr": "Cause connue",
    "check_name_fr": "Contrôle",
    "status_quality_fr": "État du contrôle",
    "severity_fr": "Gravité",
}

_DATETIME_COLUMNS = {
    "active_capture_at",
    "captured_at",
    "first_capture_at",
    "last_capture_at",
    "latest_row_captured_at",
    "start_time",
    "estimated_restore",
    "latest_estimated_restore",
    "first_hydro_eta",
    "predicted_end_at",
    "initial_no_eta_predicted_restore_time",
    "initial_first_eta_predicted_restore_time",
    "live_predicted_restore_time",
    "ml_data_as_of",
}

_DURATION_HOURS_COLUMNS = {
    "observed_duration_hours",
    "predicted_duration_hours",
    "outage_age_hours_at_capture",
    "live_predicted_remaining_hours",
}

_REDUNDANT_RAW_PAIRS = {
    "status_fr": "status",
    "analysis_cause_label_fr": "analysis_cause_label",
    "latest_raw_cause_label_fr": "latest_raw_cause_label",
    "history_cause_label_fr": "cause_label",
    "has_known_cause_fr": "has_known_cause",
    "is_geocoded_fr": "is_geocoded",
    "is_major_outage_fr": "is_major_outage",
    "check_name_fr": "check_name",
    "status_quality_fr": "status",
    "severity_fr": "severity",
}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _quebec_timestamp(value: Any) -> pd.Timestamp | None:
    if _is_missing(value):
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(QUEBEC_TIMEZONE)


def format_time_fr(value: Any, missing: str = "—") -> str:
    """Format a timestamp as Quebec local clock time, e.g. ``15 h 20``."""
    ts = _quebec_timestamp(value)
    if ts is None:
        return missing
    return f"{ts.hour} h {ts.minute:02d}"


def format_date_fr(value: Any, *, include_year: bool = False, missing: str = "—") -> str:
    """Format a calendar date without shifting date-only values across time zones."""
    if _is_missing(value):
        return missing
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return missing
    if pd.isna(ts):
        return missing
    if ts.tzinfo is not None:
        ts = ts.tz_convert(QUEBEC_TIMEZONE)
    year = f" {ts.year}" if include_year else ""
    return f"{ts.day} {_MONTHS_FR[ts.month]}{year}"


def format_quebec_datetime(
    value: Any,
    *,
    include_year: bool = True,
    missing: str = "Non disponible",
) -> str:
    """Format a timestamp as ``15 oct. 2026 à 15 h 20`` in Quebec local time."""
    ts = _quebec_timestamp(value)
    if ts is None:
        return missing
    year = f" {ts.year}" if include_year else ""
    return f"{ts.day} {_MONTHS_FR[ts.month]}{year} à {ts.hour} h {ts.minute:02d}"


def format_quebec_datetime_short(value: Any, missing: str = "—") -> str:
    """Compact public format, e.g. ``15 oct. à 15 h 20``."""
    return format_quebec_datetime(value, include_year=False, missing=missing)


def format_duration_minutes(value: Any, missing: str = "—") -> str:
    """Format minutes as a natural French duration without decimal hours."""
    if _is_missing(value):
        return missing
    try:
        total_minutes = max(int(round(float(value))), 0)
    except (TypeError, ValueError):
        return missing

    days, rem = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} j")
    if hours:
        parts.append(f"{hours} h")
    if minutes or not parts:
        parts.append(f"{minutes} min")
    return " ".join(parts)


def format_duration_hours(value: Any, missing: str = "—") -> str:
    """Format decimal hours as ``1 h 12 min`` instead of ``1.2 h``."""
    if _is_missing(value):
        return missing
    try:
        return format_duration_minutes(float(value) * 60.0, missing=missing)
    except (TypeError, ValueError):
        return missing


def format_gap_minutes_fr(value: Any, missing: str = "—") -> str:
    """Explain a model-vs-Hydro time gap without exposing signed numbers."""
    if _is_missing(value):
        return missing
    try:
        minutes = int(round(float(value)))
    except (TypeError, ValueError):
        return missing
    if abs(minutes) < 1:
        return "≈ même heure"
    duration = format_duration_minutes(abs(minutes), missing=missing)
    return f"{duration} plus tôt" if minutes < 0 else f"{duration} plus tard"


def _fallback_label(column: str) -> str:
    """Make an unknown snake_case field less technical without changing its value."""
    text = str(column).replace("_", " ").strip()
    replacements = {
        "outage": "panne",
        "customers": "clients",
        "affected": "affectés",
        "captured": "observé",
        "capture": "observation",
        "estimated": "estimé",
        "restore": "rétablissement",
        "predicted": "prévu",
        "duration": "durée",
        "hours": "heures",
        "minutes": "minutes",
        "municipality": "municipalité",
        "region": "région",
        "cause": "cause",
        "status": "état",
        "first": "premier",
        "last": "dernier",
        "latest": "dernier",
        "start": "début",
        "end": "fin",
        "count": "nombre",
        "rate": "taux",
        "known": "connu",
        "name": "nom",
        "id": "identifiant",
    }
    words = [replacements.get(word, word) for word in text.split()]
    result = " ".join(words)
    return result[:1].upper() + result[1:] if result else str(column)


def humanize_dataframe(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Return a display-only dataframe with readable labels, dates and durations."""
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()

    out = df.copy()
    if columns is not None:
        selected = [col for col in columns if col in out.columns]
        out = out[selected]

    for readable, raw_col in _REDUNDANT_RAW_PAIRS.items():
        if readable in out.columns and raw_col in out.columns:
            out = out.drop(columns=[raw_col])

    for col in list(out.columns):
        if col == "date":
            out[col] = out[col].map(lambda value: format_date_fr(value, include_year=True))
        elif col in _DATETIME_COLUMNS or col.endswith("_at"):
            out[col] = out[col].map(format_quebec_datetime_short)
        elif col in _DURATION_HOURS_COLUMNS or col.endswith("_duration_hours") or col.endswith("_hours"):
            out[col] = pd.to_numeric(out[col], errors="coerce").map(format_duration_hours)
        elif col == "live_vs_hydro_minutes":
            out[col] = pd.to_numeric(out[col], errors="coerce").map(format_gap_minutes_fr)

    renamed = {col: PUBLIC_COLUMN_LABELS.get(col, _fallback_label(col)) for col in out.columns}
    out = out.rename(columns=renamed)

    # Avoid duplicate visual labels after translating related technical fields.
    seen: dict[str, int] = {}
    clean_columns: list[str] = []
    for col in out.columns:
        label = str(col).strip() or "Colonne"
        seen[label] = seen.get(label, 0) + 1
        clean_columns.append(label if seen[label] == 1 else f"{label} ({seen[label]})")
    out.columns = clean_columns
    return out.reset_index(drop=True)
