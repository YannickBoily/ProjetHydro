"""Chargement, normalisation et accès aux données du dashboard."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from scripts.time_utils import normalize_capture_series, normalize_hydro_local_series
from dashboard.config import (
    ACTIVE_CACHE_TTL_SECONDS,
    CACHE_TTL_SECONDS,
    DAILY_CACHE_TTL_SECONDS,
    DEFAULT_HISTORY_DAYS,
    DEFAULT_HISTORY_ROWS_LIMIT,
    NUMERIC_COLUMNS,
    QUALITY_CACHE_TTL_SECONDS,
    PIPELINE_HEALTH_CACHE_TTL_SECONDS,
    QUEBEC_TIMEZONE,
    RECENT_CACHE_TTL_SECONDS,
    TIMESTAMP_COLUMNS,
)

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliser les types communs aux sources CSV et Supabase."""
    if df.empty:
        return df

    normalized = df.copy()

    # Hydro start/restore values in legacy CSV files are Quebec wall-clock
    # timestamps, while capture-derived values are UTC wall-clock timestamps.
    # Supabase TIMESTAMPTZ values and newer CSV rows already carry an offset;
    # the helpers preserve those offsets and normalize everything to UTC first.
    hydro_local_columns = {"start_time", "estimated_restore"}
    for column in TIMESTAMP_COLUMNS:
        if column not in normalized.columns:
            continue
        if column in hydro_local_columns:
            parsed = normalize_hydro_local_series(normalized[column])
        else:
            parsed = normalize_capture_series(normalized[column])
        normalized[column] = parsed.dt.tz_convert(QUEBEC_TIMEZONE)

    if "date" in normalized.columns:
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")

    for column in NUMERIC_COLUMNS:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(
                normalized[column],
                errors="coerce",
            )

    return normalized


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_csv(path: Path) -> pd.DataFrame:
    """Charger un fichier CSV et normaliser les colonnes utilisées par l'app."""
    if not path.exists():
        return pd.DataFrame()

    return normalize_dataframe(pd.read_csv(path, low_memory=False))


def get_config_value(name: str, default: Any = None) -> Any:
    value = os.environ.get(name)
    if value:
        return value
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


def using_supabase() -> bool:
    """Indiquer si une connexion Supabase est configurée."""
    return bool(get_config_value("SUPABASE_DB_URL"))


@st.cache_resource(show_spinner=False)
def get_supabase_engine():
    """Créer et réutiliser le moteur PostgreSQL/Supabase."""
    database_url = get_config_value("SUPABASE_DB_URL")
    database_hostaddr = get_config_value("SUPABASE_DB_HOSTADDR")

    if not database_url:
        return None

    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        st.error(
            "La dépendance `SQLAlchemy` est manquante."
        )
        raise exc

    # Forcer SQLAlchemy à utiliser psycopg2-binary
    database_url = str(database_url).strip()

    if database_url.startswith("postgresql://"):
        database_url = database_url.replace(
            "postgresql://",
            "postgresql+psycopg2://",
            1,
        )
    elif database_url.startswith("postgres://"):
        database_url = database_url.replace(
            "postgres://",
            "postgresql+psycopg2://",
            1,
        )

    connect_args = {
        "sslmode": "require",
        "connect_timeout": 10,
    }

    if database_hostaddr:
        connect_args["hostaddr"] = database_hostaddr

    return create_engine(
        database_url,
        connect_args=connect_args,
        pool_pre_ping=True,
        pool_recycle=240,
        use_native_hstore=False,
    )


def load_supabase_query(query: str) -> pd.DataFrame:
    """Exécuter une requête PostgreSQL/Supabase et normaliser le résultat."""
    engine = get_supabase_engine()

    if engine is None:
        return pd.DataFrame()

    df = pd.read_sql_query(
        query,
        engine,
    )

    return normalize_dataframe(df)


def get_supabase_history_days() -> int:
    """Lire la profondeur d’historique configurée, avec un backup."""
    raw_value = get_config_value("SUPABASE_HISTORY_DAYS", str(DEFAULT_HISTORY_DAYS))
    try:
        days = int(raw_value)
    except (TypeError, ValueError):
        days = DEFAULT_HISTORY_DAYS

    return max(days, 1)


def get_supabase_history_rows_limit() -> int:
    """Lire la limite de lignes historiques, avec une valeur de backup safe."""
    raw_value = get_config_value(
        "SUPABASE_HISTORY_ROWS_LIMIT",
        str(DEFAULT_HISTORY_ROWS_LIMIT),
    )
    try:
        rows_limit = int(raw_value)
    except (TypeError, ValueError):
        rows_limit = DEFAULT_HISTORY_ROWS_LIMIT

    return max(rows_limit, 1000)


@st.cache_data(show_spinner=False, ttl=ACTIVE_CACHE_TTL_SECONDS)
def load_supabase_active() -> pd.DataFrame:
    """Charger les pannes actives et faire fitter les noms des colonnes avec les exports CSV."""
    query = """
        SELECT *
        FROM app_active_outages
        ORDER BY customers_affected DESC NULLS LAST;
    """
    df = load_supabase_query(query)

    if not df.empty:
        # Harmonise les noms de colonnes avec ceux des exports CSV.
        if "active_capture_at" not in df.columns and "latest_row_captured_at" in df.columns:
            df["active_capture_at"] = df["latest_row_captured_at"]

        if (
            "outage_age_hours_at_capture" not in df.columns
            and "outage_age_hours_at_latest_capture" in df.columns
        ):
            df["outage_age_hours_at_capture"] = df["outage_age_hours_at_latest_capture"]

        if (
            "restore_eta_hours_at_capture" not in df.columns
            and "restore_eta_hours_at_latest_capture" in df.columns
        ):
            df["restore_eta_hours_at_capture"] = df["restore_eta_hours_at_latest_capture"]

    return df


@st.cache_data(show_spinner=False, ttl=RECENT_CACHE_TTL_SECONDS)
def load_supabase_latest() -> pd.DataFrame:
    """Charger la dernière observation connue de chaque panne."""
    query = """
        SELECT *
        FROM app_latest_outages
        ORDER BY last_capture_at DESC NULLS LAST, customers_affected DESC NULLS LAST;
    """
    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=RECENT_CACHE_TTL_SECONDS)
def load_supabase_recent_outages(limit: int = 25) -> pd.DataFrame:
    """Charger seulement les dernières pannes nécessaires à la page Surveillance."""
    safe_limit = max(1, min(int(limit), 100))
    query = f"""
        SELECT *
        FROM app_latest_outages
        ORDER BY first_capture_at DESC NULLS LAST
        LIMIT {safe_limit};
    """
    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=QUALITY_CACHE_TTL_SECONDS)
def load_supabase_latest_metrics() -> pd.DataFrame:
    """Retourner uniquement les taux nécessaires à la page Qualité."""
    query = """
        SELECT
            100.0 * AVG(CASE WHEN is_geocoded IS TRUE THEN 1.0 ELSE 0.0 END)
                AS geocoded_rate_pct,
            100.0 * AVG(CASE WHEN has_known_cause IS TRUE THEN 1.0 ELSE 0.0 END)
                AS known_cause_rate_pct
        FROM app_latest_outages;
    """
    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=RECENT_CACHE_TTL_SECONDS)
def load_supabase_history() -> pd.DataFrame:
    """Charger une fenêtre bornée de l’historique brut enrichi."""
    days = get_supabase_history_days()
    rows_limit = get_supabase_history_rows_limit()

    query = f"""
        WITH bounds AS (
            SELECT MAX(captured_at) AS max_captured_at
            FROM raw_outage_snapshots
            WHERE captured_at IS NOT NULL
        )
        SELECT
            r.outage_id,
            r.customers_affected,
            r.start_time,
            r.estimated_restore,
            r.status_code,
            r.status,
            r.cause_code,
            r.cause_label,
            r.municipality_id,
            r.captured_at,
            r.lon,
            r.lat,
            COALESCE(
                m.municipality_label,
                'Municipalité ' || CAST(r.municipality_id AS TEXT)
            ) AS municipality_label,
            m.municipality_name,
            m.municipality_full_name,
            m.mrc_name,
            m.region_name,
            m.is_geocoded
        FROM raw_outage_snapshots r
        LEFT JOIN dim_municipalities m
            ON r.municipality_id = m.municipality_id
        CROSS JOIN bounds b
        WHERE r.captured_at IS NOT NULL
          AND r.captured_at >= b.max_captured_at - INTERVAL '{days} days'
        ORDER BY r.captured_at DESC
        LIMIT {rows_limit};
    """

    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=DAILY_CACHE_TTL_SECONDS)
def load_supabase_daily_summary() -> pd.DataFrame:
    """Charger les agrégats quotidiens utilisés par les graphiques."""
    query = """
        SELECT *
        FROM app_daily_summary
        ORDER BY date;
    """
    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=QUALITY_CACHE_TTL_SECONDS)
def load_supabase_quality_report() -> pd.DataFrame:
    """Charger le rapport de qualité, trié par sévérité."""
    query = """
        SELECT *
        FROM app_data_quality_report
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 1
                WHEN 'warning' THEN 2
                WHEN 'info' THEN 3
                ELSE 4
            END,
            check_name;
    """

    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=PIPELINE_HEALTH_CACHE_TTL_SECONDS)
def load_supabase_pipeline_health() -> pd.DataFrame:
    """Charger un instantané léger de la santé opérationnelle du pipeline."""
    sql_path = Path(__file__).resolve().parents[1] / "sql" / "postgres" / "pipeline_health_snapshot.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return load_supabase_query(sql_path.read_text(encoding="utf-8"))


def clear_dashboard_caches() -> None:
    """Vider tous les caches de données du dashboard pour la source active."""
    if using_supabase():
        for loader in (
            load_supabase_active,
            load_supabase_latest,
            load_supabase_recent_outages,
            load_supabase_latest_metrics,
            load_supabase_history,
            load_supabase_daily_summary,
            load_supabase_quality_report,
            load_supabase_pipeline_health,
        ):
            loader.clear()
    else:
        load_csv.clear()
