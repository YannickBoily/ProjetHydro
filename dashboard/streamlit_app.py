"""Tableau de bord Streamlit consacré aux pannes électriques au Québec.

L'application peut charger les données depuis des fichiers CSV locaux ou depuis
une base PostgreSQL hébergée sur Supabase. Elle fournit des vues opérationnelles,
cartographiques et historiques, ainsi que des contrôles de qualité et des exports.

Le fichier reste autonome pour faciliter son déploiement sur
Streamlit Community Cloud. Les fonctions sont regroupées par responsabilité :
chargement, normalisation, enrichissement, rendu des composants et pages.
"""

from __future__ import annotations

import html
import os
import re
import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st


# =============================================================================
# Configuration générale
# =============================================================================


ROOT_DIR = Path(__file__).resolve().parents[1]

RAW_FILE = ROOT_DIR / "data" / "raw" / "hydroquebec_history.csv"
ACTIVE_FILE = ROOT_DIR / "data" / "processed" / "active_outages.csv"
LATEST_FILE = ROOT_DIR / "data" / "processed" / "latest_outages.csv"
DAILY_FILE = ROOT_DIR / "data" / "processed" / "daily_summary.csv"
QUALITY_FILE = ROOT_DIR / "data" / "processed" / "data_quality_report.csv"

st.set_page_config(
    page_title="Hydro-Québec | Suivi des pannes",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# Style visuel
# =============================================================================
st.markdown(
    """
<style>
    :root {
        --hq-bg: #0b0f15;
        --hq-panel: #111722;
        --hq-panel-soft: #0f141d;
        --hq-line: rgba(148, 163, 184, 0.18);
        --hq-line-strong: rgba(148, 163, 184, 0.30);
        --hq-text: #f8fafc;
        --hq-muted: #94a3b8;
        --hq-accent: #ff4b4b;
        --hq-good: #4ade80;
        --hq-warning: #fbbf24;
        --hq-danger: #fb7185;
    }

    .stApp {
        background: var(--hq-bg);
    }

    .block-container {
        max-width: 1420px;
        /* Le header Streamlit est superposé au contenu principal. */
        padding-top: 4.25rem;
        padding-bottom: 3rem;
    }

    [data-testid="stHeader"] {
        background: rgba(11, 15, 21, 0.96);
        border-bottom: 1px solid rgba(148, 163, 184, 0.08);
    }

    [data-testid="stSidebar"] {
        background: #0d121a;
        border-right: 1px solid var(--hq-line);
    }

    [data-testid="stSidebar"] .block-container {
        padding-top: 1.2rem;
    }

    [data-testid="stSidebar"] div[role="radiogroup"] > label {
        border-radius: 10px;
        padding: 0.50rem 0.65rem;
        margin-bottom: 0.18rem;
        transition: background 120ms ease;
    }

    [data-testid="stSidebar"] div[role="radiogroup"] > label:hover {
        background: rgba(148, 163, 184, 0.08);
    }

    [data-testid="stMetric"] {
        background: var(--hq-panel);
        border: 1px solid var(--hq-line);
        padding: 0.95rem 1.05rem;
        border-radius: 14px;
        box-shadow: none;
        min-height: 104px;
    }

    [data-testid="stMetricLabel"] {
        color: var(--hq-muted);
        font-size: 0.82rem;
        letter-spacing: 0.01em;
    }

    [data-testid="stMetricValue"] {
        color: var(--hq-text);
        font-size: 1.85rem;
        font-weight: 780;
        letter-spacing: -0.03em;
    }

    [data-testid="stMetricDelta"] {
        font-size: 0.78rem;
    }

    div[data-testid="stVerticalBlockBorderWrapper"] {
        border-color: var(--hq-line) !important;
        border-radius: 16px !important;
        background: rgba(17, 23, 34, 0.40);
    }

    .app-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1.5rem;
        padding: 0.35rem 0 1.15rem 0;
        border-bottom: 1px solid var(--hq-line);
        margin-bottom: 1.35rem;
    }

    .app-title {
        color: var(--hq-text);
        font-size: 1.70rem;
        line-height: 1.15;
        font-weight: 800;
        letter-spacing: -0.035em;
        margin: 0;
    }

    .app-subtitle {
        color: var(--hq-muted);
        font-size: 0.92rem;
        margin-top: 0.35rem;
    }

    .app-meta {
        display: flex;
        flex-wrap: wrap;
        justify-content: flex-end;
        gap: 0.45rem;
        max-width: 520px;
    }

    .badge {
        display: inline-flex;
        align-items: center;
        border: 1px solid var(--hq-line);
        border-radius: 999px;
        padding: 0.38rem 0.68rem;
        color: #cbd5e1;
        background: rgba(148, 163, 184, 0.06);
        font-size: 0.78rem;
        white-space: nowrap;
    }

    .badge-accent {
        border-color: rgba(255, 75, 75, 0.36);
        background: rgba(255, 75, 75, 0.09);
        color: #fecaca;
    }

    .page-head {
        margin-bottom: 1.20rem;
    }

    .page-eyebrow {
        color: var(--hq-accent);
        text-transform: uppercase;
        font-weight: 750;
        font-size: 0.72rem;
        letter-spacing: 0.10em;
        margin-bottom: 0.35rem;
    }

    .page-title {
        color: var(--hq-text);
        font-size: 2.0rem;
        font-weight: 820;
        line-height: 1.12;
        letter-spacing: -0.04em;
        margin: 0;
    }

    .page-description {
        color: var(--hq-muted);
        max-width: 900px;
        margin-top: 0.48rem;
        font-size: 0.94rem;
        line-height: 1.55;
    }

    .section-head {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 1rem;
        margin-top: 1.65rem;
        margin-bottom: 0.65rem;
    }

    .section-title {
        color: var(--hq-text);
        font-size: 1.22rem;
        font-weight: 760;
        margin: 0;
    }

    .section-note {
        color: var(--hq-muted);
        font-size: 0.82rem;
    }

    .status-banner {
        border: 1px solid var(--hq-line);
        border-left-width: 4px;
        border-radius: 12px;
        padding: 0.90rem 1rem;
        margin: 0.75rem 0 1rem 0;
        background: rgba(148, 163, 184, 0.04);
        color: #dbe5f1;
        line-height: 1.5;
    }

    .status-good {
        border-left-color: var(--hq-good);
        background: rgba(74, 222, 128, 0.055);
    }

    .status-warning {
        border-left-color: var(--hq-warning);
        background: rgba(251, 191, 36, 0.055);
    }

    .status-danger {
        border-left-color: var(--hq-danger);
        background: rgba(251, 113, 133, 0.055);
    }

    .priority-list {
        display: flex;
        flex-direction: column;
        gap: 0.42rem;
    }

    .priority-row {
        display: grid;
        grid-template-columns: 1fr auto;
        gap: 0.8rem;
        align-items: center;
        padding: 0.72rem 0;
        border-bottom: 1px solid var(--hq-line);
    }

    .priority-row:last-child {
        border-bottom: 0;
    }

    .priority-name {
        color: var(--hq-text);
        font-weight: 680;
        font-size: 0.91rem;
    }

    .priority-meta {
        color: var(--hq-muted);
        font-size: 0.78rem;
        margin-top: 0.16rem;
    }

    .priority-value {
        color: var(--hq-text);
        font-weight: 780;
        font-size: 1.0rem;
        white-space: nowrap;
    }

    .filter-summary {
        color: var(--hq-muted);
        font-size: 0.82rem;
        padding: 0.45rem 0 0.3rem 0;
    }

    .muted {
        color: var(--hq-muted);
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid var(--hq-line);
        border-radius: 12px;
        overflow: hidden;
    }

    .stDownloadButton button,
    .stButton button {
        border-radius: 10px;
        border-color: var(--hq-line-strong);
    }

    hr {
        border-color: var(--hq-line) !important;
    }

    @media (max-width: 900px) {
        .block-container {
            padding-top: 4rem;
        }
        .app-header {
            display: block;
        }
        .app-meta {
            justify-content: flex-start;
            margin-top: 0.8rem;
        }
        .page-title {
            font-size: 1.65rem;
        }
    }
</style>
""",
    unsafe_allow_html=True,
)


# Traductions

CAUSE_TRANSLATIONS = {
    "unknown": "Inconnue",
    "other": "Autre",
    "equipment": "Bris d’équipement",
    "vegetation": "Végétation",
    "accident": "Accident",
    "weather": "Conditions météorologiques",
    "animal": "Animal",
}

STATUS_TRANSLATIONS = {
    "new": "Nouvelle panne",
    "assigned": "Travaux assignés",
    "en_route": "Équipe en route",
    "working": "Équipe au travail",
}

QUALITY_TRANSLATIONS = {
    "pass": "Réussi",
    "fail": "Échec",
    "info": "Information",
    "critical": "Critique",
    "warning": "Avertissement",
}

CHECK_TRANSLATIONS = {
    "missing_outage_id": "ID de panne manquant",
    "missing_captured_at": "Moment de capture manquant",
    "negative_customers_affected": "Clients affectés négatifs",
    "invalid_coordinates": "Coordonnées invalides",
    "estimated_restore_before_start_time": "Rétablissement estimé avant le début",
    "captured_at_before_start_time": "Capture avant le début de la panne",
    "duplicate_outage_id_captured_at": "Doublon panne + capture",
    "unknown_cause_rows": "Cause inconnue",
}

COLUMN_LABELS = {
    "outage_id": "ID de panne",
    "short_outage_id": "ID court",
    "customers_affected": "Clients affectés",
    "start_time": "Début",
    "estimated_restore": "Rétablissement estimé",
    "status_fr": "Statut",
    "analysis_cause_label_fr": "Cause",
    "latest_raw_cause_label_fr": "Cause brute",
    "history_cause_label_fr": "Cause",
    "municipality_label": "Municipalité",
    "mrc_name": "MRC",
    "region_name": "Région",
    "active_capture_at": "Capture active",
    "latest_row_captured_at": "Dernière capture",
    "captured_at": "Capture",
    "first_capture_at": "Première capture",
    "last_capture_at": "Dernière capture",
    "capture_count": "Captures",
    "observed_duration_hours": "Durée observée, h",
    "outage_age_hours_at_capture": "Âge, h",
    "restore_eta_hours_at_capture": "ETA rétablissement, h",
    "lon": "Longitude",
    "lat": "Latitude",
    "is_major_outage_fr": "Panne majeure",
    "has_known_cause_fr": "Cause connue",
    "is_geocoded_fr": "Géocodée",
    "date": "Date",
    "max_active_outages_estimate": "Pannes actives max",
    "max_customers_affected": "Clients affectés max",
    "new_outages_detected": "Nouvelles pannes",
    "max_municipalities_affected": "Municipalités touchées max",
    "max_major_outages": "Pannes majeures max",
    "snapshots_count": "Captures",
    "avg_active_outages_estimate": "Pannes actives moy.",
    "avg_customers_affected": "Clients affectés moy.",
    "check_name_fr": "Contrôle",
    "severity_fr": "Sévérité",
    "status_quality_fr": "Statut",
    "rows_affected": "Lignes affectées",
    "failed_rate_pct": "Taux affecté, %",
    "description": "Description",
}

PLOT_TEMPLATE = "plotly_dark"
SOURCE_LIMIT_CHECKS = {"unknown_cause_rows"}
QUEBEC_TIMEZONE = "America/Toronto"
CACHE_TTL_SECONDS = 900
ACTIVE_CACHE_TTL_SECONDS = 900
RECENT_CACHE_TTL_SECONDS = 3600
DAILY_CACHE_TTL_SECONDS = 21600
QUALITY_CACHE_TTL_SECONDS = 21600
DEFAULT_HISTORY_DAYS = 90
DEFAULT_HISTORY_ROWS_LIMIT = 10_000

TIMESTAMP_COLUMNS = (
    "start_time",
    "estimated_restore",
    "captured_at",
    "active_capture_at",
    "latest_row_captured_at",
    "first_capture_at",
    "last_capture_at",
    "known_cause_last_seen_at",
    "created_at",
)

NUMERIC_COLUMNS = (
    "customers_affected",
    "municipality_id",
    "capture_count",
    "observed_duration_hours",
    "outage_age_hours_at_capture",
    "outage_age_hours_at_latest_capture",
    "restore_eta_hours_at_capture",
    "restore_eta_hours_at_latest_capture",
    "lon",
    "lat",
    "rows_affected",
    "total_rows",
    "failed_rate_pct",
    "snapshots_count",
    "max_active_outages_estimate",
    "avg_active_outages_estimate",
    "max_customers_affected",
    "avg_customers_affected",
    "max_municipalities_affected",
    "max_major_outages",
    "new_outages_detected",
    "raw_rows_count",
    "unique_outages_observed",
    "unknown_cause_rows",
    "municipalities_observed",
)


# Chargement et normalisation des données
def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliser les types communs aux sources CSV et Supabase."""
    if df.empty:
        return df

    normalized = df.copy()

    # Les heures sont stockés en UTC par la source, puis présentés dans
    # le fuseau du Québec. Le fuseau reste attaché pour prévenir tout décalage.
    for column in TIMESTAMP_COLUMNS:
        if column in normalized.columns:
            normalized[column] = (
                pd.to_datetime(normalized[column], errors="coerce", utc=True)
                .dt.tz_convert(QUEBEC_TIMEZONE)
            )

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


# Accès à Supabase / PostgreSQL

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


def translate_text(
    value: Any,
    mapping: dict[str, str],
    default: str = "Inconnue",
) -> str:
    """Traduire une valeur source tout en conservant les libellés inconnus."""
    if pd.isna(value):
        return default

    key = str(value).strip().lower()
    if key == "":
        return default

    return mapping.get(key, str(value).strip())


def yes_no(value: Any) -> str:
    """Convertir une valeur booléenne courante en libellé français."""
    return "Oui" if str(value).lower() in {"true", "1", "yes"} else "Non"


def short_id(value: Any, max_len: int = 18) -> str:
    """Raccourcir un identifiant pour l’affichage sans modifier sa valeur source."""
    if pd.isna(value):
        return ""
    text = str(value)
    return text if len(text) <= max_len else text[:max_len] + "…"


def add_display_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ajouter les colonnes lisibles destinées à l’interface utilisateur."""
    if df.empty:
        return df

    df = df.copy()

    if "outage_id" in df.columns:
        df["short_outage_id"] = df["outage_id"].apply(short_id)

    if "analysis_cause_label" in df.columns:
        df["analysis_cause_label_fr"] = df["analysis_cause_label"].apply(
            lambda x: translate_text(x, CAUSE_TRANSLATIONS)
        )

    if "latest_raw_cause_label" in df.columns:
        df["latest_raw_cause_label_fr"] = df["latest_raw_cause_label"].apply(
            lambda x: translate_text(x, CAUSE_TRANSLATIONS)
        )

    if "cause_label" in df.columns:
        df["history_cause_label_fr"] = df["cause_label"].apply(
            lambda x: translate_text(x, CAUSE_TRANSLATIONS)
        )

    if "status" in df.columns:
        df["status_fr"] = df["status"].apply(
            lambda x: translate_text(x, STATUS_TRANSLATIONS, default="Non disponible")
        )

    if "has_known_cause" in df.columns:
        df["has_known_cause_fr"] = df["has_known_cause"].apply(yes_no)

    if "is_geocoded" in df.columns:
        df["is_geocoded_fr"] = df["is_geocoded"].apply(yes_no)

    if "is_major_outage" in df.columns:
        df["is_major_outage_fr"] = df["is_major_outage"].apply(yes_no)

    if "municipality_label" not in df.columns and "municipality_id" in df.columns:
        df["municipality_label"] = "Municipalité " + df["municipality_id"].astype(str)

    for col in ["lat", "lon"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


    return df


def prepare_quality_report(df: pd.DataFrame) -> pd.DataFrame:
    """Ajouter les libellés français au petit rapport de qualité chargé à la demande."""
    if df is None or df.empty:
        return pd.DataFrame()

    quality_df = df.copy()

    if "check_name" in quality_df.columns:
        quality_df["check_name_fr"] = quality_df["check_name"].apply(
            lambda x: translate_text(x, CHECK_TRANSLATIONS, default=str(x))
        )
        quality_df["description_fr"] = quality_df["check_name"].map(
            QUALITY_DESCRIPTION_FR
        ).fillna(quality_df.get("description", ""))

    if "severity" in quality_df.columns:
        quality_df["severity_fr"] = quality_df["severity"].apply(
            lambda x: translate_text(x, QUALITY_TRANSLATIONS, default=str(x))
        )

    if "status" in quality_df.columns:
        quality_df["status_quality_fr"] = quality_df["status"].apply(
            lambda x: translate_text(x, QUALITY_TRANSLATIONS, default=str(x))
        )

    return quality_df


DATA_REQUEST_DATASETS = [
    "Historique complet des observations",
    "Dernières observations par panne",
    "Pannes actives",
    "Sommaire quotidien",
    "Rapport de qualité",
    "Autre / besoin spécifique",
]

DATA_REQUEST_USE_CASES = [
    "Recherche",
    "Analyse de données",
    "Projet étudiant",
    "Projet professionnel",
    "Journalisme / média",
    "Autre",
]


def _config_bool(name: str, default: bool = False) -> bool:
    """Lire une option booléenne depuis l'environnement ou les secrets Streamlit."""
    value = get_config_value(name, str(default))
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def data_request_email_is_configured() -> bool:
    """Vérifier que le minimum requis pour envoyer une demande par courriel est présent."""
    recipient = str(
        get_config_value("DATA_REQUEST_TO_EMAIL", "")
        or get_config_value("DATA_CONTACT_EMAIL", "")
        or ""
    ).strip()
    smtp_host = str(get_config_value("SMTP_HOST", "") or "").strip()
    smtp_from = str(
        get_config_value("SMTP_FROM_EMAIL", "")
        or get_config_value("SMTP_USERNAME", "")
        or ""
    ).strip()
    return bool(recipient and smtp_host and smtp_from)


def is_valid_email(value: str) -> bool:
    """Validation légère d'une adresse courriel avant l'envoi SMTP."""
    value = str(value or "").strip()
    if len(value) > 254 or "\n" in value or "\r" in value:
        return False
    return bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value))


def send_data_request_email(
    requester_name: str,
    requester_email: str,
    organization: str,
    requested_dataset: str,
    use_case: str,
    details: str,
) -> None:
    """Envoyer une demande d'accès sans déclencher de requête d'export Supabase."""
    recipient = str(
        get_config_value("DATA_REQUEST_TO_EMAIL", "")
        or get_config_value("DATA_CONTACT_EMAIL", "")
        or ""
    ).strip()
    smtp_host = str(get_config_value("SMTP_HOST", "") or "").strip()
    smtp_port = int(get_config_value("SMTP_PORT", "587"))
    smtp_username = str(get_config_value("SMTP_USERNAME", "") or "").strip()
    smtp_password = str(get_config_value("SMTP_PASSWORD", "") or "")
    smtp_from = str(
        get_config_value("SMTP_FROM_EMAIL", "")
        or smtp_username
        or ""
    ).strip()
    use_ssl = _config_bool("SMTP_USE_SSL", False)
    use_tls = _config_bool("SMTP_USE_TLS", not use_ssl)

    if not recipient or not smtp_host or not smtp_from:
        raise RuntimeError(
            "La configuration SMTP est incomplète. "
            "Vérifie DATA_REQUEST_TO_EMAIL, SMTP_HOST et SMTP_FROM_EMAIL."
        )

    # Ne jamais placer une valeur utilisateur non filtrée dans un en-tête.
    safe_name = requester_name.replace("\r", " ").replace("\n", " ").strip()
    safe_reply_to = requester_email.replace("\r", "").replace("\n", "").strip()

    message = EmailMessage()
    message["Subject"] = f"Demande d'accès aux données — {requested_dataset}"
    message["From"] = smtp_from
    message["To"] = recipient
    message["Reply-To"] = safe_reply_to

    submitted_at = datetime.now().astimezone().isoformat(timespec="seconds")
    message.set_content(
        "Nouvelle demande d'accès aux données du projet Hydro-Québec\n\n"
        f"Nom : {safe_name}\n"
        f"Courriel : {safe_reply_to}\n"
        f"Organisation : {organization.strip() or 'Non précisée'}\n"
        f"Données demandées : {requested_dataset}\n"
        f"Utilisation prévue : {use_case}\n"
        f"Date de la demande : {submitted_at}\n\n"
        "Détails :\n"
        f"{details.strip() or 'Aucun détail supplémentaire.'}\n"
    )

    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            smtp_host,
            smtp_port,
            timeout=20,
            context=context,
        ) as server:
            if smtp_username:
                server.login(smtp_username, smtp_password)
            server.send_message(message)
        return

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
        server.ehlo()
        if use_tls:
            context = ssl.create_default_context()
            server.starttls(context=context)
            server.ehlo()
        if smtp_username:
            server.login(smtp_username, smtp_password)
        server.send_message(message)


def render_full_data_access() -> None:
    """Afficher un formulaire de demande sans exposer de téléchargement direct."""
    render_section_header("Accès aux données", "Sur demande seulement")
    st.info(
        "Le téléchargement direct est désactivé afin de préserver les ressources du "
        "tableau de bord et de la base de données. Une demande peut être envoyée ici; "
        "aucun export Supabase n'est généré automatiquement lors de l'envoi."
    )

    if not data_request_email_is_configured():
        st.warning(
            "Le formulaire est prêt, mais l'envoi de courriel n'est pas encore configuré. "
            "Ajoute les paramètres SMTP dans les secrets Streamlit."
        )
        return

    with st.form("data_access_request_form", clear_on_submit=False):
        left, right = st.columns(2)
        with left:
            requester_name = st.text_input(
                "Nom *",
                max_chars=120,
                placeholder="Votre nom",
            )
            requester_email = st.text_input(
                "Courriel *",
                max_chars=254,
                placeholder="nom@exemple.com",
            )
            organization = st.text_input(
                "Organisation",
                max_chars=160,
                placeholder="Université, entreprise, média… (facultatif)",
            )

        with right:
            requested_dataset = st.selectbox(
                "Données souhaitées *",
                DATA_REQUEST_DATASETS,
            )
            use_case = st.selectbox(
                "Utilisation prévue *",
                DATA_REQUEST_USE_CASES,
            )

        details = st.text_area(
            "Décrivez brièvement votre besoin *",
            max_chars=2000,
            placeholder=(
                "Ex. période recherchée, variables nécessaires, objectif de l'analyse, "
                "format souhaité…"
            ),
            height=140,
        )
        consent = st.checkbox(
            "J'accepte d'être contacté par courriel au sujet de cette demande."
        )
        submitted = st.form_submit_button(
            "✉️ Envoyer la demande",
            width="stretch",
        )

    if not submitted:
        return

    errors = []
    if not requester_name.strip():
        errors.append("Indiquez votre nom.")
    if not is_valid_email(requester_email):
        errors.append("Indiquez une adresse courriel valide.")
    if not details.strip():
        errors.append("Décrivez brièvement votre besoin.")
    if not consent:
        errors.append("Vous devez accepter d'être contacté au sujet de la demande.")

    if errors:
        for error in errors:
            st.error(error)
        return

    # Petit garde-fou contre les doubles clics / renvois accidentels dans la même session.
    now_ts = datetime.now().timestamp()
    last_sent = float(st.session_state.get("data_request_last_sent_at", 0) or 0)
    if now_ts - last_sent < 60:
        st.warning("Une demande vient déjà d'être envoyée. Réessayez dans une minute.")
        return

    try:
        with st.spinner("Envoi de la demande…"):
            send_data_request_email(
                requester_name=requester_name,
                requester_email=requester_email,
                organization=organization,
                requested_dataset=requested_dataset,
                use_case=use_case,
                details=details,
            )
    except Exception as exc:
        print(f"Data request email error: {type(exc).__name__}: {exc}")
        st.error(
            "La demande n'a pas pu être envoyée pour le moment. "
            "Réessayez plus tard ou contactez le responsable du projet."
        )
        return

    st.session_state["data_request_last_sent_at"] = now_ts
    st.success(
        "Demande envoyée. Vous recevrez une réponse à l'adresse indiquée "
        "si l'accès aux données peut être accordé."
    )


def enrich_raw_history(raw_df: pd.DataFrame, latest_df: pd.DataFrame) -> pd.DataFrame:
    """Compléter l’historique avec les métadonnées territoriales disponibles."""
    if raw_df.empty:
        return raw_df

    history = raw_df.copy()

    lookup_cols = [
        "municipality_id",
        "municipality_label",
        "municipality_name",
        "mrc_name",
        "region_name",
        "is_geocoded",
        "is_geocoded_fr",
    ]

    available_lookup_cols = [col for col in lookup_cols if col in latest_df.columns]

    if "municipality_id" in history.columns and "municipality_id" in available_lookup_cols:
        lookup = (
            latest_df[available_lookup_cols]
            .dropna(subset=["municipality_id"])
            .drop_duplicates("municipality_id")
        )

        columns_to_add = [col for col in available_lookup_cols if col != "municipality_id"]
        history = history.drop(
            columns=[col for col in columns_to_add if col in history.columns],
            errors="ignore",
        )

        history = history.merge(
            lookup,
            on="municipality_id",
            how="left",
        )

    if "municipality_label" not in history.columns and "municipality_id" in history.columns:
        history["municipality_label"] = "Municipalité " + history["municipality_id"].astype(str)

    return add_display_columns(history)


def prepare_display_table(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Prepare a dataframe for safe Streamlit display."""
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    if columns is not None:
        selected = [col for col in columns if col in out.columns]
        out = out[selected]

    pairs = {
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

    for readable, raw_col in pairs.items():
        if readable in out.columns and raw_col in out.columns:
            out = out.drop(columns=[raw_col])

    out = out.rename(columns=COLUMN_LABELS)
    clean_columns = []
    seen = {}

    for idx, col in enumerate(out.columns):
        name = "" if col is None else str(col).strip()
        if not name:
            name = f"Colonne {idx + 1}"

        if name not in seen:
            seen[name] = 1
            clean_columns.append(name)
        else:
            seen[name] += 1
            clean_columns.append(f"{name} ({seen[name]})")

    out.columns = clean_columns

    # Reset index so Streamlit does not try to render a complex/pinned index column.
    out = out.reset_index(drop=True)

    return out


def show_table(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    height: int | str = "auto",
) -> None:
    """Display a dataframe while avoiding Streamlit frontend grid crashes."""
    display_df = prepare_display_table(df, columns)

    if display_df.empty:
        st.info("Aucune donnée à afficher selon les filtres actuels.")
        return

    max_display_rows = 250

    if len(display_df) > max_display_rows:
        st.caption(
            f"Affichage des {max_display_rows:,} premières lignes sur {len(display_df):,}. "
            "L'accès au jeu complet peut être demandé avec le formulaire prévu à cet effet."
        )
        display_df = display_df.head(max_display_rows)

    try:
        st.dataframe(
            display_df,
            width="stretch",
            height=height,
            hide_index=True,
        )
    except Exception:
        st.warning(
            "Le tableau interactif n'a pas pu être affiché. "
            "Affichage d'une version simplifiée."
        )
        st.markdown(
            display_df.head(200).to_html(index=False, escape=True),
            unsafe_allow_html=True,
        )


def format_int(value: Any) -> str:
    """Formater une valeur numérique comme entier avec séparateurs français."""
    if pd.isna(value):
        return "0"
    return f"{int(round(float(value))):,}".replace(",", " ")


def format_pct(value: Any) -> str:
    """Formater une valeur numérique en pourcentage à une décimale."""
    if pd.isna(value):
        return "0 %"
    return f"{float(value):.1f} %"


def bool_rate(series: pd.Series) -> float:
    """Calculer le pourcentage de valeurs interprétées comme vraies."""
    if series.empty:
        return 0.0
    return round(series.astype(str).str.lower().isin(["true", "1", "yes"]).mean() * 100, 2)


def latest_timestamp(*frames: pd.DataFrame):
    """Retourner l’horodatage le plus récent parmi plusieurs DataFrames."""
    values = []

    for df in frames:
        if df.empty:
            continue
        for col in [
            "active_capture_at",
            "latest_row_captured_at",
            "last_capture_at",
            "captured_at",
        ]:
            if col in df.columns and not df[col].dropna().empty:
                values.append(df[col].max())

    if not values:
        return None

    return max(values)


def get_geo(df: pd.DataFrame) -> pd.DataFrame:
    """Conserver uniquement les observations possédant des coordonnées valides."""
    if df.empty or "lat" not in df.columns or "lon" not in df.columns:
        return pd.DataFrame()

    return df.dropna(subset=["lat", "lon"]).copy()


def ensure_quebec_timestamp(value: Any) -> pd.Timestamp | None:
    """Convertir une valeur en horodatage conscient du fuseau du Québec."""
    if value is None or pd.isna(value):
        return None

    timestamp = pd.Timestamp(value)

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")

    return timestamp.tz_convert(QUEBEC_TIMEZONE)


def format_quebec_datetime(value: Any) -> str:
    """Format a timestamp in Quebec time without depending on OS locale."""
    timestamp = ensure_quebec_timestamp(value)
    if timestamp is None:
        return "Non disponible"

    timezone_label = {
        "EST": "HNE",
        "EDT": "HAE",
    }.get(timestamp.tzname(), timestamp.tzname() or "")

    return f"{timestamp:%Y-%m-%d à %H:%M} {timezone_label}".strip()


def get_cause_column(df: pd.DataFrame) -> str | None:
    """Choose the readable cause column available in a dataframe."""
    for col in ["analysis_cause_label_fr", "history_cause_label_fr", "latest_raw_cause_label_fr"]:
        if col in df.columns:
            return col
    return None


def make_download(df: pd.DataFrame, label: str, filename: str):
    """Ne pas exposer de téléchargement direct; l'accès passe par le formulaire."""
    if df is None or df.empty:
        return

    st.caption(
        "Téléchargement direct désactivé — utilisez le formulaire de demande "
        "d'accès aux données ci-dessous."
    )


def build_active_snapshot_at_time(
    history_df: pd.DataFrame,
    selected_capture_at: pd.Timestamp,
    window_minutes: int = 5,
) -> pd.DataFrame:
    """Reconstruire un instantané autour d’une capture historique donnée."""
    if history_df.empty or "captured_at" not in history_df.columns:
        return pd.DataFrame()

    history = history_df.dropna(subset=["captured_at"]).copy()

    window_start = selected_capture_at - pd.Timedelta(minutes=window_minutes)
    window_end = selected_capture_at + pd.Timedelta(minutes=window_minutes)

    snapshot = history[
        (history["captured_at"] >= window_start)
        & (history["captured_at"] <= window_end)
    ].copy()

    if snapshot.empty:
        return snapshot

    if "outage_id" in snapshot.columns:
        snapshot = (
            snapshot.sort_values("captured_at")
            .groupby("outage_id", as_index=False)
            .tail(1)
        )

    return snapshot


def apply_global_filters_to_history(snapshot: pd.DataFrame) -> pd.DataFrame:
    """Appliquer à l’historique les filtres actifs de la barre latérale."""
    if snapshot.empty:
        return snapshot

    out = snapshot.copy()

    if "customers_affected" in out.columns:
        out = out[out["customers_affected"].fillna(0) >= min_customers]

        if major_only:
            out = out[out["customers_affected"].fillna(0) >= major_threshold]

    if not include_unknown and "history_cause_label_fr" in out.columns:
        out = out[out["history_cause_label_fr"] != "Inconnue"]

    if selected_regions and "region_name" in out.columns:
        out = out[out["region_name"].isin(selected_regions)]

    if selected_mrcs and "mrc_name" in out.columns:
        out = out[out["mrc_name"].isin(selected_mrcs)]

    if selected_municipalities and "municipality_label" in out.columns:
        out = out[out["municipality_label"].isin(selected_municipalities)]

    if selected_causes and "history_cause_label_fr" in out.columns:
        out = out[out["history_cause_label_fr"].isin(selected_causes)]

    return out


# =============================================================================
# Composants visuels et agrégations du tableau de bord
# =============================================================================

ACCENT_COLOR = "#ff4b4b"
MAP_STYLE = "carto-darkmatter"
MAP_MARKER_MIN_SIZE = 10
MAP_MARKER_MAX_SIZE = 38

CAUSE_COLORS = {
    "Inconnue": "#64748b",
    "Autre": "#a78bfa",
    "Bris d’équipement": "#f59e0b",
    "Végétation": "#22c55e",
    "Accident": "#ef4444",
    "Conditions météorologiques": "#38bdf8",
    "Animal": "#f472b6",
}

QUALITY_DESCRIPTION_FR = {
    "missing_outage_id": "Chaque observation doit posséder un identifiant de panne.",
    "missing_captured_at": "Chaque observation doit contenir un moment de capture.",
    "negative_customers_affected": "Le nombre de clients affectés ne peut pas être négatif.",
    "invalid_coordinates": (
        "Les coordonnées doivent se trouver dans une plage géographique valide."
    ),
    "estimated_restore_before_start_time": (
        "Le rétablissement estimé ne doit pas précéder le début de la panne."
    ),
    "captured_at_before_start_time": (
        "La capture ne doit pas précéder le début déclaré de la panne."
    ),
    "duplicate_outage_id_captured_at": (
        "Une panne ne doit apparaître qu’une fois par moment de capture."
    ),
    "unknown_cause_rows": "La source ne fournit pas toujours la cause au moment de la capture.",
}


def render_page_header(eyebrow: str, title: str, description: str) -> None:
    """Afficher l’en-tête éditorial d’une page du tableau de bord."""
    st.markdown(
        f"""
        <div class="page-head">
            <div class="page-eyebrow">{html.escape(eyebrow)}</div>
            <h1 class="page-title">{html.escape(title)}</h1>
            <div class="page-description">{html.escape(description)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_header(title: str, note: str | None = None) -> None:
    """Afficher un titre de section accompagné d’une note facultative."""
    note_html = f'<span class="section-note">{html.escape(note)}</span>' if note else ""
    st.markdown(
        f"""
        <div class="section-head">
            <h2 class="section-title">{html.escape(title)}</h2>
            {note_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status(message: str, level: str = "good") -> None:
    """Afficher une bannière de statut selon le niveau demandé."""
    class_name = {
        "good": "status-good",
        "warning": "status-warning",
        "danger": "status-danger",
    }.get(level, "status-good")
    st.markdown(
        f'<div class="status-banner {class_name}">{html.escape(message)}</div>',
        unsafe_allow_html=True,
    )


def unique_outage_count(df: pd.DataFrame) -> int:
    """Compter les pannes uniques, ou les lignes si aucun identifiant n’existe."""
    if df is None or df.empty:
        return 0
    if "outage_id" in df.columns:
        return int(df["outage_id"].nunique())
    return int(len(df))


def safe_numeric_sum(df: pd.DataFrame, column: str) -> float:
    """Additionner une colonne numérique en tolérant les valeurs absentes."""
    if df is None or df.empty or column not in df.columns:
        return 0
    return float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def representative_outages(df: pd.DataFrame) -> pd.DataFrame:
    """Conserver une ligne représentative par panne pour les vues cumulées."""
    if df is None or df.empty or "outage_id" not in df.columns:
        return df.copy() if df is not None else pd.DataFrame()

    out = df.copy()
    if "customers_affected" in out.columns:
        out["customers_affected"] = pd.to_numeric(out["customers_affected"], errors="coerce")
        out = (
            out.sort_values(["outage_id", "customers_affected"], ascending=[True, False])
            .drop_duplicates("outage_id", keep="first")
        )
    elif "captured_at" in out.columns:
        out = out.sort_values("captured_at").drop_duplicates("outage_id", keep="last")
    else:
        out = out.drop_duplicates("outage_id", keep="last")
    return out


def clean_chart_layout(fig, height: int = 420, show_legend: bool = False):
    """Appliquer la mise en forme commune aux graphiques Plotly."""
    fig.update_layout(
        template=PLOT_TEMPLATE,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=height,
        showlegend=show_legend,
        font=dict(family="Inter, Segoe UI, Arial", size=12, color="#cbd5e1"),
        margin=dict(l=8, r=18, t=16, b=8),
        legend=dict(
            title=None,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(148,163,184,0.12)",
        zeroline=False,
        title_font=dict(color="#94a3b8"),
        tickfont=dict(color="#cbd5e1"),
    )
    fig.update_yaxes(
        showgrid=False,
        zeroline=False,
        title_font=dict(color="#94a3b8"),
        tickfont=dict(color="#cbd5e1"),
    )
    return fig


def render_horizontal_ranking(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    height: int = 420,
    max_rows: int = 12,
    axis_title: str = "Clients affectés",
) -> None:
    """Afficher un classement horizontal limité aux premières catégories."""
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("Aucune donnée disponible selon les filtres actuels.")
        return

    chart_df = df.sort_values(value_col, ascending=False).head(max_rows).sort_values(value_col)
    fig = px.bar(
        chart_df,
        x=value_col,
        y=label_col,
        orientation="h",
        text=value_col,
        color_discrete_sequence=[ACCENT_COLOR],
        labels={value_col: axis_title, label_col: ""},
    )
    fig.update_traces(
        texttemplate="%{text:,.0f}",
        textposition="outside",
        cliponaxis=False,
        hovertemplate=f"%{{y}}<br>{axis_title}: %{{x:,.0f}}<extra></extra>",
    )
    fig = clean_chart_layout(fig, height=height)
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_clean_map(
    df: pd.DataFrame,
    height: int = 650,
    max_points: int | None = None,
) -> None:
    """Afficher la carte des pannes avec une taille de point proportionnelle."""
    geo = get_geo(df)
    if geo.empty:
        st.warning("Aucune coordonnée valide selon les filtres actuels.")
        return

    geo = geo.copy()
    if max_points is not None and len(geo) > max_points:
        if "customers_affected" in geo.columns:
            geo = geo.sort_values("customers_affected", ascending=False).head(max_points)
        else:
            geo = geo.head(max_points)
        st.caption(f"Carte limitée aux {len(geo):,} observations les plus importantes.")

    if "customers_affected" in geo.columns:
        customers = (
            pd.to_numeric(geo["customers_affected"], errors="coerce")
            .fillna(0)
            .clip(lower=0)
        )
        # Une racine carrée garde les petites pannes visibles sans écraser
        # l’importance relative des événements majeurs.
        geo["taille_carte"] = customers.pow(0.5) + 4
    else:
        geo["taille_carte"] = 4

    cause_col = get_cause_column(geo)
    if cause_col:
        geo[cause_col] = geo[cause_col].fillna("Inconnue")
    hover_cols = [
        "customers_affected",
        "municipality_label",
        "mrc_name",
        "region_name",
        "status_fr",
        cause_col,
        "captured_at",
        "active_capture_at",
        "start_time",
        "estimated_restore",
    ]
    hover_cols = [col for col in hover_cols if col and col in geo.columns]

    fig = px.scatter_map(
        geo,
        lat="lat",
        lon="lon",
        size="taille_carte",
        size_max=MAP_MARKER_MAX_SIZE,
        color=cause_col,
        color_discrete_map=CAUSE_COLORS,
        hover_data=hover_cols,
        zoom=5,
        height=height,
        labels={
            "analysis_cause_label_fr": "Cause",
            "history_cause_label_fr": "Cause",
            "latest_raw_cause_label_fr": "Cause",
            "taille_carte": "Importance visuelle",
        },
    )
    fig.update_traces(
        marker=dict(sizemin=MAP_MARKER_MIN_SIZE, opacity=0.94),
    )
    fig.update_layout(
        template=PLOT_TEMPLATE,
        map_style=MAP_STYLE,
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=5, b=0),
        legend=dict(
            title=None,
            orientation="h",
            yanchor="bottom",
            y=1.01,
            xanchor="left",
            x=0,
            font=dict(size=10),
        ),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_priority_list(df: pd.DataFrame, rows: int = 6) -> None:
    """Render a compact priority list without Markdown interpreting HTML as code."""
    if df is None or df.empty:
        st.info("Aucune panne à afficher selon les filtres actuels.")
        return

    ordered = df.copy()
    if "customers_affected" in ordered.columns:
        ordered = ordered.sort_values("customers_affected", ascending=False)

    items: list[str] = []
    for _, row in ordered.head(rows).iterrows():
        municipality = html.escape(
            str(row.get("municipality_label", "Municipalité non disponible"))
        )
        region = html.escape(str(row.get("region_name", "Région non disponible")))
        cause = html.escape(
            str(
                row.get(
                    "analysis_cause_label_fr",
                    row.get("history_cause_label_fr", "Cause non disponible"),
                )
            )
        )
        customers = html.escape(format_int(row.get("customers_affected", 0)))

        # Garder chaque bloc sur une seule ligne évite que Markdown transforme
        # les balises indentées en bloc de code.
        items.append(
            '<div class="priority-row">'
            '<div>'
            f'<div class="priority-name">{municipality}</div>'
            f'<div class="priority-meta">{region} · {cause}</div>'
            '</div>'
            f'<div class="priority-value">{customers}</div>'
            '</div>'
        )

    priority_html = '<div class="priority-list">' + ''.join(items) + '</div>'
    st.markdown(priority_html, unsafe_allow_html=True)


def active_filter_summary() -> str:
    """Résumer les filtres actifs dans une phrase compacte."""
    labels = []
    if min_customers > 0:
        labels.append(f"≥ {format_int(min_customers)} clients")
    if major_only:
        labels.append("pannes majeures seulement")
    if not include_unknown:
        labels.append("causes connues seulement")
    if selected_regions:
        labels.append(f"{len(selected_regions)} région(s)")
    if selected_mrcs:
        labels.append(f"{len(selected_mrcs)} MRC")
    if selected_municipalities:
        labels.append(f"{len(selected_municipalities)} municipalité(s)")
    if selected_causes:
        labels.append(f"{len(selected_causes)} cause(s)")
    return " · ".join(labels) if labels else "Aucun filtre actif"



# =============================================================================
# Navigation et chargement paresseux des données
# =============================================================================

DATA_SOURCE = "Supabase" if using_supabase() else "CSV"

st.sidebar.markdown("## ⚡ Pannes Québec")
st.sidebar.caption("Suivi opérationnel et analytique")

PAGE_OPTIONS = [
    "Vue d’ensemble",
    "Explorer la carte",
    "Analyse territoriale",
    "Causes",
    "Surveillance",
    "Qualité des données",
    "Données",
]

page = st.sidebar.radio(
    "Navigation",
    PAGE_OPTIONS,
    label_visibility="collapsed",
    key="navigation_page",
)

st.sidebar.divider()
st.sidebar.caption(f"Source : {DATA_SOURCE}")

if st.sidebar.button("🔄 Actualiser la situation actuelle", width="stretch"):
    if using_supabase():
        load_supabase_active.clear()
    else:
        load_csv.clear()
    st.rerun()

if using_supabase():
    st.sidebar.caption(
        "L'historique complet n'est pas chargé dans le tableau de bord public."
    )

# La situation actuelle est la seule source chargée sur toutes les pages.
if using_supabase():
    active = add_display_columns(load_supabase_active())
else:
    active = add_display_columns(load_csv(ACTIVE_FILE))

if active.empty:
    st.error(
        "Les données de pannes actives sont manquantes ou indisponibles. "
        "Vérifie la synchronisation des données puis réessaie."
    )
    st.stop()

# Les jeux de données plus lourds restent vides tant que la page ne les demande pas.
latest = pd.DataFrame()
daily = pd.DataFrame()
quality = pd.DataFrame()
recent_outages = pd.DataFrame()
latest_metrics = pd.DataFrame()
history_all = pd.DataFrame()

if using_supabase():
    if page == "Vue d’ensemble":
        daily = load_supabase_daily_summary()
    elif page == "Surveillance":
        recent_outages = add_display_columns(load_supabase_recent_outages(25))
    elif page == "Qualité des données":
        quality = prepare_quality_report(load_supabase_quality_report())
        latest_metrics = load_supabase_latest_metrics()
else:
    # Le fallback CSV ne consomme pas de ressources Supabase; il peut rester complet.
    latest = add_display_columns(load_csv(LATEST_FILE))
    daily = load_csv(DAILY_FILE)
    quality = prepare_quality_report(load_csv(QUALITY_FILE))
    raw = add_display_columns(load_csv(RAW_FILE))
    history_all = enrich_raw_history(raw, latest)
    recent_outages = latest.head(25).copy()

filter_source = active.copy()


def reset_filter_state() -> None:
    """Réinitialiser les valeurs persistées des filtres Streamlit."""
    keys = [
        "filter_major_threshold",
        "filter_min_customers",
        "filter_major_only",
        "filter_include_unknown",
        "filter_regions",
        "filter_mrcs",
        "filter_municipalities",
        "filter_causes",
    ]
    for key in keys:
        st.session_state.pop(key, None)


with st.sidebar.expander("Filtres", expanded=True):
    major_threshold = st.number_input(
        "Seuil de panne majeure",
        min_value=1,
        max_value=50000,
        value=1000,
        step=100,
        key="filter_major_threshold",
    )

    max_customer_value = pd.to_numeric(
        active.get("customers_affected", pd.Series([1])),
        errors="coerce",
    ).max()
    max_customers = int(max_customer_value) if pd.notna(max_customer_value) else 1
    max_customers = max(max_customers, 1)

    min_customers = st.slider(
        "Clients affectés minimum",
        min_value=0,
        max_value=max_customers,
        value=0,
        step=1,
        key="filter_min_customers",
    )
    major_only = st.toggle("Pannes majeures seulement", value=False, key="filter_major_only")
    include_unknown = st.toggle(
        "Inclure les causes inconnues",
        value=True,
        key="filter_include_unknown",
    )

    region_options = (
        sorted(filter_source["region_name"].dropna().astype(str).unique())
        if "region_name" in filter_source.columns
        else []
    )
    selected_regions = st.multiselect("Région", region_options, key="filter_regions")

    mrc_options = (
        sorted(filter_source["mrc_name"].dropna().astype(str).unique())
        if "mrc_name" in filter_source.columns
        else []
    )
    selected_mrcs = st.multiselect("MRC", mrc_options, key="filter_mrcs")

    municipality_options = (
        sorted(filter_source["municipality_label"].dropna().astype(str).unique())
        if "municipality_label" in filter_source.columns
        else []
    )
    selected_municipalities = st.multiselect(
        "Municipalité",
        municipality_options,
        key="filter_municipalities",
    )

    cause_values = []
    if "analysis_cause_label_fr" in filter_source.columns:
        cause_values.extend(
            filter_source["analysis_cause_label_fr"].dropna().astype(str).tolist()
        )
    selected_causes = st.multiselect(
        "Cause",
        sorted(set(cause_values)),
        key="filter_causes",
    )

    st.button("Réinitialiser les filtres", on_click=reset_filter_state, width="stretch")


# =============================================================================
# Application des filtres actifs
# =============================================================================

filtered = active.copy()

if "customers_affected" in filtered.columns:
    customer_counts = pd.to_numeric(
        filtered["customers_affected"],
        errors="coerce",
    ).fillna(0)
    filtered = filtered[customer_counts >= min_customers]
if major_only and "customers_affected" in filtered.columns:
    customer_counts = pd.to_numeric(
        filtered["customers_affected"],
        errors="coerce",
    ).fillna(0)
    filtered = filtered[customer_counts >= major_threshold]
if not include_unknown and "analysis_cause_label_fr" in filtered.columns:
    filtered = filtered[filtered["analysis_cause_label_fr"] != "Inconnue"]
if selected_regions and "region_name" in filtered.columns:
    filtered = filtered[filtered["region_name"].isin(selected_regions)]
if selected_mrcs and "mrc_name" in filtered.columns:
    filtered = filtered[filtered["mrc_name"].isin(selected_mrcs)]
if selected_municipalities and "municipality_label" in filtered.columns:
    filtered = filtered[filtered["municipality_label"].isin(selected_municipalities)]
if selected_causes and "analysis_cause_label_fr" in filtered.columns:
    filtered = filtered[filtered["analysis_cause_label_fr"].isin(selected_causes)]

updated_at = latest_timestamp(active)
updated_display = format_quebec_datetime(updated_at)

st.markdown(
    f"""
    <div class="app-header">
        <div>
            <div class="app-title">⚡ Suivi des pannes électriques</div>
            <div class="app-subtitle">Québec · vue opérationnelle et historique</div>
        </div>
        <div class="app-meta">
            <span class="badge badge-accent">{html.escape(DATA_SOURCE)}</span>
            <span class="badge">Mise à jour : {html.escape(updated_display)}</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    f'<div class="filter-summary">Filtres : {html.escape(active_filter_summary())}</div>',
    unsafe_allow_html=True,
)


# =============================================================================
# Vue d’ensemble
# =============================================================================

if page == "Vue d’ensemble":
    render_page_header(
        "Synthèse",
        "Vue d’ensemble",
        "Les indicateurs essentiels pour comprendre rapidement l’ampleur "
        "et la concentration des pannes actives.",
    )

    active_count = unique_outage_count(filtered)
    customers_sum = safe_numeric_sum(filtered, "customers_affected")
    municipality_count = (
        filtered["municipality_label"].nunique()
        if "municipality_label" in filtered.columns
        else 0
    )
    major_count = (
        unique_outage_count(
            filtered[
                pd.to_numeric(
                    filtered["customers_affected"],
                    errors="coerce",
                ).fillna(0)
                >= major_threshold
            ]
        )
        if "customers_affected" in filtered.columns
        else 0
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pannes actives", format_int(active_count))
    k2.metric("Clients affectés", format_int(customers_sum))
    k3.metric("Municipalités touchées", format_int(municipality_count))
    k4.metric("Pannes majeures", format_int(major_count))

    left, right = st.columns([1.35, 1], gap="large")

    with left:
        render_section_header("Clients affectés par région", "Top 10")
        if {"region_name", "customers_affected"}.issubset(filtered.columns) and not filtered.empty:
            region_summary = (
                filtered.groupby("region_name", as_index=False)
                .agg(clients_affectes=("customers_affected", "sum"))
                .sort_values("clients_affectes", ascending=False)
            )
            render_horizontal_ranking(
                region_summary,
                "region_name",
                "clients_affectes",
                height=430,
                max_rows=10,
            )
        else:
            st.info("Aucune donnée régionale selon les filtres actuels.")

    with right:
        render_section_header("Priorités actuelles", "Clients affectés")
        render_priority_list(filtered, rows=7)

    if not daily.empty and {"date", "max_customers_affected"}.issubset(daily.columns):
        render_section_header("Évolution récente", "Maximum quotidien de clients affectés")
        trend = daily.dropna(subset=["date"]).sort_values("date").tail(45).copy()
        fig = px.line(
            trend,
            x="date",
            y="max_customers_affected",
            markers=True,
            color_discrete_sequence=[ACCENT_COLOR],
            labels={"date": "Date", "max_customers_affected": "Clients affectés"},
        )
        fig.update_traces(line=dict(width=2.5), marker=dict(size=5))
        fig = clean_chart_layout(fig, height=350)
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

    render_section_header("Pannes prioritaires", "15 premières")
    priority_cols = [
        "customers_affected",
        "municipality_label",
        "region_name",
        "mrc_name",
        "analysis_cause_label_fr",
        "status_fr",
        "observed_duration_hours",
        "estimated_restore",
    ]
    priority_table = (
        filtered.sort_values("customers_affected", ascending=False)
        if "customers_affected" in filtered.columns
        else filtered
    )
    show_table(priority_table.head(15), priority_cols, height=480)


# =============================================================================
# Explorateur cartographique
# =============================================================================


elif page == "Explorer la carte":
    render_page_header(
        "Exploration",
        "Carte des pannes",
        "Carte légère de la situation actuelle. Les captures historiques complètes "
        "sont disponibles sur demande afin de préserver les performances du service public.",
    )

    map_data = filtered.copy()
    context = f"Situation observée le {updated_display}."
    render_status(context, "good")

    if map_data.empty:
        st.info("Aucune panne à afficher selon les filtres actuels.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Pannes uniques", format_int(unique_outage_count(map_data)))
        k2.metric(
            "Clients représentés",
            format_int(safe_numeric_sum(map_data, "customers_affected")),
        )
        k3.metric(
            "Municipalités",
            format_int(
                map_data["municipality_label"].nunique()
                if "municipality_label" in map_data.columns
                else 0
            ),
        )
        k4.metric("Points géocodés", format_int(len(get_geo(map_data))))

        map_col, side_col = st.columns([1.65, 0.85], gap="large")
        with map_col:
            render_section_header("Carte", "Taille des points : clients affectés")
            render_clean_map(map_data, height=720)

        with side_col:
            render_section_header("Municipalités les plus touchées", "Top 8")
            if {"municipality_label", "customers_affected"}.issubset(map_data.columns):
                top_mun = (
                    map_data.groupby("municipality_label", as_index=False)
                    .agg(clients_affectes=("customers_affected", "sum"))
                    .sort_values("clients_affectes", ascending=False)
                )
                render_horizontal_ranking(
                    top_mun,
                    "municipality_label",
                    "clients_affectes",
                    height=420,
                    max_rows=8,
                )

            render_section_header("Causes", "Répartition")
            cause_col = get_cause_column(map_data)
            if cause_col:
                cause_summary = (
                    map_data[cause_col]
                    .fillna("Inconnue")
                    .value_counts()
                    .rename_axis("cause")
                    .reset_index(name="pannes")
                )
                render_horizontal_ranking(
                    cause_summary,
                    "cause",
                    "pannes",
                    height=300,
                    max_rows=7,
                    axis_title="Pannes",
                )

        with st.expander("Voir les données de la carte", expanded=False):
            table_cols = [
                "customers_affected",
                "municipality_label",
                "mrc_name",
                "region_name",
                "status_fr",
                get_cause_column(map_data),
                "active_capture_at",
                "start_time",
                "estimated_restore",
            ]
            table_cols = [col for col in table_cols if col]
            table_data = (
                map_data.sort_values("customers_affected", ascending=False)
                if "customers_affected" in map_data.columns
                else map_data
            )
            show_table(table_data, table_cols, height=520)
            make_download(
                map_data,
                "Télécharger les pannes actives de la carte",
                "pannes_actives_carte.csv",
            )

    render_full_data_access()


# =============================================================================
# Analyse territoriale
# =============================================================================

elif page == "Analyse territoriale":
    render_page_header(
        "Territoires",
        "Analyse territoriale",
        "Des classements simples pour comparer les régions, les MRC et les "
        "municipalités sans surcharge visuelle.",
    )

    analysis_level = st.radio(
        "Niveau d’analyse",
        ["Régions", "MRC", "Municipalités"],
        horizontal=True,
        key="territory_level",
    )

    config = {
        "Régions": ("region_name", "Région"),
        "MRC": ("mrc_name", "MRC"),
        "Municipalités": ("municipality_label", "Municipalité"),
    }
    group_col, group_label = config[analysis_level]

    if filtered.empty or group_col not in filtered.columns:
        st.info("Aucune donnée territoriale selon les filtres actuels.")
    else:
        aggregations = {
            "clients_affectes": ("customers_affected", "sum"),
            "clients_max": ("customers_affected", "max"),
        }
        if "outage_id" in filtered.columns:
            aggregations["pannes"] = ("outage_id", "nunique")

        ranking = (
            filtered.dropna(subset=[group_col])
            .groupby(group_col, as_index=False)
            .agg(**aggregations)
            .sort_values("clients_affectes", ascending=False)
        )
        if "pannes" not in ranking.columns:
            ranking["pannes"] = 0

        top_name = ranking.iloc[0][group_col] if not ranking.empty else "—"
        top_clients = ranking.iloc[0]["clients_affectes"] if not ranking.empty else 0
        total_clients = ranking["clients_affectes"].sum() if not ranking.empty else 0
        top3_share = (
            ranking.head(3)["clients_affectes"].sum() / total_clients * 100
            if total_clients > 0
            else 0
        )

        k1, k2, k3 = st.columns(3)
        k1.metric(f"{group_label} la plus touchée", str(top_name))
        k2.metric("Clients dans ce territoire", format_int(top_clients))
        k3.metric("Concentration des 3 premiers", format_pct(top3_share))

        render_section_header(
            f"Clients affectés par {group_label.lower()}",
            "Classement décroissant",
        )
        render_horizontal_ranking(
            ranking,
            group_col,
            "clients_affectes",
            height=530,
            max_rows=15,
        )

        render_section_header("Classement détaillé", "Pannes et impact maximum")
        detail = ranking.rename(
            columns={
                group_col: group_label,
                "pannes": "Pannes",
                "clients_affectes": "Clients affectés",
                "clients_max": "Clients affectés max",
            }
        )
        st.dataframe(
            detail,
            width="stretch",
            hide_index=True,
            height=min(600, 42 + 35 * min(len(detail), 16)),
        )


# =============================================================================
# Causes
# =============================================================================

elif page == "Causes":
    render_page_header(
        "Origine",
        "Causes des pannes",
        "Lecture des causes disponibles dans la vue filtrée, avec une "
        "distinction claire entre cause connue et information absente."
    )

    cause_col = get_cause_column(filtered)
    if filtered.empty or not cause_col:
        st.info("Aucune information de cause disponible selon les filtres actuels.")
    else:
        cause_data = filtered.copy()
        cause_data[cause_col] = cause_data[cause_col].fillna("Inconnue")
        summary = (
            cause_data.groupby(cause_col, as_index=False)
            .agg(
                pannes=(
                    ("outage_id", "nunique")
                    if "outage_id" in cause_data.columns
                    else (cause_col, "size")
                ),
                clients_affectes=(
                    ("customers_affected", "sum")
                    if "customers_affected" in cause_data.columns
                    else (cause_col, "size")
                ),
            )
            .sort_values("pannes", ascending=False)
        )

        known_df = cause_data[cause_data[cause_col] != "Inconnue"]
        known_rate = unique_outage_count(known_df) / max(unique_outage_count(cause_data), 1) * 100
        unknown_count = unique_outage_count(cause_data[cause_data[cause_col] == "Inconnue"])
        known_summary = summary[summary[cause_col] != "Inconnue"].sort_values(
            "pannes",
            ascending=False,
        )
        top_known = (
            known_summary.iloc[0][cause_col]
            if not known_summary.empty
            else "Non disponible"
        )

        k1, k2, k3 = st.columns(3)
        k1.metric("Causes connues", format_pct(known_rate))
        k2.metric("Pannes sans cause fournie", format_int(unknown_count))
        k3.metric("Cause connue principale", str(top_known))

        left, right = st.columns([1.35, 0.85], gap="large")
        with left:
            render_section_header("Nombre de pannes par cause", "Vue filtrée")
            render_horizontal_ranking(
                summary,
                cause_col,
                "pannes",
                height=500,
                max_rows=10,
                axis_title="Pannes",
            )
        with right:
            render_section_header("Impact en clients", "Somme observée")
            impact = summary.sort_values("clients_affectes", ascending=False)
            render_horizontal_ranking(
                impact,
                cause_col,
                "clients_affectes",
                height=500,
                max_rows=10,
            )

        render_status(
            "Une cause inconnue n’est pas nécessairement une erreur : la "
            "source peut simplement ne pas encore fournir cette information.",
            "warning",
        )


# =============================================================================
# Surveillance
# =============================================================================

elif page == "Surveillance":
    render_page_header(
        "Opérations",
        "Surveillance",
        "Priorisation des pannes majeures, des durées longues et des rétablissements à suivre.",
    )

    priority = filtered.copy()
    if "customers_affected" in priority.columns:
        priority["customers_affected"] = pd.to_numeric(
            priority["customers_affected"],
            errors="coerce",
        ).fillna(0)
        priority = priority.sort_values("customers_affected", ascending=False)

    major = (
        priority[priority["customers_affected"] >= major_threshold]
        if "customers_affected" in priority.columns
        else pd.DataFrame()
    )
    longest_hours = (
        pd.to_numeric(priority["observed_duration_hours"], errors="coerce").max()
        if "observed_duration_hours" in priority.columns and not priority.empty
        else 0
    )

    k1, k2, k3 = st.columns(3)
    k1.metric("Pannes majeures", format_int(unique_outage_count(major)))
    k2.metric(
        "Clients dans les pannes majeures",
        format_int(safe_numeric_sum(major, "customers_affected")),
    )
    k3.metric("Durée observée maximale", f"{float(longest_hours or 0):.1f} h")

    if major.empty:
        render_status("Aucune panne majeure active selon le seuil sélectionné.", "good")
    else:
        render_status(
            f"{unique_outage_count(major)} panne(s) dépassent actuellement "
            f"le seuil de {format_int(major_threshold)} clients.",
            "danger",
        )

    left, right = st.columns([1.15, 1], gap="large")
    with left:
        render_section_header("Pannes majeures", "Priorité par clients affectés")
        major_cols = [
            "customers_affected",
            "municipality_label",
            "region_name",
            "analysis_cause_label_fr",
            "status_fr",
            "estimated_restore",
        ]
        show_table(major.head(15), major_cols, height=460)

    with right:
        render_section_header("Durées les plus longues", "Pannes actives")
        if "observed_duration_hours" in priority.columns and not priority.empty:
            long_df = priority.dropna(subset=["observed_duration_hours"]).head(12).copy()
            long_df = long_df.sort_values("observed_duration_hours", ascending=False).head(10)
            if not long_df.empty and "municipality_label" in long_df.columns:
                render_horizontal_ranking(
                    long_df,
                    "municipality_label",
                    "observed_duration_hours",
                    height=460,
                    max_rows=10,
                    axis_title="Durée observée, h",
                )
            else:
                st.info("Aucune durée disponible.")
        else:
            st.info("La durée observée n’est pas disponible dans cette source.")

    render_section_header("Dernières pannes détectées", "25 premières")
    recent = recent_outages.copy() if using_supabase() else latest.copy()
    if "first_capture_at" in recent.columns:
        recent = recent.sort_values("first_capture_at", ascending=False)
    recent_cols = [
        "customers_affected",
        "municipality_label",
        "region_name",
        "mrc_name",
        "status_fr",
        "analysis_cause_label_fr",
        "first_capture_at",
        "estimated_restore",
    ]
    show_table(recent.head(25), recent_cols, height=480)


# =============================================================================
# Qualité des données
# =============================================================================

elif page == "Qualité des données":
    render_page_header(
        "Fiabilité",
        "Qualité des données",
        "Les contrôles techniques sont séparés des limites normales de la "
        "source afin d’éviter les faux signaux d’alerte.",
    )

    if quality.empty:
        st.warning("Aucun rapport qualité disponible.")
    else:
        quality_checks = (
            quality[~quality["check_name"].isin(SOURCE_LIMIT_CHECKS)].copy()
            if "check_name" in quality.columns
            else quality.copy()
        )
        source_limits = (
            quality[quality["check_name"].isin(SOURCE_LIMIT_CHECKS)].copy()
            if "check_name" in quality.columns
            else pd.DataFrame()
        )

        affected = pd.to_numeric(quality_checks.get("rows_affected", 0), errors="coerce").fillna(0)
        issues = (
            quality_checks[affected > 0].copy()
            if not quality_checks.empty
            else pd.DataFrame()
        )
        critical_failures = (
            quality_checks[
                quality_checks["severity"].astype(str).str.lower().eq("critical")
                & quality_checks["status"].astype(str).str.lower().eq("fail")
            ].shape[0]
            if {"severity", "status"}.issubset(quality_checks.columns)
            else 0
        )
        passed_count = (
            quality_checks["status"].astype(str).str.lower().eq("pass").sum()
            if "status" in quality_checks.columns
            else 0
        )

        if using_supabase() and not latest_metrics.empty:
            geocoded_rate_value = pd.to_numeric(
                latest_metrics.iloc[0].get("geocoded_rate_pct", 0),
                errors="coerce",
            )
            geocoded_rate = float(geocoded_rate_value) if pd.notna(geocoded_rate_value) else 0
        else:
            geocoded_rate = (
                bool_rate(latest["is_geocoded"])
                if "is_geocoded" in latest.columns
                else 0
            )

        k1, k2, k3 = st.columns(3)
        k1.metric("Contrôles réussis", format_int(passed_count))
        k2.metric("Erreurs critiques", format_int(critical_failures))
        k3.metric("Municipalités géocodées", format_pct(geocoded_rate))

        if critical_failures == 0 and issues.empty:
            render_status(
                f"Tous les {len(quality_checks)} contrôles techniques sont "
                "réussis et aucune ligne problématique n’est détectée.",
                "good",
            )
        elif critical_failures > 0:
            render_status(
                f"{critical_failures} contrôle(s) critique(s) sont en échec. "
                "Une intervention est recommandée.",
                "danger",
            )
        else:
            render_status(
                f"{len(issues)} contrôle(s) contiennent des lignes à examiner, "
                "sans échec critique.",
                "warning",
            )

        render_section_header("Contrôles techniques", "Vue compacte")
        compact = quality_checks.copy()
        if "status_quality_fr" in compact.columns:
            compact["statut_affiche"] = compact["status_quality_fr"].map(
                {
                    "Réussi": "✓ Réussi",
                    "Échec": "✕ Échec",
                    "Information": "ℹ Information",
                }
            ).fillna(compact["status_quality_fr"])
        else:
            compact["statut_affiche"] = compact.get("status", "")

        compact_cols = [
            col
            for col in [
                "check_name_fr",
                "severity_fr",
                "statut_affiche",
                "rows_affected",
                "failed_rate_pct",
            ]
            if col in compact.columns
        ]
        compact_display = compact[compact_cols].rename(
            columns={
                "check_name_fr": "Contrôle",
                "severity_fr": "Sévérité",
                "statut_affiche": "Statut",
                "rows_affected": "Lignes affectées",
                "failed_rate_pct": "Taux affecté, %",
            }
        )
        st.dataframe(
            compact_display,
            width="stretch",
            hide_index=True,
            height=min(460, 42 + 36 * max(len(compact_display), 1)),
        )

        if not issues.empty and {"rows_affected", "check_name_fr"}.issubset(issues.columns):
            render_section_header(
                "Contrôles à examiner",
                "Seulement les valeurs supérieures à zéro",
            )
            render_horizontal_ranking(
                issues,
                "check_name_fr",
                "rows_affected",
                height=330,
                max_rows=10,
                axis_title="Lignes affectées",
            )

        with st.expander("Description des contrôles"):
            description_cols = [
                col for col in ["check_name_fr", "description_fr"] if col in quality_checks.columns
            ]
            descriptions = quality_checks[description_cols].rename(
                columns={"check_name_fr": "Contrôle", "description_fr": "Description"}
            )
            st.dataframe(descriptions, width="stretch", hide_index=True)

        render_section_header(
            "Limites de la source",
            "Non considérées comme des erreurs techniques",
        )
        if using_supabase() and not latest_metrics.empty:
            known_cause_rate_value = pd.to_numeric(
                latest_metrics.iloc[0].get("known_cause_rate_pct", 0),
                errors="coerce",
            )
            known_cause_rate = (
                float(known_cause_rate_value)
                if pd.notna(known_cause_rate_value)
                else 0
            )
        else:
            known_cause_rate = (
                bool_rate(latest["has_known_cause"])
                if "has_known_cause" in latest.columns
                else 0
            )
        raw_unknown_rows = 0
        raw_unknown_rate = 0.0
        if not source_limits.empty:
            if "rows_affected" in source_limits.columns:
                raw_unknown_rows = (
                    pd.to_numeric(
                        source_limits["rows_affected"],
                        errors="coerce",
                    )
                    .fillna(0)
                    .max()
                )
            if "failed_rate_pct" in source_limits.columns:
                raw_unknown_rate = (
                    pd.to_numeric(
                        source_limits["failed_rate_pct"],
                        errors="coerce",
                    )
                    .fillna(0)
                    .max()
                )

        l1, l2, l3 = st.columns(3)
        l1.metric("Pannes avec cause connue", format_pct(known_cause_rate))
        l2.metric("Observations brutes sans cause", format_pct(raw_unknown_rate))
        l3.metric("Observations concernées", format_int(raw_unknown_rows))

        render_status(
            "Les deux pourcentages utilisent des dénominateurs différents : "
            "le premier porte sur la dernière observation par panne, le second "
            "sur toutes les observations brutes.",
            "warning",
        )


# =============================================================================
# Données
# =============================================================================


elif page == "Données":
    render_page_header(
        "Accès contrôlé",
        "Données",
        "Les jeux de données peuvent être consultés dans le tableau de bord, mais leur "
        "téléchargement direct est désactivé. Toute demande d'accès passe par le formulaire.",
    )

    table_name = st.selectbox(
        "Aperçu du jeu de données",
        [
            "Pannes actives filtrées",
            "Toutes les pannes actives",
            "Sommaire quotidien",
            "Rapport qualité",
        ],
    )

    data_table = pd.DataFrame()
    filename = "donnees.csv"

    if table_name == "Pannes actives filtrées":
        data_table = filtered
        filename = "pannes_actives_filtrees.csv"
    elif table_name == "Toutes les pannes actives":
        data_table = active
        filename = "pannes_actives.csv"
    elif table_name == "Sommaire quotidien":
        if using_supabase():
            with st.spinner("Chargement du sommaire quotidien..."):
                data_table = load_supabase_daily_summary()
        else:
            data_table = daily
        filename = "sommaire_quotidien.csv"
    elif table_name == "Rapport qualité":
        if using_supabase():
            with st.spinner("Chargement du rapport qualité..."):
                data_table = prepare_quality_report(load_supabase_quality_report())
        else:
            data_table = quality
        filename = "rapport_qualite.csv"

    if data_table.empty:
        st.info("Ce jeu de données est vide ou indisponible.")
    else:
        k1, k2 = st.columns(2)
        k1.metric("Lignes disponibles", format_int(len(data_table)))
        k2.metric("Colonnes", format_int(len(data_table.columns)))
        show_table(data_table, height=620)
        make_download(data_table, "Télécharger le fichier CSV", filename)

    render_full_data_access()

