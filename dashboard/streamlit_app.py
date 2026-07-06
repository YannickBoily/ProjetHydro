from pathlib import Path
from datetime import datetime
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1560px;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #151923 0%, #0f131c 100%);
        border-right: 1px solid rgba(148, 163, 184, 0.22);
    }

    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #151b26 0%, #101622 100%);
        border: 1px solid rgba(148, 163, 184, 0.22);
        padding: 1rem 1.15rem;
        border-radius: 18px;
        box-shadow: 0 12px 30px rgba(0,0,0,0.18);
    }

    [data-testid="stMetricLabel"] {
        color: #94a3b8;
        font-size: 0.88rem;
    }

    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 850;
    }

    div[data-testid="stTabs"] button {
        padding: 0.75rem 0.95rem;
        font-weight: 650;
    }

    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #ff4b4b;
        border-bottom: 2px solid #ff4b4b;
    }

    .hero {
        background:
            radial-gradient(circle at top left, rgba(255, 75, 75, 0.20), transparent 30%),
            linear-gradient(135deg, #151b26 0%, #0b0f17 68%);
        border: 1px solid rgba(148, 163, 184, 0.24);
        border-radius: 24px;
        padding: 1.6rem 1.65rem;
        margin-bottom: 1.1rem;
        box-shadow: 0 18px 44px rgba(0,0,0,0.26);
    }

    .hero-title {
        font-size: 2.35rem;
        line-height: 1.1;
        font-weight: 900;
        margin-bottom: 0.55rem;
        letter-spacing: -0.04em;
    }

    .hero-subtitle {
        color: #cbd5e1;
        font-size: 1.02rem;
        max-width: 1120px;
        line-height: 1.55;
    }

    .muted {
        color: #94a3b8;
    }

    .insight {
        background: rgba(255,255,255,0.035);
        border-left: 4px solid #ff4b4b;
        padding: 0.9rem 1rem;
        border-radius: 12px;
        color: #cbd5e1;
        margin-top: 0.55rem;
        margin-bottom: 0.9rem;
    }

    .small-note {
        color: #94a3b8;
        font-size: 0.9rem;
        line-height: 1.45;
    }

    .status-good {
        color: #86efac;
        font-weight: 800;
    }

    .status-warning {
        color: #fde68a;
        font-weight: 800;
    }

    .status-bad {
        color: #fca5a5;
        font-weight: 800;
    }

    .section-title {
        margin-top: 0.25rem;
        margin-bottom: 0.25rem;
    }
</style>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# Traductions et libellés
# =============================================================================

CAUSE_TRANSLATIONS = {
    "unknown": "Inconnue",
    "other": "Autre",
    "equipment": "Bris d’équipement",
    "vegetation": "Végétation",
    "accident": "Accident ou incident",
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


# =============================================================================
# Fonctions utilitaires
# =============================================================================

@st.cache_data(show_spinner=False, ttl=300)
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)

    date_cols = [
        "date",
        "start_time",
        "estimated_restore",
        "captured_at",
        "active_capture_at",
        "latest_row_captured_at",
        "first_capture_at",
        "last_capture_at",
        "known_cause_last_seen_at",
        "created_at",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_convert(None)

    numeric_cols = [
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
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =============================================================================
# Lecture Supabase
# =============================================================================

def get_config_value(name: str, default=None):
    """Read configuration from environment variables or Streamlit secrets."""
    value = os.environ.get(name)
    if value:
        return value

    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


def using_supabase() -> bool:
    return bool(get_config_value("SUPABASE_DB_URL"))


@st.cache_data(show_spinner=False, ttl=300)
def load_supabase_query(query: str) -> pd.DataFrame:
    """Run a SQL query against Supabase/PostgreSQL and return a DataFrame."""
    database_url = get_config_value("SUPABASE_DB_URL")
    database_hostaddr = get_config_value("SUPABASE_DB_HOSTADDR")

    if not database_url:
        return pd.DataFrame()

    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        st.error(
            "La dépendance `SQLAlchemy` est manquante. "
            "Ajoute `sqlalchemy` dans `requirements.txt` puis redéploie l'application."
        )
        raise exc

    connect_args = {}
    if database_hostaddr:
        connect_args["hostaddr"] = database_hostaddr

    engine = create_engine(
        database_url,
        connect_args=connect_args,
        pool_pre_ping=True,
    )

    try:
        df = pd.read_sql_query(query, engine)
    finally:
        engine.dispose()

    date_cols = [
        "date",
        "start_time",
        "estimated_restore",
        "captured_at",
        "active_capture_at",
        "latest_row_captured_at",
        "first_capture_at",
        "last_capture_at",
        "known_cause_last_seen_at",
        "created_at",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_convert(None)

    numeric_cols = [
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
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_supabase_history_days() -> int:
    raw_value = get_config_value("SUPABASE_HISTORY_DAYS", "90")
    try:
        days = int(raw_value)
    except (TypeError, ValueError):
        days = 90

    return max(days, 1)


def get_supabase_history_rows_limit() -> int:
    raw_value = get_config_value("SUPABASE_HISTORY_ROWS_LIMIT", "250000")
    try:
        rows_limit = int(raw_value)
    except (TypeError, ValueError):
        rows_limit = 250000

    return max(rows_limit, 1000)


@st.cache_data(show_spinner=False, ttl=300)
def load_supabase_active() -> pd.DataFrame:
    query = """
        SELECT *
        FROM app_active_outages
        ORDER BY customers_affected DESC NULLS LAST;
    """
    df = load_supabase_query(query)

    if not df.empty:
        # Compatibility with the CSV version of the dashboard.
        if "active_capture_at" not in df.columns and "latest_row_captured_at" in df.columns:
            df["active_capture_at"] = df["latest_row_captured_at"]

        if "outage_age_hours_at_capture" not in df.columns and "outage_age_hours_at_latest_capture" in df.columns:
            df["outage_age_hours_at_capture"] = df["outage_age_hours_at_latest_capture"]

        if "restore_eta_hours_at_capture" not in df.columns and "restore_eta_hours_at_latest_capture" in df.columns:
            df["restore_eta_hours_at_capture"] = df["restore_eta_hours_at_latest_capture"]

    return df


@st.cache_data(show_spinner=False, ttl=300)
def load_supabase_latest() -> pd.DataFrame:
    query = """
        SELECT *
        FROM app_latest_outages
        ORDER BY last_capture_at DESC NULLS LAST, customers_affected DESC NULLS LAST;
    """
    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=300)
def load_supabase_history() -> pd.DataFrame:
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


@st.cache_data(show_spinner=False, ttl=300)
def load_supabase_daily_summary() -> pd.DataFrame:
    query = """
        SELECT *
        FROM app_daily_summary
        ORDER BY date;
    """

    return load_supabase_query(query)


@st.cache_data(show_spinner=False, ttl=300)
def load_supabase_quality_report() -> pd.DataFrame:
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


def translate_text(value, mapping: dict, default: str = "Inconnue") -> str:
    if pd.isna(value):
        return default

    key = str(value).strip().lower()
    if key == "":
        return default

    return mapping.get(key, str(value).strip())


def yes_no(value) -> str:
    return "Oui" if str(value).lower() in {"true", "1", "yes"} else "Non"


def short_id(value, max_len: int = 18) -> str:
    if pd.isna(value):
        return ""
    text = str(value)
    return text if len(text) <= max_len else text[:max_len] + "…"


def add_display_columns(df: pd.DataFrame) -> pd.DataFrame:
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


def enrich_raw_history(raw_df: pd.DataFrame, latest_df: pd.DataFrame) -> pd.DataFrame:
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
        history = history.drop(columns=[col for col in columns_to_add if col in history.columns], errors="ignore")

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

    # Streamlit's dataframe frontend can break when column names are duplicated,
    # missing, non-string, or visually identical after renaming.
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


def show_table(df: pd.DataFrame, columns: list[str] | None = None, height: int | str = "auto") -> None:
    """Display a dataframe while avoiding Streamlit frontend grid crashes."""
    display_df = prepare_display_table(df, columns)

    if display_df.empty:
        st.info("Aucune donnée à afficher selon les filtres actuels.")
        return

    # Very large interactive tables can make Streamlit Cloud slow.
    max_display_rows = 1000

    if len(display_df) > max_display_rows:
        st.caption(
            f"Affichage des {max_display_rows:,} premières lignes sur {len(display_df):,}. "
            "Utilise le bouton de téléchargement pour obtenir le fichier complet."
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


def format_int(value) -> str:
    if pd.isna(value):
        return "0"
    return f"{int(round(float(value))):,}".replace(",", " ")


def format_pct(value) -> str:
    if pd.isna(value):
        return "0 %"
    return f"{float(value):.1f} %"


def bool_rate(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return round(series.astype(str).str.lower().isin(["true", "1", "yes"]).mean() * 100, 2)


def latest_timestamp(*frames: pd.DataFrame):
    values = []

    for df in frames:
        if df.empty:
            continue
        for col in ["active_capture_at", "latest_row_captured_at", "last_capture_at", "captured_at"]:
            if col in df.columns and not df[col].dropna().empty:
                values.append(df[col].max())

    if not values:
        return None

    return max(values)


def get_geo(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "lat" not in df.columns or "lon" not in df.columns:
        return pd.DataFrame()

    return df.dropna(subset=["lat", "lon"]).copy()


def apply_common_layout(fig, height: int | None = None):
    fig.update_layout(
        template=PLOT_TEMPLATE,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, Segoe UI, Arial", size=13),
        legend=dict(
            title=None,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=10, r=10, t=58, b=10),
    )

    if height is not None:
        fig.update_layout(height=height)

    return fig


def make_download(df: pd.DataFrame, label: str, filename: str):
    if df is None or df.empty:
        return

    csv = prepare_display_table(df).to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label=label,
        data=csv,
        file_name=filename,
        mime="text/csv",
        width="stretch",
    )


def build_active_snapshot_at_time(
    history_df: pd.DataFrame,
    selected_capture_at: pd.Timestamp,
    window_minutes: int = 5,
) -> pd.DataFrame:
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


def source_limit_summary(latest_df: pd.DataFrame, quality_df: pd.DataFrame) -> dict:
    known_cause_rate = bool_rate(latest_df["has_known_cause"]) if "has_known_cause" in latest_df.columns else 0
    geocoded_rate = bool_rate(latest_df["is_geocoded"]) if "is_geocoded" in latest_df.columns else 0

    unknown_rows = 0
    if not quality_df.empty and "check_name" in quality_df.columns and "rows_affected" in quality_df.columns:
        match = quality_df[quality_df["check_name"] == "unknown_cause_rows"]
        if not match.empty:
            unknown_rows = int(match["rows_affected"].iloc[0])

    return {
        "known_cause_rate": known_cause_rate,
        "unknown_cause_rate": round(100 - known_cause_rate, 2),
        "geocoded_rate": geocoded_rate,
        "unknown_rows": unknown_rows,
    }


# =============================================================================
# Chargement
# =============================================================================

DATA_SOURCE = "Supabase" if using_supabase() else "CSV"

if using_supabase():
    # Chargement rapide au démarrage : on ne charge pas l'historique brut ici.
    # L'historique est chargé seulement si l'utilisateur active l'option dans la sidebar.
    active = add_display_columns(load_supabase_active())
    latest = add_display_columns(load_supabase_latest())
    daily = load_supabase_daily_summary()
    quality = load_supabase_quality_report()
    history_all = pd.DataFrame()
else:
    active = add_display_columns(load_csv(ACTIVE_FILE))
    latest = add_display_columns(load_csv(LATEST_FILE))
    daily = load_csv(DAILY_FILE)
    quality = load_csv(QUALITY_FILE)
    raw = add_display_columns(load_csv(RAW_FILE))
    history_all = enrich_raw_history(raw, latest)

if not quality.empty:
    if "check_name" in quality.columns:
        quality["check_name_fr"] = quality["check_name"].apply(
            lambda x: translate_text(x, CHECK_TRANSLATIONS, default=str(x))
        )
    if "severity" in quality.columns:
        quality["severity_fr"] = quality["severity"].apply(
            lambda x: translate_text(x, QUALITY_TRANSLATIONS, default=str(x))
        )
    if "status" in quality.columns:
        quality["status_quality_fr"] = quality["status"].apply(
            lambda x: translate_text(x, QUALITY_TRANSLATIONS, default=str(x))
        )

if active.empty or latest.empty or daily.empty:
    st.error(
        "Les fichiers analytiques sont manquants. Exécute d’abord "
        "`python scripts/build_warehouse.py` puis `python scripts/export_tables.py`."
    )
    st.stop()


# =============================================================================
# Sidebar
# =============================================================================

st.sidebar.markdown("## ⚡ Hydro-Québec")
st.sidebar.caption("Dashboard BI des pannes électriques au Québec")
st.sidebar.caption(f"Source de données : {DATA_SOURCE}")

load_history_views = False

if using_supabase():
    load_history_views = st.sidebar.toggle(
        "Charger les vues historiques",
        value=False,
        help=(
            "Désactivé par défaut pour garder le dashboard rapide. "
            "Active cette option seulement pour utiliser Retour dans le temps, Historique "
            "ou télécharger l'historique."
        ),
    )

    if load_history_views:
        st.sidebar.info(
            f"Historique chargé : derniers {get_supabase_history_days()} jours, "
            f"maximum {get_supabase_history_rows_limit():,} lignes."
        )

if st.sidebar.button("Rafraîchir les données", width="stretch"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()

major_threshold = st.sidebar.number_input(
    "Seuil de panne majeure",
    min_value=1,
    max_value=50000,
    value=1000,
    step=100,
)

max_customers = int(active["customers_affected"].max()) if "customers_affected" in active.columns else 1
max_customers = max(max_customers, 1)

min_customers = st.sidebar.slider(
    "Clients affectés minimum",
    min_value=0,
    max_value=max_customers,
    value=0,
    step=1,
)

major_only = st.sidebar.toggle("Seulement les pannes majeures", value=False)
include_unknown = st.sidebar.toggle("Inclure les causes inconnues", value=True)

st.sidebar.divider()
st.sidebar.markdown("### Filtres géographiques")

region_options = sorted(active["region_name"].dropna().astype(str).unique()) if "region_name" in active else []
selected_regions = st.sidebar.multiselect("Région administrative", region_options)

mrc_options = sorted(active["mrc_name"].dropna().astype(str).unique()) if "mrc_name" in active else []
selected_mrcs = st.sidebar.multiselect("MRC", mrc_options)

municipality_options = sorted(active["municipality_label"].dropna().astype(str).unique()) if "municipality_label" in active else []
selected_municipalities = st.sidebar.multiselect("Municipalité", municipality_options)

st.sidebar.divider()
st.sidebar.markdown("### Filtres opérationnels")

cause_options = sorted(active["analysis_cause_label_fr"].dropna().astype(str).unique()) if "analysis_cause_label_fr" in active else []
selected_causes = st.sidebar.multiselect("Cause", cause_options)


# Chargement optionnel de l'historique Supabase.
# Important : Streamlit exécute le code de tous les onglets à chaque rerun.
# On évite donc de charger l'historique au démarrage.
if using_supabase() and load_history_views:
    with st.spinner("Chargement de l'historique Supabase..."):
        history_all = add_display_columns(load_supabase_history())


# =============================================================================
# Filtres actifs
# =============================================================================

filtered = active.copy()

if "customers_affected" in filtered.columns:
    filtered = filtered[filtered["customers_affected"].fillna(0) >= min_customers]

if major_only and "customers_affected" in filtered.columns:
    filtered = filtered[filtered["customers_affected"].fillna(0) >= major_threshold]

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


# =============================================================================
# Header
# =============================================================================

updated_at = latest_timestamp(active, latest)

st.markdown(
    f"""
<div class="hero">
    <div class="hero-title">⚡ Suivi automatisé des pannes électriques au Québec</div>
    <div class="hero-subtitle">
        Tableau de bord interactif pour analyser les pannes observées, les clients affectés,
        les causes, les régions touchées, l’historique et la qualité du pipeline de données.
    </div>
    <div class="muted" style="margin-top:0.8rem;">
        Dernière mise à jour observée : <strong>{updated_at if updated_at is not None else "Non disponible"}</strong>
    </div>
</div>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# KPIs globaux
# =============================================================================

active_count = len(filtered)
customers_sum = filtered["customers_affected"].sum() if "customers_affected" in filtered else 0
municipality_count = filtered["municipality_label"].nunique() if "municipality_label" in filtered else 0
region_count = filtered["region_name"].nunique() if "region_name" in filtered else 0
major_count = filtered[filtered["customers_affected"] >= major_threshold].shape[0] if "customers_affected" in filtered else 0
known_cause = bool_rate(filtered["has_known_cause"]) if "has_known_cause" in filtered else 0

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Pannes actives", format_int(active_count))
m2.metric("Clients affectés", format_int(customers_sum))
m3.metric("Municipalités", format_int(municipality_count))
m4.metric("Régions", format_int(region_count))
m5.metric("Pannes majeures", format_int(major_count))
m6.metric("Causes connues", format_pct(known_cause))


# =============================================================================
# Onglets
# =============================================================================

tab_overview, tab_realtime, tab_time_machine, tab_history, tab_geo, tab_causes, tab_watch, tab_quality, tab_data = st.tabs(
    [
        "Vue d’ensemble",
        "Temps réel",
        "Retour dans le temps",
        "Historique",
        "Analyse géographique",
        "Causes",
        "Surveillance",
        "Qualité & limites",
        "Données",
    ]
)


# =============================================================================
# Vue d'ensemble
# =============================================================================

with tab_overview:
    st.header("Vue d’ensemble")
    st.caption("Synthèse rapide des pannes actives, des régions touchées et des principales zones à surveiller.")

    left, right = st.columns([1.45, 1])

    with left:
        with st.container(border=True):
            st.subheader("Répartition des clients affectés par région")

            if "region_name" in filtered.columns and "customers_affected" in filtered.columns and not filtered.empty:
                region = (
                    filtered.groupby("region_name", as_index=False)
                    .agg(
                        clients_affectes=("customers_affected", "sum"),
                        pannes=("outage_id", "nunique"),
                    )
                    .sort_values("clients_affectes", ascending=True)
                )

                fig = px.bar(
                    region,
                    x="clients_affectes",
                    y="region_name",
                    orientation="h",
                    text="clients_affectes",
                    title="Régions les plus touchées",
                    labels={
                        "clients_affectes": "Clients affectés",
                        "region_name": "Région",
                    },
                )
                fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside", cliponaxis=False)
                fig = apply_common_layout(fig, height=430)
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("Aucune donnée régionale selon les filtres.")

    with right:
        with st.container(border=True):
            st.subheader("Top municipalités")

            if "municipality_label" in filtered.columns and "customers_affected" in filtered.columns and not filtered.empty:
                top_mun = (
                    filtered.groupby(["municipality_label", "region_name"], as_index=False, dropna=False)
                    .agg(
                        clients_affectes=("customers_affected", "sum"),
                        pannes=("outage_id", "nunique"),
                    )
                    .sort_values("clients_affectes", ascending=False)
                    .head(10)
                )

                fig = px.bar(
                    top_mun.sort_values("clients_affectes"),
                    x="clients_affectes",
                    y="municipality_label",
                    orientation="h",
                    text="clients_affectes",
                    hover_data=["region_name", "pannes"],
                    title="Municipalités par clients affectés",
                    labels={
                        "clients_affectes": "Clients affectés",
                        "municipality_label": "Municipalité",
                        "region_name": "Région",
                        "pannes": "Pannes",
                    },
                )
                fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside", cliponaxis=False)
                fig = apply_common_layout(fig, height=430)
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("Aucune municipalité selon les filtres.")

    with st.container(border=True):
        st.subheader("Pannes actives prioritaires")

        priority_cols = [
            "customers_affected",
            "municipality_label",
            "region_name",
            "mrc_name",
            "analysis_cause_label_fr",
            "status_fr",
            "active_capture_at",
            "first_capture_at",
            "observed_duration_hours",
            "estimated_restore",
        ]

        priority = filtered.sort_values("customers_affected", ascending=False) if "customers_affected" in filtered else filtered
        show_table(priority.head(15), priority_cols, height=420)


# =============================================================================
# Temps réel
# =============================================================================

with tab_realtime:
    st.header("Temps réel")
    st.caption("Vue des pannes présentes dans la dernière fenêtre de capture du pipeline.")

    geo = get_geo(filtered)

    if geo.empty:
        st.warning("Aucune coordonnée valide disponible selon les filtres.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Points affichés", format_int(len(geo)))
        c2.metric("Pannes uniques", format_int(geo["outage_id"].nunique() if "outage_id" in geo else len(geo)))
        c3.metric("Municipalités", format_int(geo["municipality_label"].nunique() if "municipality_label" in geo else 0))
        c4.metric("Clients affectés", format_int(geo["customers_affected"].sum() if "customers_affected" in geo else 0))

        geo["taille"] = geo["customers_affected"].fillna(1).clip(lower=1)

        hover_cols = [
            "customers_affected",
            "municipality_label",
            "mrc_name",
            "region_name",
            "status_fr",
            "analysis_cause_label_fr",
            "active_capture_at",
            "estimated_restore",
        ]
        hover_cols = [col for col in hover_cols if col in geo.columns]

        fig = px.scatter_map(
            geo,
            lat="lat",
            lon="lon",
            size="taille",
            color="analysis_cause_label_fr" if "analysis_cause_label_fr" in geo.columns else None,
            hover_data=hover_cols,
            zoom=5,
            height=710,
            title="Pannes actives géolocalisées",
            labels={
                "analysis_cause_label_fr": "Cause",
                "taille": "Clients affectés",
            },
        )
        fig.update_layout(
            template=PLOT_TEMPLATE,
            map_style="open-street-map",
            margin=dict(l=0, r=0, t=50, b=0),
            legend=dict(title=None, orientation="h", y=1.04, x=0),
        )
        st.plotly_chart(fig, width="stretch")

    st.markdown(
        "<div class='insight'>Les coordonnées représentent les points fournis par la source. "
        "Elles servent à l’analyse visuelle, mais ne doivent pas être interprétées comme des limites exactes de panne.</div>",
        unsafe_allow_html=True,
    )


# =============================================================================
# Retour dans le temps
# =============================================================================

with tab_time_machine:
    st.header("Retour dans le temps")
    st.caption(
        "Reconstitue les pannes actives observées autour d’une date et d’une heure passées "
        "à partir de l’historique brut collecté."
    )

    if history_all.empty:
        st.info("L’historique n’est pas chargé. Active `Charger les vues historiques` dans la sidebar pour utiliser cette section.")
    elif "captured_at" not in history_all.columns:
        st.error("La colonne `captured_at` est manquante dans l’historique brut.")
    else:
        history_tm = get_geo(history_all)

        if history_tm.empty:
            st.warning("Aucune observation historique avec coordonnées valides.")
        else:
            history_tm = history_tm.dropna(subset=["captured_at"]).copy()
            history_tm["capture_batch_minute"] = history_tm["captured_at"].dt.floor("min")

            available_batches = (
                history_tm["capture_batch_minute"]
                .dropna()
                .drop_duplicates()
                .sort_values()
                .tolist()
            )

            if not available_batches:
                st.warning("Aucune capture historique disponible.")
            else:
                min_dt = pd.Timestamp(available_batches[0])
                max_dt = pd.Timestamp(available_batches[-1])

                st.markdown(
                    "<div class='insight'>"
                    "Cette vue sélectionne la capture disponible la plus proche de l’heure choisie, "
                    "puis affiche les pannes observées dans une fenêtre autour de cette capture."
                    "</div>",
                    unsafe_allow_html=True,
                )

                c1, c2, c3, c4 = st.columns([1, 1, 1, 1])

                with c1:
                    requested_date = st.date_input(
                        "Date souhaitée",
                        value=max_dt.date(),
                        min_value=min_dt.date(),
                        max_value=max_dt.date(),
                        key="tm_requested_date",
                    )

                with c2:
                    requested_time = st.time_input(
                        "Heure souhaitée",
                        value=max_dt.time().replace(microsecond=0),
                        key="tm_requested_time",
                    )

                with c3:
                    window_minutes = st.slider(
                        "Fenêtre de capture, minutes",
                        min_value=1,
                        max_value=30,
                        value=5,
                        step=1,
                        key="tm_window_minutes",
                    )

                with c4:
                    apply_sidebar_filters = st.toggle(
                        "Appliquer les filtres globaux",
                        value=False,
                        key="tm_apply_sidebar_filters",
                    )

                requested_dt = pd.Timestamp(datetime.combine(requested_date, requested_time))

                nearest_batch = min(
                    available_batches,
                    key=lambda x: abs(pd.Timestamp(x) - requested_dt),
                )

                nearest_batch = pd.Timestamp(nearest_batch)
                gap_minutes = abs((nearest_batch - requested_dt).total_seconds()) / 60

                raw_snapshot = build_active_snapshot_at_time(
                    history_tm,
                    selected_capture_at=nearest_batch,
                    window_minutes=window_minutes,
                )

                snapshot = (
                    apply_global_filters_to_history(raw_snapshot)
                    if apply_sidebar_filters
                    else raw_snapshot
                )

                st.markdown(
                    f"""
                    <div class="small-note">
                        Moment demandé : <strong>{requested_dt}</strong><br>
                        Capture disponible la plus proche : <strong>{nearest_batch}</strong>
                        · écart : <strong>{gap_minutes:.1f} minutes</strong>
                        · fenêtre utilisée : <strong>±{window_minutes} minutes</strong>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                if snapshot.empty:
                    st.warning("Aucune panne observée dans la fenêtre sélectionnée.")
                else:
                    k1, k2, k3, k4, k5, k6 = st.columns(6)

                    k1.metric("Pannes actives estimées", format_int(len(snapshot)))
                    k2.metric(
                        "Clients affectés",
                        format_int(snapshot["customers_affected"].sum() if "customers_affected" in snapshot.columns else 0),
                    )
                    k3.metric(
                        "Municipalités",
                        format_int(snapshot["municipality_label"].nunique() if "municipality_label" in snapshot.columns else 0),
                    )
                    k4.metric(
                        "Régions",
                        format_int(snapshot["region_name"].nunique() if "region_name" in snapshot.columns else 0),
                    )
                    k5.metric(
                        "Pannes majeures",
                        format_int(
                            snapshot[snapshot["customers_affected"] >= major_threshold].shape[0]
                            if "customers_affected" in snapshot.columns
                            else 0
                        ),
                    )
                    k6.metric("Points carte", format_int(len(get_geo(snapshot))))

                    map_col, side_col = st.columns([1.55, 1])

                    with map_col:
                        geo_snapshot = get_geo(snapshot)

                        if geo_snapshot.empty:
                            st.warning("Aucune coordonnée valide pour ce snapshot.")
                        else:
                            geo_snapshot["taille"] = (
                                geo_snapshot["customers_affected"].fillna(1).clip(lower=1)
                                if "customers_affected" in geo_snapshot.columns
                                else 1
                            )

                            hover_cols = [
                                "customers_affected",
                                "municipality_label",
                                "mrc_name",
                                "region_name",
                                "status_fr",
                                "history_cause_label_fr",
                                "captured_at",
                                "start_time",
                                "estimated_restore",
                            ]
                            hover_cols = [col for col in hover_cols if col in geo_snapshot.columns]

                            fig = px.scatter_map(
                                geo_snapshot,
                                lat="lat",
                                lon="lon",
                                size="taille",
                                color="history_cause_label_fr"
                                if "history_cause_label_fr" in geo_snapshot.columns
                                else None,
                                hover_data=hover_cols,
                                zoom=5,
                                height=720,
                                title="Pannes actives estimées au moment sélectionné",
                                labels={
                                    "history_cause_label_fr": "Cause",
                                    "taille": "Clients affectés",
                                },
                            )

                            fig.update_layout(
                                template=PLOT_TEMPLATE,
                                map_style="open-street-map",
                                margin=dict(l=0, r=0, t=50, b=0),
                                legend=dict(title=None, orientation="h", y=1.04, x=0),
                            )

                            st.plotly_chart(fig, width="stretch")

                    with side_col:
                        with st.container(border=True):
                            st.subheader("Top municipalités")

                            if "municipality_label" in snapshot.columns and "customers_affected" in snapshot.columns:
                                top_snapshot_mun = (
                                    snapshot.groupby(["municipality_label", "region_name"], as_index=False, dropna=False)
                                    .agg(
                                        clients_affectes=("customers_affected", "sum"),
                                        pannes=("outage_id", "nunique"),
                                    )
                                    .sort_values("clients_affectes", ascending=False)
                                    .head(10)
                                )

                                fig = px.bar(
                                    top_snapshot_mun.sort_values("clients_affectes"),
                                    x="clients_affectes",
                                    y="municipality_label",
                                    orientation="h",
                                    text="clients_affectes",
                                    hover_data=["region_name", "pannes"],
                                    title="Municipalités touchées",
                                    labels={
                                        "clients_affectes": "Clients affectés",
                                        "municipality_label": "Municipalité",
                                        "region_name": "Région",
                                        "pannes": "Pannes",
                                    },
                                )
                                fig.update_traces(
                                    texttemplate="%{text:,.0f}",
                                    textposition="outside",
                                    cliponaxis=False,
                                )
                                fig.update_layout(yaxis={"categoryorder": "total ascending"})
                                fig = apply_common_layout(fig, height=345)
                                st.plotly_chart(fig, width="stretch")

                        with st.container(border=True):
                            st.subheader("Causes")

                            if "history_cause_label_fr" in snapshot.columns:
                                cause_snapshot = (
                                    snapshot["history_cause_label_fr"]
                                    .fillna("Inconnue")
                                    .value_counts()
                                    .rename_axis("cause")
                                    .reset_index(name="pannes")
                                )

                                fig = px.pie(
                                    cause_snapshot,
                                    names="cause",
                                    values="pannes",
                                    hole=0.55,
                                    title="Répartition des causes",
                                )
                                fig = apply_common_layout(fig, height=345)
                                st.plotly_chart(fig, width="stretch")

                    with st.container(border=True):
                        st.subheader("Pannes observées dans ce snapshot")

                        snapshot_cols = [
                            "customers_affected",
                            "municipality_label",
                            "mrc_name",
                            "region_name",
                            "status_fr",
                            "history_cause_label_fr",
                            "captured_at",
                            "start_time",
                            "estimated_restore",
                        ]

                        snapshot_table = (
                            snapshot.sort_values("customers_affected", ascending=False)
                            if "customers_affected" in snapshot.columns
                            else snapshot
                        )

                        show_table(snapshot_table, snapshot_cols, height=520)

                        with st.expander("Voir les identifiants techniques"):
                            show_table(
                                snapshot_table,
                                ["short_outage_id", "outage_id", "lon", "lat"],
                                height=300,
                            )

                        make_download(
                            snapshot_table,
                            "Télécharger ce snapshot historique",
                            "snapshot_historique_pannes.csv",
                        )


# =============================================================================
# Historique
# =============================================================================

with tab_history:
    st.header("Historique")
    st.caption(
        "Analyse cumulée des pannes observées sur une période. "
        "Cette vue répond à la question : où et quand les pannes ont-elles été observées ?"
    )

    if history_all.empty:
        st.info("L’historique n’est pas chargé. Active `Charger les vues historiques` dans la sidebar pour utiliser cette section.")
    elif "captured_at" not in history_all.columns:
        st.error("La colonne `captured_at` est manquante dans l’historique brut.")
    else:
        history = get_geo(history_all)

        if history.empty:
            st.warning("Aucune observation historique avec coordonnées valides.")
        else:
            history = history.dropna(subset=["captured_at"]).copy()

            min_date = history["captured_at"].min().date()
            max_date = history["captured_at"].max().date()

            h1, h2, h3 = st.columns([1.35, 1, 1])

            with h1:
                selected_dates = st.date_input(
                    "Période historique",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    key="hist_period",
                )

            with h2:
                history_min_customers = st.number_input(
                    "Clients affectés minimum",
                    min_value=0,
                    value=0,
                    step=1,
                    key="hist_min_customers",
                )

            with h3:
                max_history_points = st.slider(
                    "Points maximum sur la carte",
                    min_value=500,
                    max_value=50000,
                    value=12000,
                    step=500,
                    key="hist_max_points",
                )

            if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                start_date, end_date = selected_dates
                history = history[
                    (history["captured_at"].dt.date >= start_date)
                    & (history["captured_at"].dt.date <= end_date)
                ]

            if "customers_affected" in history.columns:
                history = history[history["customers_affected"].fillna(0) >= history_min_customers]

            history = apply_global_filters_to_history(history)

            if history.empty:
                st.warning("Aucune observation historique ne correspond aux filtres.")
            else:
                unique_outages = history["outage_id"].nunique() if "outage_id" in history.columns else len(history)
                observations = len(history)
                hist_clients_max = history["customers_affected"].max() if "customers_affected" in history.columns else 0
                hist_mun = history["municipality_label"].nunique() if "municipality_label" in history.columns else 0
                hist_regions = history["region_name"].nunique() if "region_name" in history.columns else 0

                k1, k2, k3, k4, k5 = st.columns(5)
                k1.metric("Pannes uniques", format_int(unique_outages))
                k2.metric("Observations", format_int(observations))
                k3.metric("Clients max", format_int(hist_clients_max))
                k4.metric("Municipalités", format_int(hist_mun))
                k5.metric("Régions", format_int(hist_regions))

                col_map, col_side = st.columns([1.35, 1])

                with col_map:
                    with st.container(border=True):
                        st.subheader("Carte cumulée des observations")

                        history_map = history.copy()
                        if len(history_map) > max_history_points:
                            history_map = (
                                history_map.sort_values("customers_affected", ascending=False)
                                .head(max_history_points)
                                if "customers_affected" in history_map.columns
                                else history_map.head(max_history_points)
                            )

                        history_map["taille"] = (
                            history_map["customers_affected"].fillna(1).clip(lower=1)
                            if "customers_affected" in history_map.columns
                            else 1
                        )

                        hover_cols = [
                            "customers_affected",
                            "municipality_label",
                            "mrc_name",
                            "region_name",
                            "status_fr",
                            "history_cause_label_fr",
                            "captured_at",
                            "start_time",
                            "estimated_restore",
                        ]
                        hover_cols = [col for col in hover_cols if col in history_map.columns]

                        fig = px.scatter_map(
                            history_map,
                            lat="lat",
                            lon="lon",
                            size="taille",
                            color="history_cause_label_fr" if "history_cause_label_fr" in history_map.columns else None,
                            hover_data=hover_cols,
                            zoom=5,
                            height=620,
                            title="Observations historiques de pannes",
                            labels={
                                "history_cause_label_fr": "Cause",
                                "taille": "Clients affectés",
                            },
                        )
                        fig.update_layout(
                            template=PLOT_TEMPLATE,
                            map_style="open-street-map",
                            margin=dict(l=0, r=0, t=50, b=0),
                            legend=dict(title=None, orientation="h", y=1.04, x=0),
                        )
                        st.plotly_chart(fig, width="stretch")

                with col_side:
                    with st.container(border=True):
                        st.subheader("Municipalités les plus observées")

                        if "municipality_label" in history.columns:
                            hist_top = (
                                history.groupby(["municipality_label", "region_name"], as_index=False, dropna=False)
                                .agg(
                                    pannes_uniques=("outage_id", "nunique"),
                                    observations=("outage_id", "count"),
                                    clients_max=("customers_affected", "max"),
                                )
                                .sort_values("pannes_uniques", ascending=False)
                                .head(12)
                            )

                            fig = px.bar(
                                hist_top.sort_values("pannes_uniques"),
                                x="pannes_uniques",
                                y="municipality_label",
                                orientation="h",
                                text="pannes_uniques",
                                hover_data=["region_name", "observations", "clients_max"],
                                title="Pannes uniques par municipalité",
                                labels={
                                    "pannes_uniques": "Pannes uniques",
                                    "municipality_label": "Municipalité",
                                },
                            )
                            fig.update_traces(textposition="outside", cliponaxis=False)
                            fig = apply_common_layout(fig, height=620)
                            st.plotly_chart(fig, width="stretch")

                with st.container(border=True):
                    st.subheader("Évolution historique selon les filtres")

                    history_daily = (
                        history.assign(date=history["captured_at"].dt.date)
                        .groupby("date", as_index=False)
                        .agg(
                            pannes_uniques=("outage_id", "nunique"),
                            observations=("outage_id", "count"),
                            clients_max=("customers_affected", "max"),
                            municipalites=("municipality_label", "nunique"),
                        )
                    )

                    fig = px.line(
                        history_daily,
                        x="date",
                        y=["pannes_uniques", "clients_max", "municipalites"],
                        markers=True,
                        title="Évolution historique",
                        labels={
                            "date": "Date",
                            "value": "Valeur",
                            "variable": "Indicateur",
                        },
                    )
                    fig = apply_common_layout(fig, height=430)
                    st.plotly_chart(fig, width="stretch")

                    show_table(history_daily.sort_values("date", ascending=False), height=320)


# =============================================================================
# Analyse géographique
# =============================================================================

with tab_geo:
    st.header("Analyse géographique")
    st.caption("Lecture territoriale des pannes actives par région, MRC et municipalité.")

    g1, g2 = st.columns(2)

    with g1:
        with st.container(border=True):
            st.subheader("Part des clients affectés par région")

            if "region_name" in filtered.columns and "customers_affected" in filtered.columns and not filtered.empty:
                region = (
                    filtered.groupby("region_name", as_index=False)
                    .agg(clients_affectes=("customers_affected", "sum"))
                    .sort_values("clients_affectes", ascending=False)
                )

                fig = px.treemap(
                    region,
                    path=["region_name"],
                    values="clients_affectes",
                    title="Poids relatif des régions touchées",
                    labels={"clients_affectes": "Clients affectés"},
                )
                fig = apply_common_layout(fig, height=480)
                st.plotly_chart(fig, width="stretch")

    with g2:
        with st.container(border=True):
            st.subheader("Top MRC")

            if "mrc_name" in filtered.columns and "customers_affected" in filtered.columns and not filtered.empty:
                mrc = (
                    filtered.groupby(["mrc_name", "region_name"], as_index=False, dropna=False)
                    .agg(
                        clients_affectes=("customers_affected", "sum"),
                        pannes=("outage_id", "nunique"),
                        municipalites=("municipality_label", "nunique"),
                    )
                    .sort_values("clients_affectes", ascending=False)
                    .head(15)
                )

                fig = px.scatter(
                    mrc,
                    x="pannes",
                    y="clients_affectes",
                    size="municipalites",
                    color="region_name",
                    hover_name="mrc_name",
                    title="MRC : volume de pannes vs clients affectés",
                    labels={
                        "pannes": "Pannes actives",
                        "clients_affectes": "Clients affectés",
                        "municipalites": "Municipalités",
                        "region_name": "Région",
                    },
                )
                fig = apply_common_layout(fig, height=480)
                st.plotly_chart(fig, width="stretch")

    with st.container(border=True):
        st.subheader("Classement territorial")

        if "municipality_label" in filtered.columns and "customers_affected" in filtered.columns:
            geo_summary = (
                filtered.groupby(["municipality_label", "mrc_name", "region_name"], as_index=False, dropna=False)
                .agg(
                    pannes=("outage_id", "nunique"),
                    clients_affectes=("customers_affected", "sum"),
                    clients_max=("customers_affected", "max"),
                )
                .sort_values("clients_affectes", ascending=False)
            )

            st.dataframe(
                geo_summary.rename(
                    columns={
                        "municipality_label": "Municipalité",
                        "mrc_name": "MRC",
                        "region_name": "Région",
                        "pannes": "Pannes",
                        "clients_affectes": "Clients affectés",
                        "clients_max": "Clients affectés max",
                    }
                ),
                width="stretch",
                height=520,
                hide_index=True,
            )


# =============================================================================
# Causes
# =============================================================================

with tab_causes:
    st.header("Causes")
    st.caption("Analyse des causes connues et inconnues, avec distinction entre données disponibles et limites de la source.")

    cause_df = latest.copy()

    c1, c2 = st.columns([1.1, 1])

    with c1:
        with st.container(border=True):
            st.subheader("Distribution des causes")

            if "analysis_cause_label_fr" in cause_df.columns:
                cause_summary = (
                    cause_df["analysis_cause_label_fr"]
                    .fillna("Inconnue")
                    .value_counts()
                    .rename_axis("cause")
                    .reset_index(name="pannes")
                )

                fig = px.bar(
                    cause_summary,
                    x="pannes",
                    y="cause",
                    orientation="h",
                    text="pannes",
                    title="Nombre de pannes par cause",
                    labels={"pannes": "Pannes", "cause": "Cause"},
                )
                fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside", cliponaxis=False)
                fig.update_layout(yaxis={"categoryorder": "total ascending"})
                fig = apply_common_layout(fig, height=500)
                st.plotly_chart(fig, width="stretch")

    with c2:
        with st.container(border=True):
            st.subheader("Causes connues seulement")

            if "analysis_cause_label_fr" in cause_df.columns:
                known = cause_df[cause_df["analysis_cause_label_fr"] != "Inconnue"]
                known_summary = (
                    known["analysis_cause_label_fr"]
                    .value_counts()
                    .rename_axis("cause")
                    .reset_index(name="pannes")
                )

                if not known_summary.empty:
                    fig = px.pie(
                        known_summary,
                        names="cause",
                        values="pannes",
                        hole=0.58,
                        title="Répartition des causes connues",
                    )
                    fig = apply_common_layout(fig, height=500)
                    st.plotly_chart(fig, width="stretch")
                else:
                    st.info("Aucune cause connue disponible.")

    st.markdown(
        "<div class='insight'>Une cause inconnue ne signifie pas une erreur du pipeline. "
        "Elle indique que la source ne fournit pas toujours la cause au moment de la capture.</div>",
        unsafe_allow_html=True,
    )


# =============================================================================
# Surveillance
# =============================================================================

with tab_watch:
    st.header("Surveillance")
    st.caption("Vue opérationnelle des pannes actives qui méritent une attention particulière.")

    priority = filtered.copy()

    if "customers_affected" in priority.columns:
        priority = priority.sort_values("customers_affected", ascending=False)

    alert_cols = [
        "customers_affected",
        "municipality_label",
        "region_name",
        "mrc_name",
        "analysis_cause_label_fr",
        "status_fr",
        "active_capture_at",
        "estimated_restore",
    ]

    w1, w2 = st.columns([1.05, 1])

    with w1:
        with st.container(border=True):
            st.subheader("Alertes prioritaires")

            major = priority.copy()
            if "customers_affected" in major.columns:
                major = major[major["customers_affected"] >= major_threshold]

            if major.empty:
                st.success("Aucune panne majeure active selon le seuil actuel.")
            else:
                show_table(major.head(10), alert_cols, height=420)

    with w2:
        with st.container(border=True):
            st.subheader("Pannes observées le plus longtemps")

            long = filtered.copy()
            if "observed_duration_hours" in long.columns:
                long = long.sort_values("observed_duration_hours", ascending=False).head(12)

                if not long.empty:
                    fig = px.bar(
                        long.sort_values("observed_duration_hours"),
                        x="observed_duration_hours",
                        y="municipality_label",
                        orientation="h",
                        color="analysis_cause_label_fr" if "analysis_cause_label_fr" in long.columns else None,
                        hover_data=[
                            col for col in [
                                "customers_affected",
                                "region_name",
                                "mrc_name",
                                "status_fr",
                                "first_capture_at",
                                "estimated_restore",
                            ]
                            if col in long.columns
                        ],
                        title="Durée observée des pannes actives",
                        labels={
                            "observed_duration_hours": "Durée observée, heures",
                            "municipality_label": "Municipalité",
                            "analysis_cause_label_fr": "Cause",
                        },
                    )
                    fig = apply_common_layout(fig, height=420)
                    st.plotly_chart(fig, width="stretch")
                else:
                    st.info("Aucune panne active selon les filtres.")
            else:
                st.info("La colonne de durée observée n’est pas disponible.")

    with st.container(border=True):
        st.subheader("Dernières pannes détectées")

        recent = latest.copy()
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

        show_table(recent.head(25), recent_cols, height=420)

        with st.expander("Voir les identifiants techniques des dernières pannes"):
            show_table(
                recent.head(25),
                ["short_outage_id", "outage_id", "lon", "lat"],
                height=260,
            )


# =============================================================================
# Qualité & limites
# =============================================================================

with tab_quality:
    st.header("Qualité & limites")
    st.caption(
        "Séparation entre les contrôles qualité du pipeline et les limites normales de la source, "
        "comme les causes inconnues."
    )

    if quality.empty:
        st.warning("Aucun rapport qualité disponible.")
    else:
        quality_checks = quality[
            ~quality["check_name"].isin(SOURCE_LIMIT_CHECKS)
        ].copy() if "check_name" in quality.columns else quality.copy()

        source_limits = quality[
            quality["check_name"].isin(SOURCE_LIMIT_CHECKS)
        ].copy() if "check_name" in quality.columns else pd.DataFrame()

        failed_critical = (
            quality_checks[
                quality_checks["severity"].astype(str).str.lower().eq("critical")
                & quality_checks["status"].astype(str).str.lower().eq("fail")
            ].shape[0]
            if {"severity", "status"}.issubset(quality_checks.columns)
            else 0
        )

        warnings_affected = (
            quality_checks[
                quality_checks["severity"].astype(str).str.lower().eq("warning")
                & (quality_checks["rows_affected"].fillna(0) > 0)
            ].shape[0]
            if {"severity", "rows_affected"}.issubset(quality_checks.columns)
            else 0
        )

        total_rows = quality["total_rows"].max() if "total_rows" in quality.columns else 0
        limits = source_limit_summary(latest, quality)

        q1, q2, q3, q4, q5 = st.columns(5)
        q1.metric("Erreurs critiques", format_int(failed_critical))
        q2.metric("Avertissements actifs", format_int(warnings_affected))
        q3.metric("Lignes brutes", format_int(total_rows))
        q4.metric("Municipalités géocodées", format_pct(limits["geocoded_rate"]))
        q5.metric("Causes connues", format_pct(limits["known_cause_rate"]))

        left, right = st.columns([1.2, 1])

        with left:
            with st.container(border=True):
                st.subheader("Contrôles qualité du pipeline")

                q_cols = [
                    "check_name_fr",
                    "severity_fr",
                    "status_quality_fr",
                    "rows_affected",
                    "failed_rate_pct",
                    "description",
                ]
                show_table(quality_checks, q_cols, height=520)

        with right:
            with st.container(border=True):
                st.subheader("Lignes affectées par contrôle")

                if "rows_affected" in quality_checks.columns and "check_name_fr" in quality_checks.columns:
                    qchart = quality_checks.sort_values("rows_affected", ascending=True)
                    fig = px.bar(
                        qchart,
                        x="rows_affected",
                        y="check_name_fr",
                        orientation="h",
                        title="Contrôles qualité, hors limites de source",
                        labels={
                            "rows_affected": "Lignes affectées",
                            "check_name_fr": "Contrôle",
                        },
                    )
                    fig = apply_common_layout(fig, height=520)
                    st.plotly_chart(fig, width="stretch")

        with st.container(border=True):
            st.subheader("Limites connues de la source")

            l1, l2, l3 = st.columns(3)
            l1.metric("Lignes avec cause inconnue", format_int(limits["unknown_rows"]))
            l2.metric("Taux de causes inconnues", format_pct(limits["unknown_cause_rate"]))
            l3.metric("Taux de causes connues", format_pct(limits["known_cause_rate"]))

            st.markdown(
                "<div class='insight'>Les causes inconnues sont conservées volontairement. "
                "Elles documentent une limite de disponibilité de la source et ne sont pas traitées comme une erreur critique du pipeline.</div>",
                unsafe_allow_html=True,
            )

            if not source_limits.empty:
                show_table(
                    source_limits,
                    [
                        "check_name_fr",
                        "severity_fr",
                        "status_quality_fr",
                        "rows_affected",
                        "failed_rate_pct",
                        "description",
                    ],
                    height=180,
                )


# =============================================================================
# Données
# =============================================================================

with tab_data:
    st.header("Données")
    st.caption(
        "Accès contrôlé aux tables principales du pipeline. "
        "Pour garder l'application rapide, l'affichage est limité à 1 000 lignes, "
        "mais les téléchargements contiennent les données complètes."
    )

    table_name = st.selectbox(
        "Table à afficher",
        [
            "Pannes actives filtrées",
            "Toutes les pannes actives",
            "Dernière observation par panne",
            "Sommaire quotidien",
            "Rapport qualité",
            "Historique brut enrichi, échantillon",
        ],
    )

    if table_name == "Pannes actives filtrées":
        show_table(filtered, height=620)
        make_download(filtered, "Télécharger les pannes filtrées", "pannes_actives_filtrees.csv")

    elif table_name == "Toutes les pannes actives":
        show_table(active, height=620)
        make_download(active, "Télécharger les pannes actives", "pannes_actives.csv")

    elif table_name == "Dernière observation par panne":
        show_table(latest, height=620)
        make_download(latest, "Télécharger les dernières observations", "dernieres_observations.csv")

    elif table_name == "Sommaire quotidien":
        show_table(daily, height=620)
        make_download(daily, "Télécharger le sommaire quotidien", "sommaire_quotidien.csv")

    elif table_name == "Rapport qualité":
        show_table(quality, height=620)
        make_download(quality, "Télécharger le rapport qualité", "rapport_qualite.csv")

    else:
        if history_all.empty:
            st.info(
                "L’historique n’est pas chargé. Active `Charger les vues historiques` "
                "dans la sidebar pour afficher ou télécharger cette table."
            )
        else:
            show_table(history_all.head(5000), height=620)
            make_download(history_all, "Télécharger l’historique brut enrichi", "historique_pannes_enrichi.csv")
