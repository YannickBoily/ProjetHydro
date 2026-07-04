from pathlib import Path

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
    :root {
        --bg-main: #0b0f17;
        --bg-card: #151b26;
        --bg-card-soft: #1c2433;
        --text-main: #f8fafc;
        --text-muted: #94a3b8;
        --accent: #ff4b4b;
        --accent-soft: rgba(255, 75, 75, 0.16);
        --border: rgba(148, 163, 184, 0.22);
    }

    .block-container {
        padding-top: 1.6rem;
        padding-bottom: 2rem;
        max-width: 1540px;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #151923 0%, #10131b 100%);
        border-right: 1px solid var(--border);
    }

    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #151b26 0%, #101622 100%);
        border: 1px solid var(--border);
        padding: 1rem 1.15rem;
        border-radius: 18px;
        box-shadow: 0 10px 28px rgba(0,0,0,0.22);
    }

    [data-testid="stMetricLabel"] {
        color: var(--text-muted);
        font-size: 0.88rem;
    }

    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 800;
    }

    div[data-testid="stTabs"] button {
        padding: 0.75rem 1rem;
        font-weight: 650;
    }

    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #ff4b4b;
        border-bottom: 2px solid #ff4b4b;
    }

    .hero {
        background:
            radial-gradient(circle at top left, rgba(255, 75, 75, 0.22), transparent 32%),
            linear-gradient(135deg, #151b26 0%, #0b0f17 68%);
        border: 1px solid var(--border);
        border-radius: 24px;
        padding: 1.5rem 1.6rem;
        margin-bottom: 1.15rem;
        box-shadow: 0 16px 42px rgba(0,0,0,0.26);
    }

    .hero-title {
        font-size: 2.35rem;
        line-height: 1.08;
        font-weight: 900;
        margin-bottom: 0.5rem;
        letter-spacing: -0.04em;
    }

    .hero-subtitle {
        color: var(--text-muted);
        font-size: 1.02rem;
        max-width: 1040px;
        line-height: 1.55;
    }

    .section-card {
        background: linear-gradient(180deg, #151b26 0%, #111827 100%);
        border: 1px solid var(--border);
        border-radius: 22px;
        padding: 1.15rem 1.2rem;
        margin-bottom: 1rem;
        box-shadow: 0 12px 32px rgba(0,0,0,0.18);
    }

    .mini-card {
        background: #111827;
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 1rem;
    }

    .muted {
        color: var(--text-muted);
    }

    .pill {
        display: inline-block;
        padding: 0.32rem 0.65rem;
        border-radius: 999px;
        background: var(--accent-soft);
        color: #fecaca;
        border: 1px solid rgba(255,75,75,0.28);
        font-size: 0.82rem;
        font-weight: 700;
        margin-right: 0.4rem;
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

    .stDataFrame {
        border-radius: 16px;
        overflow: hidden;
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
    "customers_affected": "Clients affectés",
    "start_time": "Début",
    "estimated_restore": "Rétablissement estimé",
    "status_fr": "Statut",
    "analysis_cause_label_fr": "Cause",
    "latest_raw_cause_label_fr": "Cause brute",
    "municipality_label": "Municipalité",
    "mrc_name": "MRC",
    "region_name": "Région",
    "active_capture_at": "Capture active",
    "latest_row_captured_at": "Dernière capture",
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


# =============================================================================
# Fonctions utilitaires
# =============================================================================

@st.cache_data(show_spinner=False)
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


def translate_text(value, mapping: dict, default: str = "Inconnue") -> str:
    if pd.isna(value):
        return default

    key = str(value).strip().lower()
    if key == "":
        return default

    return mapping.get(key, str(value).strip())


def yes_no(value) -> str:
    return "Oui" if str(value).lower() in {"true", "1", "yes"} else "Non"


def add_display_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

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


def prepare_display_table(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()

    if columns is not None:
        selected = [col for col in columns if col in out.columns]
        out = out[selected]

    # Supprimer les colonnes brutes lorsqu'une version lisible existe.
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

    for readable, raw in pairs.items():
        if readable in out.columns and raw in out.columns:
            out = out.drop(columns=[raw])

    out = out.rename(columns=COLUMN_LABELS)

    # Sécurité anti-colonnes dupliquées.
    seen = {}
    unique_cols = []
    for col in out.columns:
        if col not in seen:
            seen[col] = 1
            unique_cols.append(col)
        else:
            seen[col] += 1
            unique_cols.append(f"{col} ({seen[col]})")

    out.columns = unique_cols
    return out


def show_table(df: pd.DataFrame, columns: list[str] | None = None, height: int | str = "auto") -> None:
    st.dataframe(
        prepare_display_table(df, columns),
        width="stretch",
        height=height,
        hide_index=True,
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


def metric_delta(current, previous):
    if previous is None or pd.isna(previous) or previous == 0:
        return None
    change = (current - previous) / previous * 100
    return f"{change:+.1f} %"


def section(title: str, caption: str | None = None):
    st.markdown(f"### {title}")
    if caption:
        st.markdown(f"<div class='muted'>{caption}</div>", unsafe_allow_html=True)


def make_download(df: pd.DataFrame, label: str, filename: str):
    csv = prepare_display_table(df).to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label=label,
        data=csv,
        file_name=filename,
        mime="text/csv",
        width="stretch",
    )


# =============================================================================
# Chargement
# =============================================================================

active = add_display_columns(load_csv(ACTIVE_FILE))
latest = add_display_columns(load_csv(LATEST_FILE))
daily = load_csv(DAILY_FILE)
quality = load_csv(QUALITY_FILE)
raw = add_display_columns(load_csv(RAW_FILE))

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

status_options = sorted(active["status_fr"].dropna().astype(str).unique()) if "status_fr" in active else []
selected_statuses = st.sidebar.multiselect("Statut", status_options)


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

if selected_statuses and "status_fr" in filtered.columns:
    filtered = filtered[filtered["status_fr"].isin(selected_statuses)]


# =============================================================================
# Hero
# =============================================================================

updated_at = latest_timestamp(active, latest)

st.markdown(
    f"""
<div class="hero">
    <div>
        <span class="pill">Portfolio Data / BI</span>
        <span class="pill">Python · DuckDB · SQL · Streamlit</span>
        <span class="pill">Géospatial</span>
    </div>
    <div class="hero-title">⚡ Suivi automatisé des pannes électriques au Québec</div>
    <div class="hero-subtitle">
        Tableau de bord interactif pour analyser les pannes observées, les clients affectés,
        les causes, les régions touchées et la qualité du pipeline de données.
    </div>
    <div class="muted" style="margin-top:0.8rem;">
        Dernière mise à jour observée : <strong>{updated_at if updated_at is not None else "Non disponible"}</strong>
    </div>
</div>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# KPIs
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
# Tabs
# =============================================================================

tab_overview, tab_map, tab_trends, tab_geo, tab_causes, tab_watch, tab_quality, tab_data = st.tabs(
    [
        "Vue d’ensemble",
        "Carte interactive",
        "Tendances",
        "Géographie",
        "Causes",
        "Surveillance",
        "Qualité",
        "Données",
    ]
)


# =============================================================================
# Vue d'ensemble
# =============================================================================

with tab_overview:
    section(
        "Vue d’ensemble opérationnelle",
        "Une synthèse rapide des pannes actives, des régions touchées et des principales zones à surveiller.",
    )

    left, right = st.columns([1.45, 1])

    with left:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
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
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
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
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.subheader("Pannes actives prioritaires")

    priority_cols = [
        "outage_id",
        "customers_affected",
        "municipality_label",
        "region_name",
        "mrc_name",
        "status_fr",
        "analysis_cause_label_fr",
        "active_capture_at",
        "first_capture_at",
        "observed_duration_hours",
        "estimated_restore",
    ]

    priority = filtered.sort_values("customers_affected", ascending=False) if "customers_affected" in filtered else filtered
    show_table(priority.head(15), priority_cols, height=420)
    st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# Carte
# =============================================================================

with tab_map:
    section(
        "Carte interactive des pannes",
        "Les points sont dimensionnés selon le nombre de clients affectés. Le survol affiche les informations opérationnelles.",
    )

    geo = get_geo(filtered)

    if geo.empty:
        st.warning("Aucune coordonnée valide disponible selon les filtres.")
    else:
        geo["taille"] = geo["customers_affected"].fillna(1).clip(lower=1)

        hover_cols = [
            "outage_id",
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
# Tendances
# =============================================================================

with tab_trends:
    section(
        "Tendances temporelles",
        "Suivi quotidien de l’intensité des pannes, des clients affectés et des nouvelles pannes détectées.",
    )

    daily_view = daily.copy()
    if "date" in daily_view.columns:
        daily_view = daily_view.dropna(subset=["date"]).sort_values("date")

    if daily_view.empty:
        st.warning("Aucune donnée temporelle disponible.")
    else:
        min_date = daily_view["date"].min().date()
        max_date = daily_view["date"].max().date()

        selected_dates = st.date_input(
            "Période d’analyse",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
            start_date, end_date = selected_dates
            daily_view = daily_view[
                (daily_view["date"].dt.date >= start_date)
                & (daily_view["date"].dt.date <= end_date)
            ]

        c1, c2 = st.columns([1.5, 1])

        with c1:
            st.markdown("<div class='section-card'>", unsafe_allow_html=True)
            st.subheader("Clients affectés et pannes actives")

            fig = go.Figure()

            if "max_customers_affected" in daily_view.columns:
                fig.add_trace(
                    go.Scatter(
                        x=daily_view["date"],
                        y=daily_view["max_customers_affected"],
                        mode="lines+markers",
                        name="Clients affectés max",
                        hovertemplate="%{x|%Y-%m-%d}<br>Clients: %{y:,.0f}<extra></extra>",
                    )
                )

            if "max_active_outages_estimate" in daily_view.columns:
                fig.add_trace(
                    go.Scatter(
                        x=daily_view["date"],
                        y=daily_view["max_active_outages_estimate"],
                        mode="lines+markers",
                        name="Pannes actives max",
                        yaxis="y2",
                        hovertemplate="%{x|%Y-%m-%d}<br>Pannes: %{y:,.0f}<extra></extra>",
                    )
                )

            fig.update_layout(
                title="Évolution quotidienne",
                yaxis=dict(title="Clients affectés"),
                yaxis2=dict(
                    title="Pannes actives",
                    overlaying="y",
                    side="right",
                    showgrid=False,
                ),
            )
            fig = apply_common_layout(fig, height=460)
            st.plotly_chart(fig, width="stretch")
            st.markdown("</div>", unsafe_allow_html=True)

        with c2:
            st.markdown("<div class='section-card'>", unsafe_allow_html=True)
            st.subheader("Nouvelles pannes détectées")

            if "new_outages_detected" in daily_view.columns:
                fig = px.area(
                    daily_view,
                    x="date",
                    y="new_outages_detected",
                    title="Volume quotidien de nouvelles pannes",
                    labels={
                        "date": "Date",
                        "new_outages_detected": "Nouvelles pannes",
                    },
                )
                fig = apply_common_layout(fig, height=460)
                st.plotly_chart(fig, width="stretch")
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.subheader("Sommaire quotidien")

        daily_cols = [
            "date",
            "snapshots_count",
            "max_active_outages_estimate",
            "max_customers_affected",
            "new_outages_detected",
            "max_municipalities_affected",
            "max_major_outages",
        ]
        show_table(daily_view.sort_values("date", ascending=False), daily_cols, height=440)
        st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# Géographie
# =============================================================================

with tab_geo:
    section(
        "Analyse géographique",
        "Lecture territoriale des pannes par région, MRC et municipalité.",
    )

    g1, g2 = st.columns(2)

    with g1:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
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
        st.markdown("</div>", unsafe_allow_html=True)

    with g2:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
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
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.subheader("Classement territorial")

    geo_summary = pd.DataFrame()
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

    if not geo_summary.empty:
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
    st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# Causes
# =============================================================================

with tab_causes:
    section(
        "Analyse des causes",
        "Distinction entre les causes connues et inconnues, utile pour comprendre les limites de la source.",
    )

    cause_df = latest.copy()

    c1, c2 = st.columns([1.1, 1])

    with c1:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
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
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
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
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        "<div class='insight'>Une cause inconnue ne signifie pas une erreur du pipeline. "
        "Elle indique simplement que la source ne fournit pas toujours la cause au moment de la capture.</div>",
        unsafe_allow_html=True,
    )


# =============================================================================
# Surveillance
# =============================================================================

with tab_watch:
    section(
        "Pannes à surveiller",
        "Pannes majeures, longues ou récemment détectées qui méritent une attention particulière.",
    )

    w1, w2 = st.columns(2)

    with w1:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.subheader("Pannes majeures actives")

        major = filtered.copy()
        if "customers_affected" in major:
            major = major[major["customers_affected"] >= major_threshold].sort_values(
                "customers_affected",
                ascending=False,
            )

        watch_cols = [
            "outage_id",
            "customers_affected",
            "municipality_label",
            "region_name",
            "status_fr",
            "analysis_cause_label_fr",
            "active_capture_at",
        ]

        show_table(major.head(15), watch_cols, height=460)
        st.markdown("</div>", unsafe_allow_html=True)

    with w2:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        st.subheader("Pannes observées le plus longtemps")

        long = filtered.copy()
        if "observed_duration_hours" in long:
            long = long.sort_values("observed_duration_hours", ascending=False)

        long_cols = [
            "outage_id",
            "customers_affected",
            "municipality_label",
            "region_name",
            "observed_duration_hours",
            "capture_count",
            "analysis_cause_label_fr",
            "first_capture_at",
        ]

        show_table(long.head(15), long_cols, height=460)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.subheader("Dernières pannes détectées")

    recent = latest.copy()
    if "first_capture_at" in recent:
        recent = recent.sort_values("first_capture_at", ascending=False)

    recent_cols = [
        "outage_id",
        "customers_affected",
        "municipality_label",
        "region_name",
        "mrc_name",
        "status_fr",
        "analysis_cause_label_fr",
        "first_capture_at",
        "estimated_restore",
    ]

    show_table(recent.head(25), recent_cols, height=500)
    st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# Qualité
# =============================================================================

with tab_quality:
    section(
        "Qualité des données",
        "Contrôles automatiques sur les identifiants, coordonnées, doublons, dates et causes inconnues.",
    )

    if quality.empty:
        st.warning("Aucun rapport qualité disponible.")
    else:
        failed = quality["status"].astype(str).str.lower().eq("fail").sum() if "status" in quality else 0
        warnings = quality["severity"].astype(str).str.lower().eq("warning").sum() if "severity" in quality else 0
        total_rows = quality["total_rows"].max() if "total_rows" in quality else 0
        geocoded = bool_rate(latest["is_geocoded"]) if "is_geocoded" in latest else 0

        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Contrôles échoués", format_int(failed))
        q2.metric("Avertissements", format_int(warnings))
        q3.metric("Lignes brutes", format_int(total_rows))
        q4.metric("Municipalités géocodées", format_pct(geocoded))

        left, right = st.columns([1.2, 1])

        with left:
            st.markdown("<div class='section-card'>", unsafe_allow_html=True)
            st.subheader("Rapport qualité")
            q_cols = [
                "check_name_fr",
                "severity_fr",
                "status_quality_fr",
                "rows_affected",
                "failed_rate_pct",
                "description",
            ]
            show_table(quality, q_cols, height=520)
            st.markdown("</div>", unsafe_allow_html=True)

        with right:
            st.markdown("<div class='section-card'>", unsafe_allow_html=True)
            st.subheader("Lignes affectées")

            if "rows_affected" in quality and "check_name_fr" in quality:
                qchart = quality.sort_values("rows_affected", ascending=True)
                fig = px.bar(
                    qchart,
                    x="rows_affected",
                    y="check_name_fr",
                    orientation="h",
                    title="Contrôles qualité",
                    labels={
                        "rows_affected": "Lignes affectées",
                        "check_name_fr": "Contrôle",
                    },
                )
                fig = apply_common_layout(fig, height=520)
                st.plotly_chart(fig, width="stretch")
            st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# Données
# =============================================================================

with tab_data:
    section(
        "Tables analytiques",
        "Accès contrôlé aux tables principales du pipeline.",
    )

    table_name = st.selectbox(
        "Table à afficher",
        [
            "Pannes actives filtrées",
            "Toutes les pannes actives",
            "Dernière observation par panne",
            "Sommaire quotidien",
            "Rapport qualité",
            "Historique brut, échantillon",
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
        show_table(raw.head(5000), height=620)
        make_download(raw, "Télécharger l’historique brut", "historique_pannes.csv")
