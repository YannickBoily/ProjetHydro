from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

ACTIVE_FILE = PROCESSED_DIR / "active_outages.csv"
LATEST_FILE = PROCESSED_DIR / "latest_outages.csv"
DAILY_FILE = PROCESSED_DIR / "daily_summary.csv"
QUALITY_FILE = PROCESSED_DIR / "data_quality_report.csv"


# -------------------------------------------------------------------
# Streamlit config
# -------------------------------------------------------------------

st.set_page_config(
    page_title="Pannes Hydro-Québec",
    page_icon="⚡",
    layout="wide",
)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    """Load a CSV file safely."""
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)

    date_columns = [
        "start_time",
        "estimated_restore",
        "captured_at",
        "active_capture_at",
        "latest_row_captured_at",
        "first_capture_at",
        "last_capture_at",
        "known_cause_last_seen_at",
        "created_at",
        "date",
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_columns = [
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
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def bool_rate(series: pd.Series) -> float:
    """Calculate the percentage of truthy values in a possibly mixed boolean column."""
    if series.empty:
        return 0.0

    truthy = series.astype(str).str.lower().isin(["true", "1", "yes"])
    return round(truthy.mean() * 100, 2)


def format_number(value) -> str:
    """Format numbers for KPI cards."""
    if pd.isna(value):
        return "0"
    return f"{int(value):,}".replace(",", " ")


def get_latest_update(active_df: pd.DataFrame, latest_df: pd.DataFrame):
    """Get the most recent update timestamp available."""
    candidates = []

    for df, cols in [
        (active_df, ["active_capture_at", "captured_at"]),
        (latest_df, ["latest_row_captured_at", "captured_at", "last_capture_at"]),
    ]:
        for col in cols:
            if col in df.columns and not df[col].dropna().empty:
                candidates.append(df[col].max())

    if not candidates:
        return None

    return max(candidates)


# -------------------------------------------------------------------
# Load data
# -------------------------------------------------------------------

active = load_csv(ACTIVE_FILE)
latest = load_csv(LATEST_FILE)
daily = load_csv(DAILY_FILE)
quality = load_csv(QUALITY_FILE)


# -------------------------------------------------------------------
# Sidebar
# -------------------------------------------------------------------

st.sidebar.title("⚡ Hydro-Québec")
st.sidebar.markdown("Dashboard de suivi automatisé des pannes électriques.")

st.sidebar.divider()

major_threshold = st.sidebar.number_input(
    "Seuil panne majeure - clients affectés",
    min_value=1,
    max_value=10000,
    value=1000,
    step=100,
)

show_unknown_causes = st.sidebar.checkbox(
    "Inclure les causes inconnues dans les graphiques",
    value=True,
)

st.sidebar.divider()

st.sidebar.caption("Sources utilisées")
st.sidebar.write("`active_outages.csv`")
st.sidebar.write("`latest_outages.csv`")
st.sidebar.write("`daily_summary.csv`")
st.sidebar.write("`data_quality_report.csv`")


# -------------------------------------------------------------------
# Header
# -------------------------------------------------------------------

st.title("⚡ Suivi automatisé des pannes électriques au Québec")

st.markdown(
    """
Ce dashboard présente les pannes électriques observées à partir des données Hydro-Québec.
Le pipeline collecte les données, conserve un historique de captures, construit des tables SQL analytiques avec DuckDB,
puis exporte les fichiers utilisés par ce tableau de bord.
"""
)


if active.empty or latest.empty or daily.empty:
    st.error(
        "Certains fichiers de données sont manquants ou vides. "
        "Exécute d'abord `python scripts/build_warehouse.py` puis `python scripts/export_tables.py`."
    )
    st.stop()


# -------------------------------------------------------------------
# KPI section
# -------------------------------------------------------------------

latest_update = get_latest_update(active, latest)

active_outages_count = len(active)
active_customers = active["customers_affected"].sum() if "customers_affected" in active.columns else 0
active_municipalities = active["municipality_id"].nunique() if "municipality_id" in active.columns else 0
major_active_count = (
    active[active["customers_affected"] >= major_threshold].shape[0]
    if "customers_affected" in active.columns
    else 0
)

known_active_cause_rate = (
    bool_rate(active["has_known_cause"])
    if "has_known_cause" in active.columns
    else 0.0
)

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

kpi1.metric("Pannes actives", format_number(active_outages_count))
kpi2.metric("Clients affectés", format_number(active_customers))
kpi3.metric("Municipalités touchées", format_number(active_municipalities))
kpi4.metric("Pannes majeures", format_number(major_active_count))
kpi5.metric("Causes connues", f"{known_active_cause_rate} %")

if latest_update is not None:
    st.caption(f"Dernière mise à jour observée : {latest_update}")


# -------------------------------------------------------------------
# Tabs
# -------------------------------------------------------------------

tab_current, tab_trends, tab_causes, tab_quality, tab_tables = st.tabs(
    [
        "Vue actuelle",
        "Évolution temporelle",
        "Causes",
        "Qualité des données",
        "Tables",
    ]
)


# -------------------------------------------------------------------
# Tab 1: Current view
# -------------------------------------------------------------------

with tab_current:
    st.header("Vue actuelle des pannes")

    left, right = st.columns([2, 1])

    with left:
        st.subheader("Carte des pannes actives")

        active_geo = active.dropna(subset=["lat", "lon"]).copy()

        if active_geo.empty:
            st.warning("Aucune coordonnée valide pour la carte.")
        else:
            st.map(active_geo[["lat", "lon"]])

    with right:
        st.subheader("Top municipalités touchées")

        if "municipality_id" in active.columns and "customers_affected" in active.columns:
            top_municipalities = (
                active.groupby("municipality_id", as_index=False)
                .agg(
                    active_outages=("outage_id", "nunique"),
                    customers_affected=("customers_affected", "sum"),
                )
                .sort_values("customers_affected", ascending=False)
                .head(10)
            )

            fig = px.bar(
                top_municipalities,
                x="customers_affected",
                y="municipality_id",
                orientation="h",
                title="Top municipalités par clients affectés",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Colonnes insuffisantes pour créer le graphique.")

    st.subheader("Pannes majeures actives")

    major_active = active.copy()

    if "customers_affected" in major_active.columns:
        major_active = major_active[major_active["customers_affected"] >= major_threshold]

    display_cols = [
        "outage_id",
        "customers_affected",
        "municipality_id",
        "status",
        "analysis_cause_label",
        "latest_raw_cause_label",
        "active_capture_at",
        "first_capture_at",
        "observed_duration_hours",
        "estimated_restore",
        "lon",
        "lat",
    ]

    display_cols = [col for col in display_cols if col in major_active.columns]

    st.dataframe(
        major_active[display_cols].sort_values("customers_affected", ascending=False),
        use_container_width=True,
    )


# -------------------------------------------------------------------
# Tab 2: Trends
# -------------------------------------------------------------------

with tab_trends:
    st.header("Évolution temporelle")

    if "date" in daily.columns:
        daily = daily.sort_values("date")

    metric_options = {
        "Pannes actives estimées - maximum": "max_active_outages_estimate",
        "Clients affectés - maximum": "max_customers_affected",
        "Nouvelles pannes détectées": "new_outages_detected",
        "Municipalités touchées - maximum": "max_municipalities_affected",
        "Pannes majeures - maximum": "max_major_outages",
    }

    available_metrics = {
        label: col for label, col in metric_options.items() if col in daily.columns
    }

    selected_labels = st.multiselect(
        "Indicateurs à afficher",
        options=list(available_metrics.keys()),
        default=list(available_metrics.keys())[:2],
    )

    selected_columns = [available_metrics[label] for label in selected_labels]

    if selected_columns and "date" in daily.columns:
        trend_df = daily[["date"] + selected_columns].copy()

        fig = px.line(
            trend_df,
            x="date",
            y=selected_columns,
            markers=True,
            title="Évolution quotidienne des indicateurs",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucun indicateur disponible pour le graphique temporel.")

    st.subheader("Sommaire quotidien")

    st.dataframe(daily, use_container_width=True)


# -------------------------------------------------------------------
# Tab 3: Causes
# -------------------------------------------------------------------

with tab_causes:
    st.header("Analyse des causes")

    cause_col = "analysis_cause_label" if "analysis_cause_label" in latest.columns else "cause_label"

    causes_df = latest.copy()

    if not show_unknown_causes and cause_col in causes_df.columns:
        causes_df = causes_df[
            causes_df[cause_col].fillna("unknown").str.lower() != "unknown"
        ]

    if cause_col in causes_df.columns:
        cause_summary = (
            causes_df[cause_col]
            .fillna("unknown")
            .value_counts()
            .reset_index()
        )
        cause_summary.columns = ["cause", "outage_count"]

        fig = px.bar(
            cause_summary,
            x="cause",
            y="outage_count",
            title="Distribution des pannes par cause analytique",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(cause_summary, use_container_width=True)

    else:
        st.info("Aucune colonne de cause disponible.")

    st.subheader("Causes connues vs inconnues")

    if "has_known_cause" in latest.columns:
        known_rate = bool_rate(latest["has_known_cause"])
        unknown_rate = round(100 - known_rate, 2)

        c1, c2 = st.columns(2)
        c1.metric("Pannes avec cause connue", f"{known_rate} %")
        c2.metric("Pannes sans cause connue", f"{unknown_rate} %")

        known_summary = (
            latest["has_known_cause"]
            .astype(str)
            .str.lower()
            .isin(["true", "1", "yes"])
            .value_counts()
            .reset_index()
        )
        known_summary.columns = ["has_known_cause", "outage_count"]

        known_summary["has_known_cause"] = known_summary["has_known_cause"].map(
            {True: "Cause connue", False: "Cause inconnue"}
        )

        fig = px.pie(
            known_summary,
            names="has_known_cause",
            values="outage_count",
            title="Part des pannes avec cause connue",
        )
        st.plotly_chart(fig, use_container_width=True)


# -------------------------------------------------------------------
# Tab 4: Data quality
# -------------------------------------------------------------------

with tab_quality:
    st.header("Qualité des données")

    if quality.empty:
        st.warning("Aucun rapport qualité disponible.")
    else:
        total_checks = len(quality)
        failed_checks = (
            quality["status"].astype(str).str.lower().eq("fail").sum()
            if "status" in quality.columns
            else 0
        )
        warning_checks = (
            quality["severity"].astype(str).str.lower().eq("warning").sum()
            if "severity" in quality.columns
            else 0
        )

        q1, q2, q3 = st.columns(3)
        q1.metric("Contrôles qualité", format_number(total_checks))
        q2.metric("Contrôles échoués", format_number(failed_checks))
        q3.metric("Warnings", format_number(warning_checks))

        st.dataframe(quality, use_container_width=True)

        if "rows_affected" in quality.columns and "check_name" in quality.columns:
            quality_chart = quality.sort_values("rows_affected", ascending=False)

            fig = px.bar(
                quality_chart,
                x="check_name",
                y="rows_affected",
                title="Lignes affectées par contrôle qualité",
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        """
### Note sur les causes inconnues

Hydro-Québec fournit la cause d’une panne seulement lorsqu’elle est connue.
Les valeurs `unknown` sont donc conservées afin de refléter fidèlement l’information disponible au moment de la collecte.
"""
    )


# -------------------------------------------------------------------
# Tab 5: Tables
# -------------------------------------------------------------------

with tab_tables:
    st.header("Tables analytiques")

    table_choice = st.selectbox(
        "Choisir une table",
        [
            "Pannes actives",
            "Dernière observation par panne",
            "Sommaire quotidien",
            "Rapport qualité",
        ],
    )

    if table_choice == "Pannes actives":
        st.dataframe(active, use_container_width=True)

    elif table_choice == "Dernière observation par panne":
        st.dataframe(latest, use_container_width=True)

    elif table_choice == "Sommaire quotidien":
        st.dataframe(daily, use_container_width=True)

    elif table_choice == "Rapport qualité":
        st.dataframe(quality, use_container_width=True)