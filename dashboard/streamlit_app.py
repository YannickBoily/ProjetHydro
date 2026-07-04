from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

RAW_FILE = RAW_DIR / "hydroquebec_history.csv"
ACTIVE_FILE = PROCESSED_DIR / "active_outages.csv"
LATEST_FILE = PROCESSED_DIR / "latest_outages.csv"
DAILY_FILE = PROCESSED_DIR / "daily_summary.csv"
QUALITY_FILE = PROCESSED_DIR / "data_quality_report.csv"


# -------------------------------------------------------------------
# Streamlit config
# -------------------------------------------------------------------

st.set_page_config(
    page_title="Suivi des pannes Hydro-Québec",
    page_icon="⚡",
    layout="wide",
)


# -------------------------------------------------------------------
# Friendly column labels
# -------------------------------------------------------------------

DISPLAY_NAMES = {
    "outage_id": "ID de panne",
    "customers_affected": "Clients affectés",
    "start_time": "Début de la panne",
    "estimated_restore": "Rétablissement estimé",
    "status_code": "Code statut",
    "status": "Statut",
    "cause_code": "Code cause",
    "cause_label": "Cause brute",
    "history_cause_label": "Cause historique",
    "latest_raw_cause_code": "Code cause brute",
    "latest_raw_cause_label": "Cause brute dernière capture",
    "analysis_cause_code": "Code cause analytique",
    "analysis_cause_label": "Cause analytique",
    "has_known_cause": "Cause connue",
    "known_cause_last_seen_at": "Dernière observation de la cause connue",
    "municipality_id": "ID municipalité",
    "captured_at": "Moment de capture",
    "active_capture_at": "Capture active",
    "latest_row_captured_at": "Dernière capture",
    "first_capture_at": "Première capture",
    "last_capture_at": "Dernière capture observée",
    "capture_count": "Nombre de captures",
    "observed_duration_hours": "Durée observée, heures",
    "outage_age_hours_at_capture": "Âge de la panne à la capture, heures",
    "outage_age_hours_at_latest_capture": "Âge à la dernière capture, heures",
    "restore_eta_hours_at_capture": "Temps estimé avant rétablissement, heures",
    "restore_eta_hours_at_latest_capture": "Temps estimé avant rétablissement, heures",
    "lon": "Longitude",
    "lat": "Latitude",
    "is_major_outage": "Panne majeure",
    "date": "Date",
    "snapshots_count": "Nombre de captures",
    "max_active_outages_estimate": "Maximum de pannes actives estimées",
    "avg_active_outages_estimate": "Moyenne de pannes actives estimées",
    "max_customers_affected": "Maximum de clients affectés",
    "avg_customers_affected": "Moyenne de clients affectés",
    "max_municipalities_affected": "Maximum de municipalités touchées",
    "max_major_outages": "Maximum de pannes majeures",
    "new_outages_detected": "Nouvelles pannes détectées",
    "raw_rows_count": "Lignes brutes",
    "unique_outages_observed": "Pannes uniques observées",
    "unknown_cause_rows": "Lignes avec cause inconnue",
    "municipalities_observed": "Municipalités observées",
    "check_name": "Contrôle qualité",
    "severity": "Sévérité",
    "rows_affected": "Lignes affectées",
    "total_rows": "Total lignes",
    "failed_rate_pct": "Taux affecté, %",
    "description": "Description",
    "created_at": "Créé le",
    "pannes_uniques": "Pannes uniques",
    "captures": "Captures",
    "clients_affectes_max": "Clients affectés, maximum",
    "clients_affectes_moyenne": "Clients affectés, moyenne",
    "municipalites_touchees": "Municipalités touchées",
}


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    """Load and lightly type a CSV file."""
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)

    date_columns = [
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

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def rename_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of a dataframe with readable French column names."""
    return df.rename(columns=DISPLAY_NAMES)


def show_dataframe(df: pd.DataFrame, height: int | str = "auto") -> None:
    """Display a dataframe with readable French column names."""
    st.dataframe(
        rename_for_display(df),
        width="stretch",
        height=height,
    )


def format_number(value) -> str:
    """Format KPI numbers."""
    if pd.isna(value):
        return "0"
    return f"{int(value):,}".replace(",", " ")


def bool_rate(series: pd.Series) -> float:
    """Return percentage of truthy values."""
    if series.empty:
        return 0.0

    truthy = series.astype(str).str.lower().isin(["true", "1", "yes"])
    return round(truthy.mean() * 100, 2)


def safe_max_int(series: pd.Series, default: int = 1) -> int:
    """Return a safe integer max for sliders."""
    if series.empty:
        return default
    max_value = pd.to_numeric(series, errors="coerce").max()
    if pd.isna(max_value) or max_value < default:
        return default
    return int(max_value)


def get_latest_update(active_df: pd.DataFrame, latest_df: pd.DataFrame):
    """Return the most recent timestamp available."""
    candidates = []

    for df, cols in [
        (active_df, ["active_capture_at", "captured_at"]),
        (latest_df, ["latest_row_captured_at", "last_capture_at", "captured_at"]),
    ]:
        for col in cols:
            if col in df.columns and not df[col].dropna().empty:
                candidates.append(df[col].max())

    if not candidates:
        return None

    return max(candidates)


def normalize_cause_column(df: pd.DataFrame, source_col: str, output_col: str) -> pd.DataFrame:
    """Normalize a cause column and fill missing values with unknown."""
    result = df.copy()
    if source_col in result.columns:
        result[output_col] = (
            result[source_col]
            .fillna("unknown")
            .astype(str)
            .str.strip()
            .replace("", "unknown")
        )
    else:
        result[output_col] = "unknown"
    return result


def filter_active_outages(
    df: pd.DataFrame,
    selected_causes,
    selected_statuses,
    selected_municipalities,
    min_customers: int,
    include_unknown: bool,
    major_only: bool,
    major_threshold: int,
) -> pd.DataFrame:
    """Apply dashboard filters to active outages."""
    filtered = df.copy()

    if "analysis_cause_label" in filtered.columns:
        filtered["analysis_cause_label"] = filtered["analysis_cause_label"].fillna("unknown")

        if not include_unknown:
            filtered = filtered[
                filtered["analysis_cause_label"].str.lower() != "unknown"
            ]

        if selected_causes:
            filtered = filtered[
                filtered["analysis_cause_label"].isin(selected_causes)
            ]

    if selected_statuses and "status" in filtered.columns:
        filtered = filtered[filtered["status"].isin(selected_statuses)]

    if selected_municipalities and "municipality_id" in filtered.columns:
        filtered = filtered[filtered["municipality_id"].isin(selected_municipalities)]

    if "customers_affected" in filtered.columns:
        filtered = filtered[filtered["customers_affected"].fillna(0) >= min_customers]

        if major_only:
            filtered = filtered[filtered["customers_affected"].fillna(0) >= major_threshold]

    return filtered


def make_download_button(df: pd.DataFrame, label: str, file_name: str) -> None:
    """Create a CSV download button."""
    csv = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label=label,
        data=csv,
        file_name=file_name,
        mime="text/csv",
    )


# -------------------------------------------------------------------
# Load data
# -------------------------------------------------------------------

active = load_csv(ACTIVE_FILE)
latest = load_csv(LATEST_FILE)
daily = load_csv(DAILY_FILE)
quality = load_csv(QUALITY_FILE)
raw_history = load_csv(RAW_FILE)

if not raw_history.empty:
    raw_history = normalize_cause_column(raw_history, "cause_label", "history_cause_label")
    if "captured_at" in raw_history.columns:
        raw_history["capture_date"] = raw_history["captured_at"].dt.date


if active.empty or latest.empty or daily.empty:
    st.error(
        "Les fichiers analytiques sont manquants ou vides. "
        "Exécute `python scripts/build_warehouse.py` puis `python scripts/export_tables.py`."
    )
    st.stop()


# -------------------------------------------------------------------
# Sidebar filters
# -------------------------------------------------------------------

st.sidebar.title("⚡ Hydro-Québec")
st.sidebar.markdown("Suivi automatisé des pannes électriques au Québec.")

st.sidebar.divider()

major_threshold = st.sidebar.number_input(
    "Seuil panne majeure",
    min_value=1,
    max_value=50000,
    value=1000,
    step=100,
    help="Nombre minimal de clients affectés pour considérer une panne comme majeure.",
)

max_customers = safe_max_int(active["customers_affected"]) if "customers_affected" in active.columns else 1

min_customers = st.sidebar.slider(
    "Clients affectés minimum",
    min_value=0,
    max_value=max_customers,
    value=0,
    step=1,
)

major_only = st.sidebar.checkbox(
    "Afficher seulement les pannes majeures",
    value=False,
)

include_unknown = st.sidebar.checkbox(
    "Inclure les causes inconnues",
    value=True,
)

cause_options = []
if "analysis_cause_label" in active.columns:
    cause_options = sorted(
        active["analysis_cause_label"].fillna("unknown").astype(str).unique()
    )

selected_causes = st.sidebar.multiselect(
    "Filtrer par cause active",
    options=cause_options,
    default=[],
)

status_options = []
if "status" in active.columns:
    status_options = sorted(active["status"].dropna().astype(str).unique())

selected_statuses = st.sidebar.multiselect(
    "Filtrer par statut actif",
    options=status_options,
    default=[],
)

municipality_options = []
if "municipality_id" in active.columns:
    municipality_options = sorted(
        active["municipality_id"].dropna().astype(int).unique().tolist()
    )

selected_municipalities = st.sidebar.multiselect(
    "Filtrer par municipalité active",
    options=municipality_options,
    default=[],
)

st.sidebar.divider()
st.sidebar.caption("Données utilisées")
st.sidebar.write("`data/raw/hydroquebec_history.csv`")
st.sidebar.write("`data/processed/active_outages.csv`")
st.sidebar.write("`data/processed/latest_outages.csv`")
st.sidebar.write("`data/processed/daily_summary.csv`")
st.sidebar.write("`data/processed/data_quality_report.csv`")


active_filtered = filter_active_outages(
    active,
    selected_causes=selected_causes,
    selected_statuses=selected_statuses,
    selected_municipalities=selected_municipalities,
    min_customers=min_customers,
    include_unknown=include_unknown,
    major_only=major_only,
    major_threshold=major_threshold,
)


# -------------------------------------------------------------------
# Header
# -------------------------------------------------------------------

st.title("⚡ Suivi automatisé des pannes électriques au Québec")

st.markdown(
    """
Dashboard BI construit à partir d’un pipeline automatisé avec **Python**, **GitHub Actions**, **DuckDB**, **SQL** et **Streamlit**.
Il permet de suivre les pannes actives, d’explorer l’historique des captures et d’analyser la qualité des données.
"""
)

latest_update = get_latest_update(active, latest)

if latest_update is not None:
    st.caption(f"Dernière mise à jour observée : **{latest_update}**")


# -------------------------------------------------------------------
# KPI cards
# -------------------------------------------------------------------

active_outages_count = len(active_filtered)

active_customers = (
    active_filtered["customers_affected"].sum()
    if "customers_affected" in active_filtered.columns
    else 0
)

active_municipalities = (
    active_filtered["municipality_id"].nunique()
    if "municipality_id" in active_filtered.columns
    else 0
)

major_active_count = (
    active_filtered[active_filtered["customers_affected"] >= major_threshold].shape[0]
    if "customers_affected" in active_filtered.columns
    else 0
)

known_cause_rate = (
    bool_rate(active_filtered["has_known_cause"])
    if "has_known_cause" in active_filtered.columns
    else 0.0
)

avg_observed_duration = (
    active_filtered["observed_duration_hours"].mean()
    if "observed_duration_hours" in active_filtered.columns
    else 0
)

kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)

kpi1.metric("Pannes actives", format_number(active_outages_count))
kpi2.metric("Clients affectés", format_number(active_customers))
kpi3.metric("Municipalités", format_number(active_municipalities))
kpi4.metric("Pannes majeures", format_number(major_active_count))
kpi5.metric("Causes connues", f"{known_cause_rate} %")
kpi6.metric("Durée observée moy.", f"{avg_observed_duration:.1f} h")


# -------------------------------------------------------------------
# Tabs
# -------------------------------------------------------------------

(
    tab_summary,
    tab_active_map,
    tab_history_map,
    tab_trends,
    tab_causes,
    tab_monitoring,
    tab_quality,
    tab_tables,
) = st.tabs(
    [
        "Résumé exécutif",
        "Carte active",
        "Carte historique",
        "Évolution temporelle",
        "Analyse des causes",
        "Pannes à surveiller",
        "Qualité des données",
        "Tables",
    ]
)


# -------------------------------------------------------------------
# Tab 1 - Executive summary
# -------------------------------------------------------------------

with tab_summary:
    st.header("Résumé exécutif")

    left, right = st.columns([2, 1])

    with left:
        st.subheader("Carte rapide des pannes actives")

        geo = active_filtered.dropna(subset=["lat", "lon"]).copy()

        if geo.empty:
            st.warning("Aucune panne active avec coordonnées valides selon les filtres actuels.")
        else:
            geo["map_size"] = geo["customers_affected"].fillna(1).clip(lower=1)

            fig = px.scatter_map(
                geo,
                lat="lat",
                lon="lon",
                size="map_size",
                color="analysis_cause_label" if "analysis_cause_label" in geo.columns else None,
                hover_data=[
                    col
                    for col in [
                        "outage_id",
                        "customers_affected",
                        "municipality_id",
                        "status",
                        "analysis_cause_label",
                        "active_capture_at",
                        "first_capture_at",
                        "observed_duration_hours",
                    ]
                    if col in geo.columns
                ],
                zoom=5,
                height=450,
                map_style="open-street-map",
                title="Pannes actives géolocalisées",
            )
            fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig, width="stretch")

    with right:
        st.subheader("Top municipalités")

        if "municipality_id" in active_filtered.columns and "customers_affected" in active_filtered.columns:
            top_municipalities = (
                active_filtered.groupby("municipality_id", as_index=False)
                .agg(
                    active_outages=("outage_id", "nunique"),
                    customers_affected=("customers_affected", "sum"),
                )
                .sort_values("customers_affected", ascending=False)
                .head(10)
            )

            fig = px.bar(
                rename_for_display(top_municipalities),
                x="Clients affectés",
                y="ID municipalité",
                orientation="h",
                title="Municipalités par clients affectés",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, width="stretch")

    st.subheader("Pannes actives les plus importantes")

    major_cols = [
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
    ]

    major_cols = [col for col in major_cols if col in active_filtered.columns]
    major_table = active_filtered.sort_values("customers_affected", ascending=False)
    show_dataframe(major_table[major_cols].head(20))


# -------------------------------------------------------------------
# Tab 2 - Active map
# -------------------------------------------------------------------

with tab_active_map:
    st.header("Carte active")

    geo = active_filtered.dropna(subset=["lat", "lon"]).copy()

    if geo.empty:
        st.warning("Aucune coordonnée valide disponible selon les filtres actuels.")
    else:
        geo["map_size"] = geo["customers_affected"].fillna(1).clip(lower=1)

        hover_cols = [
            "outage_id",
            "customers_affected",
            "municipality_id",
            "status",
            "analysis_cause_label",
            "latest_raw_cause_label",
            "active_capture_at",
            "first_capture_at",
            "last_capture_at",
            "capture_count",
            "observed_duration_hours",
            "estimated_restore",
        ]
        hover_cols = [col for col in hover_cols if col in geo.columns]

        fig = px.scatter_map(
            geo,
            lat="lat",
            lon="lon",
            size="map_size",
            color="analysis_cause_label" if "analysis_cause_label" in geo.columns else None,
            hover_data=hover_cols,
            zoom=5,
            height=700,
            map_style="open-street-map",
            title="Pannes actives selon les filtres",
        )
        fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig, width="stretch")

    st.info(
        "Les pannes actives sont reconstruites à partir de la dernière fenêtre de collecte. "
        "Les coordonnées sont approximatives et proviennent de la source Hydro-Québec."
    )


# -------------------------------------------------------------------
# Tab 3 - Historical map
# -------------------------------------------------------------------

with tab_history_map:
    st.header("Carte historique des pannes observées")

    if raw_history.empty:
        st.warning(
            "Le fichier historique brut est introuvable ou vide. "
            "La carte historique utilise `data/raw/hydroquebec_history.csv`."
        )
    else:
        history = raw_history.copy()

        required_cols = ["captured_at", "lat", "lon", "outage_id"]
        missing_required = [col for col in required_cols if col not in history.columns]

        if missing_required:
            st.error(
                "Colonnes manquantes pour la carte historique : "
                + ", ".join(missing_required)
            )
        else:
            history = history.dropna(subset=["captured_at", "lat", "lon"])

            st.markdown(
                """
Cette carte permet d’explorer les pannes observées dans l’historique collecté.
Elle ne montre pas seulement les pannes actives actuellement, mais les pannes capturées dans une période donnée.
"""
            )

            min_date = history["captured_at"].min().date()
            max_date = history["captured_at"].max().date()

            col_filters_1, col_filters_2, col_filters_3 = st.columns(3)

            with col_filters_1:
                selected_dates = st.date_input(
                    "Période de capture",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                )

            with col_filters_2:
                history_mode = st.selectbox(
                    "Mode d’affichage",
                    [
                        "Dernière observation par panne dans la période",
                        "Toutes les observations de la période",
                        "Première observation par panne dans la période",
                    ],
                )

            with col_filters_3:
                max_points = st.slider(
                    "Nombre maximum de points",
                    min_value=100,
                    max_value=10000,
                    value=3000,
                    step=100,
                )

            if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                start_date, end_date = selected_dates
                history = history[
                    (history["captured_at"].dt.date >= start_date)
                    & (history["captured_at"].dt.date <= end_date)
                ]

            col_filters_4, col_filters_5, col_filters_6 = st.columns(3)

            with col_filters_4:
                history_min_customers = st.number_input(
                    "Clients affectés minimum, historique",
                    min_value=0,
                    value=0,
                    step=1,
                )

            with col_filters_5:
                history_major_only = st.checkbox(
                    "Afficher seulement les pannes majeures historiques",
                    value=False,
                )

            with col_filters_6:
                include_unknown_history = st.checkbox(
                    "Inclure les causes inconnues, historique",
                    value=True,
                )

            if "customers_affected" in history.columns:
                history = history[
                    history["customers_affected"].fillna(0) >= history_min_customers
                ]

                if history_major_only:
                    history = history[
                        history["customers_affected"].fillna(0) >= major_threshold
                    ]

            if "history_cause_label" in history.columns:
                if not include_unknown_history:
                    history = history[
                        history["history_cause_label"].str.lower() != "unknown"
                    ]

                history_cause_options = sorted(
                    history["history_cause_label"]
                    .fillna("unknown")
                    .astype(str)
                    .unique()
                )

                selected_history_causes = st.multiselect(
                    "Causes historiques",
                    options=history_cause_options,
                    default=[],
                )

                if selected_history_causes:
                    history = history[
                        history["history_cause_label"].isin(selected_history_causes)
                    ]

            if "status" in history.columns:
                history_status_options = sorted(
                    history["status"].dropna().astype(str).unique()
                )

                selected_history_statuses = st.multiselect(
                    "Statuts historiques",
                    options=history_status_options,
                    default=[],
                )

                if selected_history_statuses:
                    history = history[history["status"].isin(selected_history_statuses)]

            if "municipality_id" in history.columns:
                history_municipality_options = sorted(
                    history["municipality_id"]
                    .dropna()
                    .astype(int)
                    .unique()
                    .tolist()
                )

                selected_history_municipalities = st.multiselect(
                    "Municipalités historiques",
                    options=history_municipality_options,
                    default=[],
                )

                if selected_history_municipalities:
                    history = history[
                        history["municipality_id"].isin(selected_history_municipalities)
                    ]

            if history.empty:
                st.warning("Aucune panne historique ne correspond aux filtres.")
            else:
                if history_mode == "Dernière observation par panne dans la période":
                    history_map = (
                        history.sort_values("captured_at")
                        .groupby("outage_id", as_index=False)
                        .tail(1)
                    )
                elif history_mode == "Première observation par panne dans la période":
                    history_map = (
                        history.sort_values("captured_at")
                        .groupby("outage_id", as_index=False)
                        .head(1)
                    )
                else:
                    history_map = history.copy()

                if len(history_map) > max_points:
                    history_map = history_map.sort_values(
                        "customers_affected",
                        ascending=False,
                    ).head(max_points)

                h1, h2, h3, h4 = st.columns(4)

                h1.metric("Pannes uniques", format_number(history_map["outage_id"].nunique()))
                h2.metric("Points sur la carte", format_number(len(history_map)))

                if "customers_affected" in history_map.columns:
                    h3.metric(
                        "Clients affectés, total points",
                        format_number(history_map["customers_affected"].sum()),
                    )
                    h4.metric(
                        "Clients affectés, maximum",
                        format_number(history_map["customers_affected"].max()),
                    )

                history_map["map_size"] = history_map["customers_affected"].fillna(1).clip(lower=1)

                color_col = "history_cause_label" if "history_cause_label" in history_map.columns else None

                hover_cols = [
                    "outage_id",
                    "customers_affected",
                    "municipality_id",
                    "status",
                    "history_cause_label",
                    "captured_at",
                    "start_time",
                    "estimated_restore",
                ]
                hover_cols = [col for col in hover_cols if col in history_map.columns]

                fig = px.scatter_map(
                    history_map,
                    lat="lat",
                    lon="lon",
                    size="map_size",
                    color=color_col,
                    hover_data=hover_cols,
                    zoom=5,
                    height=700,
                    map_style="open-street-map",
                    title="Carte historique des pannes observées",
                )
                fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
                st.plotly_chart(fig, width="stretch")

                st.subheader("Sommaire historique par jour")

                history_daily = (
                    history.assign(date=history["captured_at"].dt.date)
                    .groupby("date", as_index=False)
                    .agg(
                        pannes_uniques=("outage_id", "nunique"),
                        captures=("outage_id", "count"),
                        clients_affectes_max=("customers_affected", "max"),
                        clients_affectes_moyenne=("customers_affected", "mean"),
                        municipalites_touchees=("municipality_id", "nunique"),
                    )
                )

                history_daily["clients_affectes_moyenne"] = history_daily[
                    "clients_affectes_moyenne"
                ].round(2)

                fig_daily = px.line(
                    history_daily,
                    x="date",
                    y=[
                        "pannes_uniques",
                        "clients_affectes_max",
                        "municipalites_touchees",
                    ],
                    markers=True,
                    title="Historique quotidien selon les filtres",
                )
                st.plotly_chart(fig_daily, width="stretch")

                show_dataframe(history_daily)

                st.subheader("Données historiques filtrées")

                history_display_cols = [
                    "outage_id",
                    "customers_affected",
                    "municipality_id",
                    "status",
                    "history_cause_label",
                    "captured_at",
                    "start_time",
                    "estimated_restore",
                    "lon",
                    "lat",
                ]
                history_display_cols = [
                    col for col in history_display_cols if col in history_map.columns
                ]

                show_dataframe(
                    history_map[history_display_cols].sort_values(
                        "captured_at",
                        ascending=False,
                    ).head(500),
                    height=400,
                )


# -------------------------------------------------------------------
# Tab 4 - Temporal trends
# -------------------------------------------------------------------

with tab_trends:
    st.header("Évolution temporelle")

    daily_sorted = daily.copy()

    if "date" in daily_sorted.columns:
        daily_sorted = daily_sorted.dropna(subset=["date"]).sort_values("date")

    if daily_sorted.empty or "date" not in daily_sorted.columns:
        st.warning("Aucune donnée temporelle disponible.")
    else:
        min_date = daily_sorted["date"].min().date()
        max_date = daily_sorted["date"].max().date()

        selected_range = st.date_input(
            "Période d’analyse quotidienne",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
            daily_filtered = daily_sorted[
                (daily_sorted["date"].dt.date >= start_date)
                & (daily_sorted["date"].dt.date <= end_date)
            ]
        else:
            daily_filtered = daily_sorted

        metric_options = {
            "Pannes actives estimées - maximum": "max_active_outages_estimate",
            "Clients affectés - maximum": "max_customers_affected",
            "Nouvelles pannes détectées": "new_outages_detected",
            "Municipalités touchées - maximum": "max_municipalities_affected",
            "Pannes majeures - maximum": "max_major_outages",
            "Captures quotidiennes": "snapshots_count",
        }

        available_metrics = {
            label: col
            for label, col in metric_options.items()
            if col in daily_filtered.columns
        }

        selected_metrics = st.multiselect(
            "Indicateurs à afficher",
            options=list(available_metrics.keys()),
            default=list(available_metrics.keys())[:3],
        )

        selected_columns = [available_metrics[label] for label in selected_metrics]

        if selected_columns:
            chart_df = daily_filtered[["date"] + selected_columns].rename(columns=DISPLAY_NAMES)
            fig = px.line(
                chart_df,
                x="Date",
                y=[DISPLAY_NAMES.get(col, col) for col in selected_columns],
                markers=True,
                title="Tendances quotidiennes",
            )
            st.plotly_chart(fig, width="stretch")

        st.subheader("Sommaire quotidien")
        show_dataframe(daily_filtered)


# -------------------------------------------------------------------
# Tab 5 - Cause analysis
# -------------------------------------------------------------------

with tab_causes:
    st.header("Analyse des causes")

    cause_col = "analysis_cause_label" if "analysis_cause_label" in latest.columns else "cause_label"

    latest_cause_df = latest.copy()

    if cause_col in latest_cause_df.columns:
        latest_cause_df[cause_col] = latest_cause_df[cause_col].fillna("unknown")

        known_rate_all = (
            bool_rate(latest_cause_df["has_known_cause"])
            if "has_known_cause" in latest_cause_df.columns
            else 0.0
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Pannes uniques", format_number(len(latest_cause_df)))
        c2.metric("Causes connues", f"{known_rate_all} %")
        c3.metric("Causes inconnues", f"{round(100 - known_rate_all, 2)} %")

        left, right = st.columns(2)

        with left:
            st.subheader("Toutes les causes")

            cause_summary = latest_cause_df[cause_col].value_counts().reset_index()
            cause_summary.columns = ["Cause", "Nombre de pannes"]

            fig = px.bar(
                cause_summary,
                x="Cause",
                y="Nombre de pannes",
                title="Distribution des causes, incluant unknown",
            )
            st.plotly_chart(fig, width="stretch")

        with right:
            st.subheader("Causes connues seulement")

            known_causes = latest_cause_df[
                latest_cause_df[cause_col].str.lower() != "unknown"
            ]

            known_summary = known_causes[cause_col].value_counts().reset_index()
            known_summary.columns = ["Cause", "Nombre de pannes"]

            fig = px.bar(
                known_summary,
                x="Cause",
                y="Nombre de pannes",
                title="Distribution des causes connues",
            )
            st.plotly_chart(fig, width="stretch")

        st.markdown(
            """
### Interprétation

La cause d’une panne n’est pas toujours disponible dans l’API Hydro-Québec.
Le champ `analysis_cause_label` utilise la dernière cause connue observée pour une panne lorsqu’elle existe,
tout en conservant la valeur brute de la dernière capture dans `latest_raw_cause_label`.
"""
        )

    else:
        st.warning("Aucune colonne de cause disponible.")


# -------------------------------------------------------------------
# Tab 6 - Outages to monitor
# -------------------------------------------------------------------

with tab_monitoring:
    st.header("Pannes à surveiller")

    st.subheader("Pannes majeures actives")

    major_active = active_filtered.copy()

    if "customers_affected" in major_active.columns:
        major_active = major_active[
            major_active["customers_affected"] >= major_threshold
        ].sort_values("customers_affected", ascending=False)

    cols_major = [
        "outage_id",
        "customers_affected",
        "municipality_id",
        "status",
        "analysis_cause_label",
        "active_capture_at",
        "first_capture_at",
        "observed_duration_hours",
        "estimated_restore",
    ]
    cols_major = [col for col in cols_major if col in major_active.columns]
    show_dataframe(major_active[cols_major])

    st.subheader("Pannes actives observées le plus longtemps")

    long_active = active_filtered.copy()

    if "observed_duration_hours" in long_active.columns:
        long_active = long_active.sort_values("observed_duration_hours", ascending=False)

    cols_long = [
        "outage_id",
        "customers_affected",
        "municipality_id",
        "status",
        "analysis_cause_label",
        "first_capture_at",
        "last_capture_at",
        "capture_count",
        "observed_duration_hours",
        "estimated_restore",
    ]
    cols_long = [col for col in cols_long if col in long_active.columns]
    show_dataframe(long_active[cols_long].head(25))

    st.subheader("Dernières pannes détectées")

    recent = latest.copy()

    if "first_capture_at" in recent.columns:
        recent = recent.sort_values("first_capture_at", ascending=False)

    cols_recent = [
        "outage_id",
        "customers_affected",
        "municipality_id",
        "status",
        "analysis_cause_label",
        "first_capture_at",
        "last_capture_at",
        "capture_count",
        "observed_duration_hours",
        "estimated_restore",
    ]
    cols_recent = [col for col in cols_recent if col in recent.columns]
    show_dataframe(recent[cols_recent].head(25))


# -------------------------------------------------------------------
# Tab 7 - Data quality
# -------------------------------------------------------------------

with tab_quality:
    st.header("Qualité des données")

    if quality.empty:
        st.warning("Aucun rapport qualité disponible.")
    else:
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

        total_rows = (
            quality["total_rows"].max()
            if "total_rows" in quality.columns and not quality["total_rows"].dropna().empty
            else 0
        )

        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Contrôles qualité", format_number(len(quality)))
        q2.metric("Contrôles échoués", format_number(failed_checks))
        q3.metric("Warnings", format_number(warning_checks))
        q4.metric("Lignes brutes", format_number(total_rows))

        st.subheader("Rapport qualité")
        show_dataframe(quality)

        if "rows_affected" in quality.columns and "check_name" in quality.columns:
            quality_chart = quality.sort_values("rows_affected", ascending=False).rename(columns=DISPLAY_NAMES)

            fig = px.bar(
                quality_chart,
                x="Contrôle qualité",
                y="Lignes affectées",
                title="Nombre de lignes affectées par contrôle qualité",
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, width="stretch")

        st.markdown(
            """
### Limites connues

- Les causes ne sont pas toujours disponibles dans la source.
- Une panne est considérée active si elle apparaît dans la dernière fenêtre de collecte.
- La durée observée correspond au temps pendant lequel une panne est visible dans l’historique collecté, pas nécessairement à la durée réelle complète.
- Les coordonnées représentent des points approximatifs.
"""
        )


# -------------------------------------------------------------------
# Tab 8 - Tables
# -------------------------------------------------------------------

with tab_tables:
    st.header("Tables analytiques")

    table_choice = st.selectbox(
        "Choisir une table",
        [
            "Pannes actives filtrées",
            "Dernière observation par panne",
            "Historique brut",
            "Sommaire quotidien",
            "Rapport qualité",
        ],
    )

    if table_choice == "Pannes actives filtrées":
        show_dataframe(active_filtered)
        make_download_button(active_filtered, "Télécharger les pannes actives filtrées", "active_outages_filtered.csv")

    elif table_choice == "Dernière observation par panne":
        show_dataframe(latest)
        make_download_button(latest, "Télécharger latest_outages", "latest_outages.csv")

    elif table_choice == "Historique brut":
        show_dataframe(raw_history.head(5000), height=500)
        make_download_button(raw_history, "Télécharger l’historique brut", "hydroquebec_history.csv")

    elif table_choice == "Sommaire quotidien":
        show_dataframe(daily)
        make_download_button(daily, "Télécharger daily_summary", "daily_summary.csv")

    elif table_choice == "Rapport qualité":
        show_dataframe(quality)
        make_download_button(quality, "Télécharger data_quality_report", "data_quality_report.csv")
