from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# -------------------------------------------------------------------
# Chemins
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
# Configuration Streamlit
# -------------------------------------------------------------------

st.set_page_config(
    page_title="Suivi des pannes Hydro-Québec",
    page_icon="⚡",
    layout="wide",
)


# -------------------------------------------------------------------
# Traductions et noms lisibles
# -------------------------------------------------------------------

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

QUALITY_STATUS_TRANSLATIONS = {
    "pass": "Réussi",
    "fail": "Échec",
    "info": "Information",
}

SEVERITY_TRANSLATIONS = {
    "critical": "Critique",
    "warning": "Avertissement",
    "info": "Information",
}

CHECK_NAME_TRANSLATIONS = {
    "missing_outage_id": "ID de panne manquant",
    "missing_captured_at": "Moment de capture manquant",
    "negative_customers_affected": "Clients affectés négatifs",
    "invalid_coordinates": "Coordonnées invalides",
    "estimated_restore_before_start_time": "Rétablissement estimé avant le début",
    "captured_at_before_start_time": "Capture avant le début de la panne",
    "duplicate_outage_id_captured_at": "Doublon panne + capture",
    "unknown_cause_rows": "Cause inconnue",
}

DISPLAY_NAMES = {
    "outage_id": "ID de panne",
    "customers_affected": "Clients affectés",
    "start_time": "Début de la panne",
    "estimated_restore": "Rétablissement estimé",
    "status_code": "Code statut",
    "status": "Statut brut",
    "status_fr": "Statut",
    "cause_code": "Code cause",
    "cause_label": "Cause brute",
    "history_cause_label": "Cause historique brute",
    "history_cause_label_fr": "Cause historique",
    "latest_raw_cause_code": "Code cause brute",
    "latest_raw_cause_label": "Cause brute dernière capture",
    "latest_raw_cause_label_fr": "Cause brute dernière capture",
    "analysis_cause_code": "Code cause analytique",
    "analysis_cause_label": "Cause analytique brute",
    "analysis_cause_label_fr": "Cause analytique",
    "has_known_cause": "Cause connue brute",
    "has_known_cause_fr": "Cause connue",
    "known_cause_last_seen_at": "Dernière observation de la cause connue",
    "municipality_id": "ID municipalité",
    "municipality_label": "Municipalité",
    "municipality_name": "Nom municipalité",
    "municipality_full_name": "Nom complet municipalité",
    "mrc_name": "MRC",
    "region_name": "Région administrative",
    "is_geocoded": "Municipalité géocodée brute",
    "is_geocoded_fr": "Municipalité géocodée",
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
    "is_major_outage": "Panne majeure brute",
    "is_major_outage_fr": "Panne majeure",
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
    "check_name": "Contrôle qualité brut",
    "check_name_fr": "Contrôle qualité",
    "status_quality_fr": "Statut",
    "severity": "Sévérité brute",
    "severity_fr": "Sévérité",
    "rows_affected": "Lignes affectées",
    "total_rows": "Total lignes",
    "failed_rate_pct": "Taux affecté, %",
    "description": "Description",
    "created_at": "Créé le",
}


# -------------------------------------------------------------------
# Fonctions utilitaires
# -------------------------------------------------------------------

@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    """Charge un CSV et convertit les colonnes principales."""
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


def translate_series(series: pd.Series, mapping: dict, default_unknown: str | None = None) -> pd.Series:
    """Traduit une série texte selon un dictionnaire."""
    normalized = series.fillna("unknown").astype(str).str.strip()
    translated = normalized.str.lower().map(mapping)

    if default_unknown is not None:
        return translated.fillna(default_unknown)

    return translated.fillna(normalized)


def yes_no_series(series: pd.Series) -> pd.Series:
    """Transforme une colonne booléenne en Oui / Non."""
    truthy = series.astype(str).str.lower().isin(["true", "1", "yes"])
    return truthy.map({True: "Oui", False: "Non"})


def add_display_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute des colonnes traduites/lisibles pour l’interface."""
    if df.empty:
        return df

    df = df.copy()

    if "analysis_cause_label" in df.columns:
        df["analysis_cause_label_fr"] = translate_series(
            df["analysis_cause_label"],
            CAUSE_TRANSLATIONS,
            default_unknown="Inconnue",
        )

    if "latest_raw_cause_label" in df.columns:
        df["latest_raw_cause_label_fr"] = translate_series(
            df["latest_raw_cause_label"],
            CAUSE_TRANSLATIONS,
            default_unknown="Inconnue",
        )

    if "cause_label" in df.columns:
        df["history_cause_label"] = (
            df["cause_label"]
            .fillna("unknown")
            .astype(str)
            .str.strip()
            .replace("", "unknown")
        )

        df["history_cause_label_fr"] = translate_series(
            df["history_cause_label"],
            CAUSE_TRANSLATIONS,
            default_unknown="Inconnue",
        )

    if "status" in df.columns:
        df["status_fr"] = translate_series(
            df["status"],
            STATUS_TRANSLATIONS,
        )

    if "has_known_cause" in df.columns:
        df["has_known_cause_fr"] = yes_no_series(df["has_known_cause"])

    if "is_geocoded" in df.columns:
        df["is_geocoded_fr"] = yes_no_series(df["is_geocoded"])

    if "is_major_outage" in df.columns:
        df["is_major_outage_fr"] = yes_no_series(df["is_major_outage"])

    if "municipality_label" not in df.columns and "municipality_id" in df.columns:
        df["municipality_label"] = "Municipalité " + df["municipality_id"].fillna("").astype(str)

    return df


def prepare_table_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prépare une table pour l'affichage Streamlit.

    Objectifs :
    - garder les colonnes traduites plutôt que les colonnes brutes quand les deux existent ;
    - éviter les noms de colonnes dupliqués après renommage ;
    - conserver les colonnes techniques utiles seulement lorsqu'il n'y a pas de meilleure version lisible.
    """
    if df.empty:
        return df.copy()

    display_df = df.copy()

    # Quand une version française/lisible existe, on masque la version brute.
    preferred_pairs = {
        "status_fr": "status",
        "analysis_cause_label_fr": "analysis_cause_label",
        "latest_raw_cause_label_fr": "latest_raw_cause_label",
        "history_cause_label_fr": "history_cause_label",
        "has_known_cause_fr": "has_known_cause",
        "is_geocoded_fr": "is_geocoded",
        "is_major_outage_fr": "is_major_outage",
        "check_name_fr": "check_name",
        "status_quality_fr": "status",
        "severity_fr": "severity",
    }

    columns_to_drop = [
        raw_col
        for readable_col, raw_col in preferred_pairs.items()
        if readable_col in display_df.columns and raw_col in display_df.columns
    ]

    if columns_to_drop:
        display_df = display_df.drop(columns=columns_to_drop)

    # Renommage en français.
    display_df = display_df.rename(columns=DISPLAY_NAMES)

    # Sécurité : Streamlit / PyArrow refuse les noms de colonnes dupliqués.
    # On rend donc chaque nom unique si un doublon subsiste.
    seen = {}
    unique_columns = []

    for col in display_df.columns:
        if col not in seen:
            seen[col] = 0
            unique_columns.append(col)
        else:
            seen[col] += 1
            unique_columns.append(f"{col} ({seen[col] + 1})")

    display_df.columns = unique_columns

    return display_df


def rename_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Renomme les colonnes pour affichage, en gardant des noms uniques."""
    return prepare_table_for_display(df)


def show_dataframe(df: pd.DataFrame, height: int | str = "auto") -> None:
    """Affiche un dataframe avec colonnes traduites."""
    st.dataframe(
        prepare_table_for_display(df),
        width="stretch",
        height=height,
    )


def format_number(value) -> str:
    """Formate un nombre pour les KPI."""
    if pd.isna(value):
        return "0"
    return f"{int(value):,}".replace(",", " ")


def bool_rate(series: pd.Series) -> float:
    """Calcule le pourcentage de valeurs vraies."""
    if series.empty:
        return 0.0

    truthy = series.astype(str).str.lower().isin(["true", "1", "yes"])
    return round(truthy.mean() * 100, 2)


def get_latest_update(active_df: pd.DataFrame, latest_df: pd.DataFrame):
    """Retourne le timestamp le plus récent disponible."""
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


def filter_active_outages(
    df: pd.DataFrame,
    selected_causes: list[str],
    selected_statuses: list[str],
    selected_regions: list[str],
    selected_mrcs: list[str],
    selected_municipalities: list[str],
    min_customers: int,
    major_only: bool,
    major_threshold: int,
    include_unknown: bool,
) -> pd.DataFrame:
    """Applique les filtres de la barre latérale aux pannes actives."""
    filtered = df.copy()

    if "analysis_cause_label_fr" in filtered.columns:
        if not include_unknown:
            filtered = filtered[filtered["analysis_cause_label_fr"].str.lower() != "inconnue"]

        if selected_causes:
            filtered = filtered[filtered["analysis_cause_label_fr"].isin(selected_causes)]

    if selected_statuses and "status_fr" in filtered.columns:
        filtered = filtered[filtered["status_fr"].isin(selected_statuses)]

    if selected_regions and "region_name" in filtered.columns:
        filtered = filtered[filtered["region_name"].isin(selected_regions)]

    if selected_mrcs and "mrc_name" in filtered.columns:
        filtered = filtered[filtered["mrc_name"].isin(selected_mrcs)]

    if selected_municipalities and "municipality_label" in filtered.columns:
        filtered = filtered[filtered["municipality_label"].isin(selected_municipalities)]

    if "customers_affected" in filtered.columns:
        filtered = filtered[filtered["customers_affected"].fillna(0) >= min_customers]

        if major_only:
            filtered = filtered[filtered["customers_affected"].fillna(0) >= major_threshold]

    return filtered


def make_download_button(df: pd.DataFrame, label: str, file_name: str) -> None:
    """Crée un bouton de téléchargement CSV."""
    csv = rename_for_display(df).to_csv(index=False).encode("utf-8-sig")

    st.download_button(
        label=label,
        data=csv,
        file_name=file_name,
        mime="text/csv",
    )


# -------------------------------------------------------------------
# Chargement des données
# -------------------------------------------------------------------

active = add_display_columns(load_csv(ACTIVE_FILE))
latest = add_display_columns(load_csv(LATEST_FILE))
daily = load_csv(DAILY_FILE)
quality = load_csv(QUALITY_FILE)
raw_history = add_display_columns(load_csv(RAW_FILE))

if not quality.empty:
    if "check_name" in quality.columns:
        quality["check_name_fr"] = translate_series(quality["check_name"], CHECK_NAME_TRANSLATIONS)
    if "status" in quality.columns:
        quality["status_quality_fr"] = translate_series(quality["status"], QUALITY_STATUS_TRANSLATIONS)
    if "severity" in quality.columns:
        quality["severity_fr"] = translate_series(quality["severity"], SEVERITY_TRANSLATIONS)

if active.empty or latest.empty or daily.empty:
    st.error(
        "Les fichiers analytiques sont manquants ou vides. "
        "Exécute `python scripts/build_warehouse.py` puis `python scripts/export_tables.py`."
    )
    st.stop()


# -------------------------------------------------------------------
# Barre latérale
# -------------------------------------------------------------------

st.sidebar.title("⚡ Hydro-Québec")
st.sidebar.markdown("Suivi automatisé des pannes électriques au Québec.")

st.sidebar.divider()

major_threshold = st.sidebar.number_input(
    "Seuil de panne majeure",
    min_value=1,
    max_value=50000,
    value=1000,
    step=100,
    help="Nombre minimal de clients affectés pour considérer une panne comme majeure.",
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

major_only = st.sidebar.checkbox("Afficher seulement les pannes majeures", value=False)
include_unknown = st.sidebar.checkbox("Inclure les causes inconnues", value=True)

cause_options = sorted(active["analysis_cause_label_fr"].dropna().astype(str).unique()) if "analysis_cause_label_fr" in active.columns else []
selected_causes = st.sidebar.multiselect("Filtrer par cause", options=cause_options, default=[])

status_options = sorted(active["status_fr"].dropna().astype(str).unique()) if "status_fr" in active.columns else []
selected_statuses = st.sidebar.multiselect("Filtrer par statut", options=status_options, default=[])

region_options = sorted(active["region_name"].dropna().astype(str).unique()) if "region_name" in active.columns else []
selected_regions = st.sidebar.multiselect("Filtrer par région administrative", options=region_options, default=[])

mrc_options = sorted(active["mrc_name"].dropna().astype(str).unique()) if "mrc_name" in active.columns else []
selected_mrcs = st.sidebar.multiselect("Filtrer par MRC", options=mrc_options, default=[])

municipality_options = sorted(active["municipality_label"].dropna().astype(str).unique()) if "municipality_label" in active.columns else []
selected_municipalities = st.sidebar.multiselect("Filtrer par municipalité", options=municipality_options, default=[])

st.sidebar.divider()
st.sidebar.caption("Tables utilisées")
st.sidebar.write("`active_outages.csv`")
st.sidebar.write("`latest_outages.csv`")
st.sidebar.write("`daily_summary.csv`")
st.sidebar.write("`data_quality_report.csv`")
st.sidebar.write("`hydroquebec_history.csv`")

active_filtered = filter_active_outages(
    active,
    selected_causes=selected_causes,
    selected_statuses=selected_statuses,
    selected_regions=selected_regions,
    selected_mrcs=selected_mrcs,
    selected_municipalities=selected_municipalities,
    min_customers=min_customers,
    major_only=major_only,
    major_threshold=major_threshold,
    include_unknown=include_unknown,
)


# -------------------------------------------------------------------
# En-tête
# -------------------------------------------------------------------

st.title("⚡ Suivi automatisé des pannes électriques au Québec")

st.markdown(
    """
Ce tableau de bord présente les pannes électriques observées à partir des données Hydro-Québec.
Le pipeline utilise **Python**, **GitHub Actions**, **DuckDB**, **SQL**, un enrichissement géospatial des municipalités
et **Streamlit** pour visualiser les pannes actives et l’historique collecté.
"""
)

latest_update = get_latest_update(active, latest)
if latest_update is not None:
    st.caption(f"Dernière mise à jour observée : **{latest_update}**")


# -------------------------------------------------------------------
# KPI principaux
# -------------------------------------------------------------------

active_outages_count = len(active_filtered)
active_customers = active_filtered["customers_affected"].sum() if "customers_affected" in active_filtered.columns else 0
active_municipalities = active_filtered["municipality_label"].nunique() if "municipality_label" in active_filtered.columns else 0
active_regions = active_filtered["region_name"].nunique() if "region_name" in active_filtered.columns else 0
major_active_count = active_filtered[active_filtered["customers_affected"] >= major_threshold].shape[0] if "customers_affected" in active_filtered.columns else 0
known_cause_rate = bool_rate(active_filtered["has_known_cause"]) if "has_known_cause" in active_filtered.columns else 0.0

kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
kpi1.metric("Pannes actives", format_number(active_outages_count))
kpi2.metric("Clients affectés", format_number(active_customers))
kpi3.metric("Municipalités", format_number(active_municipalities))
kpi4.metric("Régions", format_number(active_regions))
kpi5.metric("Pannes majeures", format_number(major_active_count))
kpi6.metric("Causes connues", f"{known_cause_rate} %")


# -------------------------------------------------------------------
# Onglets
# -------------------------------------------------------------------

(
    tab_summary,
    tab_active_map,
    tab_history_map,
    tab_trends,
    tab_causes,
    tab_monitoring,
    tab_geo,
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
        "Analyse géographique",
        "Qualité des données",
        "Tables",
    ]
)


# -------------------------------------------------------------------
# Résumé exécutif
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
            st.map(geo[["lat", "lon"]])

    with right:
        st.subheader("Top municipalités")
        if "municipality_label" in active_filtered.columns and "customers_affected" in active_filtered.columns:
            top_municipalities = (
                active_filtered.groupby("municipality_label", as_index=False)
                .agg(
                    pannes_actives=("outage_id", "nunique"),
                    clients_affectes=("customers_affected", "sum"),
                )
                .sort_values("clients_affectes", ascending=False)
                .head(10)
            )

            fig = px.bar(
                top_municipalities,
                x="clients_affectes",
                y="municipality_label",
                orientation="h",
                title="Municipalités par clients affectés",
                labels={"clients_affectes": "Clients affectés", "municipality_label": "Municipalité"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, width="stretch")

    st.subheader("Pannes actives les plus importantes")
    major_cols = [
        "outage_id",
        "customers_affected",
        "municipality_label",
        "region_name",
        "mrc_name",
        "status_fr",
        "analysis_cause_label_fr",
        "latest_raw_cause_label_fr",
        "active_capture_at",
        "first_capture_at",
        "observed_duration_hours",
        "estimated_restore",
    ]
    major_cols = [col for col in major_cols if col in active_filtered.columns]
    major_table = active_filtered.sort_values("customers_affected", ascending=False) if "customers_affected" in active_filtered.columns else active_filtered
    show_dataframe(major_table[major_cols].head(20))


# -------------------------------------------------------------------
# Carte active
# -------------------------------------------------------------------

with tab_active_map:
    st.header("Carte interactive des pannes actives")
    geo = active_filtered.dropna(subset=["lat", "lon"]).copy()

    if geo.empty:
        st.warning("Aucune coordonnée valide disponible.")
    else:
        geo["taille_carte"] = geo["customers_affected"].fillna(1).clip(lower=1) if "customers_affected" in geo.columns else 1
        hover_cols = [
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
        hover_cols = [col for col in hover_cols if col in geo.columns]

        fig = px.scatter_map(
            geo,
            lat="lat",
            lon="lon",
            size="taille_carte",
            color="analysis_cause_label_fr" if "analysis_cause_label_fr" in geo.columns else None,
            hover_data=hover_cols,
            zoom=5,
            height=650,
            title="Pannes actives géolocalisées",
            labels={"analysis_cause_label_fr": "Cause", "taille_carte": "Clients affectés"},
        )
        fig.update_layout(map_style="open-street-map", margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig, width="stretch")

    st.info("Les coordonnées représentent un point approximatif fourni par la source. Les zones réelles de panne peuvent être plus larges.")


# -------------------------------------------------------------------
# Carte historique
# -------------------------------------------------------------------

with tab_history_map:
    st.header("Carte historique des pannes observées")

    if raw_history.empty:
        st.warning("Le fichier historique brut est introuvable ou vide.")
    else:
        history = raw_history.copy()
        required_cols = ["captured_at", "lat", "lon", "outage_id"]
        missing_required = [col for col in required_cols if col not in history.columns]

        if missing_required:
            st.error("Colonnes manquantes pour la carte historique : " + ", ".join(missing_required))
        else:
            history = history.dropna(subset=["captured_at", "lat", "lon"])

            st.markdown(
                """
Cette carte permet d’explorer les pannes observées dans l’historique collecté.
Elle affiche les observations historiques selon une période et des filtres.
"""
            )

            min_date = history["captured_at"].min().date()
            max_date = history["captured_at"].max().date()

            col_filters_1, col_filters_2, col_filters_3 = st.columns(3)
            with col_filters_1:
                selected_dates = st.date_input("Période de capture", value=(min_date, max_date), min_value=min_date, max_value=max_date)
            with col_filters_2:
                history_mode = st.selectbox(
                    "Mode d’affichage",
                    [
                        "Dernière observation par panne dans la période",
                        "Première observation par panne dans la période",
                        "Toutes les observations de la période",
                    ],
                )
            with col_filters_3:
                max_points = st.slider("Nombre maximum de points", min_value=100, max_value=20000, value=3000, step=100)

            if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                start_date, end_date = selected_dates
                history = history[(history["captured_at"].dt.date >= start_date) & (history["captured_at"].dt.date <= end_date)]

            # Enrichissement de l'historique brut à partir de latest_outages.
            if "municipality_label" not in history.columns and "municipality_id" in history.columns:
                lookup_cols = ["municipality_id", "municipality_label", "region_name", "mrc_name"]
                lookup_cols = [col for col in lookup_cols if col in latest.columns]
                if "municipality_id" in lookup_cols:
                    municipality_lookup = latest[lookup_cols].drop_duplicates("municipality_id")
                    history = history.merge(municipality_lookup, on="municipality_id", how="left")

            col_filters_4, col_filters_5, col_filters_6 = st.columns(3)
            with col_filters_4:
                history_min_customers = st.number_input("Clients affectés minimum, historique", min_value=0, value=0, step=1)
            with col_filters_5:
                history_major_only = st.checkbox("Afficher seulement les pannes majeures historiques", value=False)
            with col_filters_6:
                include_unknown_history = st.checkbox("Inclure les causes inconnues, historique", value=True)

            if "customers_affected" in history.columns:
                history = history[history["customers_affected"].fillna(0) >= history_min_customers]
                if history_major_only:
                    history = history[history["customers_affected"].fillna(0) >= major_threshold]

            if "history_cause_label_fr" in history.columns:
                if not include_unknown_history:
                    history = history[history["history_cause_label_fr"].str.lower() != "inconnue"]
                history_cause_options = sorted(history["history_cause_label_fr"].dropna().astype(str).unique())
                selected_history_causes = st.multiselect("Causes historiques", options=history_cause_options, default=[])
                if selected_history_causes:
                    history = history[history["history_cause_label_fr"].isin(selected_history_causes)]

            if "status_fr" in history.columns:
                history_status_options = sorted(history["status_fr"].dropna().astype(str).unique())
                selected_history_statuses = st.multiselect("Statuts historiques", options=history_status_options, default=[])
                if selected_history_statuses:
                    history = history[history["status_fr"].isin(selected_history_statuses)]

            if "region_name" in history.columns:
                history_regions = sorted(history["region_name"].dropna().astype(str).unique())
                selected_history_regions = st.multiselect("Régions historiques", options=history_regions, default=[])
                if selected_history_regions:
                    history = history[history["region_name"].isin(selected_history_regions)]

            if "mrc_name" in history.columns:
                history_mrcs = sorted(history["mrc_name"].dropna().astype(str).unique())
                selected_history_mrcs = st.multiselect("MRC historiques", options=history_mrcs, default=[])
                if selected_history_mrcs:
                    history = history[history["mrc_name"].isin(selected_history_mrcs)]

            municipality_filter_col = "municipality_label" if "municipality_label" in history.columns else "municipality_id"
            if municipality_filter_col in history.columns:
                history_municipalities = sorted(history[municipality_filter_col].dropna().astype(str).unique())
                selected_history_municipalities = st.multiselect("Municipalités historiques", options=history_municipalities, default=[])
                if selected_history_municipalities:
                    history = history[history[municipality_filter_col].astype(str).isin(selected_history_municipalities)]

            if history.empty:
                st.warning("Aucune panne historique ne correspond aux filtres.")
            else:
                if history_mode == "Dernière observation par panne dans la période":
                    history_map = history.sort_values("captured_at").groupby("outage_id", as_index=False).tail(1)
                elif history_mode == "Première observation par panne dans la période":
                    history_map = history.sort_values("captured_at").groupby("outage_id", as_index=False).head(1)
                else:
                    history_map = history.copy()

                if len(history_map) > max_points:
                    history_map = history_map.sort_values("customers_affected", ascending=False).head(max_points)

                h1, h2, h3, h4 = st.columns(4)
                h1.metric("Pannes uniques", format_number(history_map["outage_id"].nunique()))
                h2.metric("Points affichés", format_number(len(history_map)))
                h3.metric("Clients affectés, total points", format_number(history_map["customers_affected"].sum()))
                h4.metric("Clients affectés, max", format_number(history_map["customers_affected"].max()))

                history_map["taille_carte"] = history_map["customers_affected"].fillna(1).clip(lower=1)
                hover_cols = [
                    "outage_id",
                    "customers_affected",
                    "municipality_label",
                    "region_name",
                    "mrc_name",
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
                    size="taille_carte",
                    color="history_cause_label_fr" if "history_cause_label_fr" in history_map.columns else None,
                    hover_data=hover_cols,
                    zoom=5,
                    height=650,
                    title="Carte historique des pannes observées",
                    labels={"history_cause_label_fr": "Cause", "taille_carte": "Clients affectés"},
                )
                fig.update_layout(map_style="open-street-map", margin={"r": 0, "t": 40, "l": 0, "b": 0})
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
                        municipalites_touchees=(municipality_filter_col, "nunique"),
                    )
                )
                history_daily["clients_affectes_moyenne"] = history_daily["clients_affectes_moyenne"].round(2)

                fig_daily = px.line(
                    history_daily,
                    x="date",
                    y=["pannes_uniques", "clients_affectes_max", "municipalites_touchees"],
                    markers=True,
                    title="Historique quotidien selon les filtres",
                    labels={"date": "Date", "value": "Valeur", "variable": "Indicateur"},
                )
                st.plotly_chart(fig_daily, width="stretch")
                show_dataframe(history_daily)

                st.subheader("Données historiques filtrées")
                history_display_cols = [
                    "outage_id",
                    "customers_affected",
                    "municipality_label",
                    "region_name",
                    "mrc_name",
                    "status_fr",
                    "history_cause_label_fr",
                    "captured_at",
                    "start_time",
                    "estimated_restore",
                    "lon",
                    "lat",
                ]
                history_display_cols = [col for col in history_display_cols if col in history_map.columns]
                show_dataframe(history_map[history_display_cols].sort_values("captured_at", ascending=False).head(500), height=400)


# -------------------------------------------------------------------
# Évolution temporelle
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
        selected_range = st.date_input("Période d’analyse", value=(min_date, max_date), min_value=min_date, max_value=max_date, key="trend_date_range")

        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
            daily_filtered = daily_sorted[(daily_sorted["date"].dt.date >= start_date) & (daily_sorted["date"].dt.date <= end_date)]
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
        available_metrics = {label: col for label, col in metric_options.items() if col in daily_filtered.columns}
        selected_metrics = st.multiselect("Indicateurs à afficher", options=list(available_metrics.keys()), default=list(available_metrics.keys())[:3])
        selected_columns = [available_metrics[label] for label in selected_metrics]

        if selected_columns:
            fig = px.line(
                daily_filtered,
                x="date",
                y=selected_columns,
                markers=True,
                title="Tendances quotidiennes",
                labels={"date": "Date", "value": "Valeur", "variable": "Indicateur"},
            )
            st.plotly_chart(fig, width="stretch")

        st.subheader("Sommaire quotidien")
        show_dataframe(daily_filtered)


# -------------------------------------------------------------------
# Analyse des causes
# -------------------------------------------------------------------

with tab_causes:
    st.header("Analyse des causes")
    cause_col = "analysis_cause_label_fr" if "analysis_cause_label_fr" in latest.columns else "analysis_cause_label"
    latest_cause_df = latest.copy()

    if cause_col in latest_cause_df.columns:
        latest_cause_df[cause_col] = latest_cause_df[cause_col].fillna("Inconnue")
        known_rate_all = bool_rate(latest_cause_df["has_known_cause"]) if "has_known_cause" in latest_cause_df.columns else 0.0

        c1, c2, c3 = st.columns(3)
        c1.metric("Pannes uniques", format_number(len(latest_cause_df)))
        c2.metric("Causes connues", f"{known_rate_all} %")
        c3.metric("Causes inconnues", f"{round(100 - known_rate_all, 2)} %")

        left, right = st.columns(2)
        with left:
            st.subheader("Toutes les causes")
            cause_summary = latest_cause_df[cause_col].value_counts().reset_index()
            cause_summary.columns = ["Cause", "Nombre de pannes"]
            fig = px.bar(cause_summary, x="Cause", y="Nombre de pannes", title="Distribution des causes, incluant les causes inconnues")
            st.plotly_chart(fig, width="stretch")

        with right:
            st.subheader("Causes connues seulement")
            known_causes = latest_cause_df[latest_cause_df[cause_col].str.lower() != "inconnue"]
            known_summary = known_causes[cause_col].value_counts().reset_index()
            known_summary.columns = ["Cause", "Nombre de pannes"]
            fig = px.bar(known_summary, x="Cause", y="Nombre de pannes", title="Distribution des causes connues")
            st.plotly_chart(fig, width="stretch")

        st.markdown(
            """
### Interprétation

La cause d’une panne n’est pas toujours disponible dans l’API Hydro-Québec.
Le champ de cause analytique utilise la dernière cause connue observée pour une panne lorsqu’elle existe,
tout en conservant la cause brute de la dernière capture.
"""
        )
    else:
        st.warning("Aucune colonne de cause disponible.")


# -------------------------------------------------------------------
# Pannes à surveiller
# -------------------------------------------------------------------

with tab_monitoring:
    st.header("Pannes à surveiller")

    st.subheader("Pannes majeures actives")
    major_active = active_filtered.copy()
    if "customers_affected" in major_active.columns:
        major_active = major_active[major_active["customers_affected"] >= major_threshold].sort_values("customers_affected", ascending=False)

    cols_major = [
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
    cols_major = [col for col in cols_major if col in major_active.columns]
    show_dataframe(major_active[cols_major])

    st.subheader("Pannes actives observées le plus longtemps")
    long_active = active_filtered.copy()
    if "observed_duration_hours" in long_active.columns:
        long_active = long_active.sort_values("observed_duration_hours", ascending=False)

    cols_long = [
        "outage_id",
        "customers_affected",
        "municipality_label",
        "region_name",
        "mrc_name",
        "status_fr",
        "analysis_cause_label_fr",
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
        "municipality_label",
        "region_name",
        "mrc_name",
        "status_fr",
        "analysis_cause_label_fr",
        "first_capture_at",
        "last_capture_at",
        "capture_count",
        "observed_duration_hours",
        "estimated_restore",
    ]
    cols_recent = [col for col in cols_recent if col in recent.columns]
    show_dataframe(recent[cols_recent].head(25))


# -------------------------------------------------------------------
# Analyse géographique
# -------------------------------------------------------------------

with tab_geo:
    st.header("Analyse géographique")

    st.subheader("Clients affectés par région administrative")
    if "region_name" in active_filtered.columns and "customers_affected" in active_filtered.columns:
        region_summary = (
            active_filtered.groupby("region_name", as_index=False)
            .agg(
                pannes_actives=("outage_id", "nunique"),
                clients_affectes=("customers_affected", "sum"),
                municipalites_touchees=("municipality_label", "nunique"),
            )
            .sort_values("clients_affectes", ascending=False)
        )
        fig = px.bar(
            region_summary,
            x="clients_affectes",
            y="region_name",
            orientation="h",
            title="Régions par clients affectés",
            labels={"clients_affectes": "Clients affectés", "region_name": "Région administrative"},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, width="stretch")
        show_dataframe(region_summary)

    st.subheader("Clients affectés par MRC")
    if "mrc_name" in active_filtered.columns and "customers_affected" in active_filtered.columns:
        mrc_summary = (
            active_filtered.groupby("mrc_name", as_index=False)
            .agg(
                pannes_actives=("outage_id", "nunique"),
                clients_affectes=("customers_affected", "sum"),
                municipalites_touchees=("municipality_label", "nunique"),
            )
            .sort_values("clients_affectes", ascending=False)
            .head(20)
        )
        fig = px.bar(
            mrc_summary,
            x="clients_affectes",
            y="mrc_name",
            orientation="h",
            title="Top MRC par clients affectés",
            labels={"clients_affectes": "Clients affectés", "mrc_name": "MRC"},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, width="stretch")
        show_dataframe(mrc_summary)

    st.subheader("Municipalités les plus touchées")
    if "municipality_label" in active_filtered.columns and "customers_affected" in active_filtered.columns:
        municipality_summary = (
            active_filtered.groupby(["municipality_label", "region_name", "mrc_name"], as_index=False, dropna=False)
            .agg(
                pannes_actives=("outage_id", "nunique"),
                clients_affectes=("customers_affected", "sum"),
                clients_max=("customers_affected", "max"),
            )
            .sort_values("clients_affectes", ascending=False)
            .head(30)
        )
        show_dataframe(municipality_summary)


# -------------------------------------------------------------------
# Qualité des données
# -------------------------------------------------------------------

with tab_quality:
    st.header("Qualité des données")

    if quality.empty:
        st.warning("Aucun rapport qualité disponible.")
    else:
        failed_checks = quality["status"].astype(str).str.lower().eq("fail").sum() if "status" in quality.columns else 0
        warning_checks = quality["severity"].astype(str).str.lower().eq("warning").sum() if "severity" in quality.columns else 0
        total_rows = quality["total_rows"].max() if "total_rows" in quality.columns and not quality["total_rows"].dropna().empty else 0
        geocoded_rate_latest = bool_rate(latest["is_geocoded"]) if "is_geocoded" in latest.columns else 0.0

        q1, q2, q3, q4, q5 = st.columns(5)
        q1.metric("Contrôles qualité", format_number(len(quality)))
        q2.metric("Contrôles échoués", format_number(failed_checks))
        q3.metric("Avertissements", format_number(warning_checks))
        q4.metric("Lignes brutes", format_number(total_rows))
        q5.metric("Municipalités géocodées", f"{geocoded_rate_latest} %")

        st.subheader("Rapport qualité")
        quality_cols = ["check_name_fr", "severity_fr", "status_quality_fr", "rows_affected", "total_rows", "failed_rate_pct", "description", "created_at"]
        quality_cols = [col for col in quality_cols if col in quality.columns]
        show_dataframe(quality[quality_cols])

        if "rows_affected" in quality.columns and "check_name_fr" in quality.columns:
            quality_chart = quality.sort_values("rows_affected", ascending=False)
            fig = px.bar(
                quality_chart,
                x="check_name_fr",
                y="rows_affected",
                title="Nombre de lignes affectées par contrôle qualité",
                labels={"check_name_fr": "Contrôle qualité", "rows_affected": "Lignes affectées"},
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
- Les noms de municipalités sont enrichis par jointure géospatiale à partir des coordonnées.
"""
        )


# -------------------------------------------------------------------
# Tables
# -------------------------------------------------------------------

with tab_tables:
    st.header("Tables analytiques")

    table_choice = st.selectbox(
        "Choisir une table",
        [
            "Pannes actives filtrées",
            "Toutes les pannes actives",
            "Dernière observation par panne",
            "Sommaire quotidien",
            "Rapport qualité",
            "Historique brut",
        ],
    )

    if table_choice == "Pannes actives filtrées":
        show_dataframe(active_filtered, height=600)
        make_download_button(active_filtered, "Télécharger les pannes actives filtrées", "pannes_actives_filtrees.csv")

    elif table_choice == "Toutes les pannes actives":
        show_dataframe(active, height=600)
        make_download_button(active, "Télécharger toutes les pannes actives", "pannes_actives.csv")

    elif table_choice == "Dernière observation par panne":
        show_dataframe(latest, height=600)
        make_download_button(latest, "Télécharger la dernière observation par panne", "dernieres_observations_pannes.csv")

    elif table_choice == "Sommaire quotidien":
        show_dataframe(daily, height=600)
        make_download_button(daily, "Télécharger le sommaire quotidien", "sommaire_quotidien.csv")

    elif table_choice == "Rapport qualité":
        show_dataframe(quality, height=600)
        make_download_button(quality, "Télécharger le rapport qualité", "rapport_qualite.csv")

    elif table_choice == "Historique brut":
        show_dataframe(raw_history.head(5000), height=600)
        make_download_button(raw_history, "Télécharger l’historique brut", "historique_pannes.csv")
