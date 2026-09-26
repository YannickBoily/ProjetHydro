"""Tableau de bord Streamlit consacré aux pannes électriques au Québec.

L'application peut charger les données depuis des fichiers CSV locaux ou depuis
une base PostgreSQL hébergée sur Supabase. Elle fournit des vues opérationnelles,
cartographiques et historiques, ainsi que des contrôles de qualité et un accès aux données sur demande.

Le fichier reste autonome pour faciliter son déploiement sur
Streamlit Community Cloud. Les fonctions sont regroupées par responsabilité :
chargement, normalisation, enrichissement, rendu des composants et pages.
"""

from __future__ import annotations

import html
import os
import copy
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

_REPO_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT_FOR_IMPORTS) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FOR_IMPORTS))


from dashboard.config import ACCENT_COLOR, ACTIVE_FILE, DAILY_FILE, LATEST_FILE, QUALITY_FILE, RAW_FILE, SOURCE_LIMIT_CHECKS
from dashboard.data_access import (
    load_csv,
    load_supabase_active,
    load_supabase_daily_summary,
    load_supabase_history,
    load_supabase_latest,
    load_supabase_latest_metrics,
    load_supabase_quality_report,
    load_supabase_recent_outages,
    using_supabase,
)
from dashboard.view_helpers import (
    add_display_columns,
    bool_rate,
    build_active_snapshot_at_time,
    enrich_raw_history,
    format_int,
    format_pct,
    format_quebec_datetime,
    get_cause_column,
    get_geo,
    latest_timestamp,
    make_download,
    prepare_quality_report,
    show_table,
)
from dashboard.components import (
    clean_chart_layout,
    render_cause_donut,
    render_cause_impact,
    render_clean_map,
    render_compact_ranking,
    render_duration_dotplot,
    render_full_data_access,
    render_horizontal_ranking,
    render_page_header,
    render_priority_list,
    render_section_header,
    render_status,
    representative_outages,
    safe_numeric_sum,
    unique_outage_count,
)



# =============================================================================
# Configuration générale
# =============================================================================




st.set_page_config(
    page_title="Hydro-Québec | Suivi des pannes",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# Google Analytics 4
# =============================================================================

GA_MEASUREMENT_ID = "G-RLV9TKJE88"

GA_TRACKER_JS = f"""
export default function(component) {{
    const pageName = component.data?.page || "Dashboard";

    // Charger Google Analytics une seule fois
    if (!window.__ga4DashboardLoaded) {{
        window.dataLayer = window.dataLayer || [];

        window.gtag = window.gtag || function() {{
            window.dataLayer.push(arguments);
        }};

        const script = document.createElement("script");
        script.async = true;
        script.src = "https://www.googletagmanager.com/gtag/js?id={GA_MEASUREMENT_ID}";
        document.head.appendChild(script);

        window.gtag("js", new Date());

        // On désactive le page_view automatique,
        // car Streamlit rerun souvent le script Python.
        window.gtag("config", "{GA_MEASUREMENT_ID}", {{
            send_page_view: false
        }});

        window.__ga4DashboardLoaded = true;
    }}

    // Créer un nom de page propre
    const slug = pageName
        .toLowerCase()
        .normalize("NFD")
        .replace(/[\\u0300-\\u036f]/g, "")
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "");

    // Éviter qu'un simple rerun Streamlit crée un faux page_view
    const storageKey = "ga4_dashboard_last_page";
    const lastPage = sessionStorage.getItem(storageKey);

    if (lastPage !== pageName) {{
        window.gtag("event", "page_view", {{
            page_title: "Dashboard | " + pageName,
            page_location: window.location.href,
            page_path: "/dashboard/" + slug
        }});

        sessionStorage.setItem(storageKey, pageName);
    }}
}}
"""

ga_tracker = st.components.v2.component(
    "google_analytics_tracker",
    js=GA_TRACKER_JS,
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
        --hq-accent: #38bdf8;
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
        border-color: rgba(56, 189, 248, 0.36);
        background: rgba(56, 189, 248, 0.09);
        color: #bae6fd;
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










# Chargement et normalisation des données




# Accès à Supabase / PostgreSQL






































# =============================================================================
# Accès aux données sur demande
# =============================================================================

# Lien public du Google Form utilisé pour les demandes d'accès.































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
st.sidebar.caption("Projet data · snapshots de pannes au Québec")

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
ga_tracker(
    data={"page": page},
    key="ga4_dashboard_tracker",
)
st.sidebar.divider()
st.sidebar.caption(f"Source : {DATA_SOURCE}")

if st.sidebar.button("🔄 Recharger depuis Supabase", width="stretch"):
    if using_supabase():
        load_supabase_active.clear()
        load_supabase_recent_outages.clear()
        load_supabase_latest_metrics.clear()
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

    territory_source = filter_source.copy()
    if selected_regions and "region_name" in territory_source.columns:
        territory_source = territory_source[territory_source["region_name"].isin(selected_regions)]

    mrc_options = (
        sorted(territory_source["mrc_name"].dropna().astype(str).unique())
        if "mrc_name" in territory_source.columns
        else []
    )
    if "filter_mrcs" in st.session_state:
        st.session_state["filter_mrcs"] = [
            value for value in st.session_state["filter_mrcs"] if value in mrc_options
        ]
    selected_mrcs = st.multiselect("MRC", mrc_options, key="filter_mrcs")

    municipality_source = territory_source.copy()
    if selected_mrcs and "mrc_name" in municipality_source.columns:
        municipality_source = municipality_source[municipality_source["mrc_name"].isin(selected_mrcs)]

    municipality_options = (
        sorted(municipality_source["municipality_label"].dropna().astype(str).unique())
        if "municipality_label" in municipality_source.columns
        else []
    )
    if "filter_municipalities" in st.session_state:
        st.session_state["filter_municipalities"] = [
            value
            for value in st.session_state["filter_municipalities"]
            if value in municipality_options
        ]
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
            <div class="app-subtitle">Québec · collecte par snapshots, analyse et qualité des données</div>
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
        "Portfolio data",
        "Situation actuelle des pannes",
        "Une vue synthétique construite à partir de snapshots publics : situation actuelle, "
        "concentration géographique et évolution récente.",
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
                pd.to_numeric(filtered["customers_affected"], errors="coerce").fillna(0)
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
    k4.metric("Pannes majeures", format_int(major_count), help=f"Seuil actuel : {format_int(major_threshold)} clients")

    map_col, priority_col = st.columns([1.65, 0.85], gap="large")
    with map_col:
        render_section_header("Situation au Québec", "Taille des points : clients affectés")
        render_clean_map(filtered, height=565, max_points=500)

    with priority_col:
        render_section_header("À surveiller", "Principaux impacts actuels")
        render_priority_list(filtered, rows=7)
        st.caption(
            "Les heures de rétablissement sont affichées uniquement lorsqu’elles ont été capturées dans la source."
        )

    left, right = st.columns([1.2, 1], gap="large")
    with left:
        render_section_header("Évolution récente", "Maximum quotidien de clients affectés")
        if not daily.empty and {"date", "max_customers_affected"}.issubset(daily.columns):
            trend = daily.dropna(subset=["date"]).sort_values("date").tail(45).copy()
            fig = px.line(
                trend,
                x="date",
                y="max_customers_affected",
                color_discrete_sequence=[ACCENT_COLOR],
                labels={"date": "Date", "max_customers_affected": "Clients affectés"},
            )
            fig.update_traces(line=dict(width=3), hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f} clients<extra></extra>")
            fig = clean_chart_layout(fig, height=335)
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
        else:
            st.info("L’historique quotidien n’est pas disponible.")

    with right:
        render_section_header("Territoires les plus touchés", "Top 6 régions")
        if {"region_name", "customers_affected"}.issubset(filtered.columns) and not filtered.empty:
            region_summary = (
                filtered.groupby("region_name", as_index=False)
                .agg(clients_affectes=("customers_affected", "sum"))
                .sort_values("clients_affectes", ascending=False)
            )
            render_compact_ranking(region_summary, "region_name", "clients_affectes", rows=6)
        else:
            st.info("Aucune donnée régionale selon les filtres actuels.")

    with st.expander("Méthodologie et limites de la collecte", expanded=False):
        st.markdown(
            """
            **Collecte.** Le jeu de données est construit à partir de snapshots successifs des pannes publiées par Hydro-Québec.

            **Conséquence.** Certaines informations peuvent ne jamais être observées : une panne peut être rétablie entre deux captures, et une heure estimée de rétablissement ou une cause peut être publiée tardivement sans être capturée.

            **Interprétation.** Une valeur absente signifie donc *non observée dans les snapshots disponibles*, et non nécessairement *information inexistante à la source*.
            """
        )


# =============================================================================
# Explorateur cartographique
# =============================================================================

elif page == "Explorer la carte":
    render_page_header(
        "Géographie",
        "Explorer les pannes sur la carte",
        "Une vue spatiale de la situation active, centrée sur le Québec et filtrable par territoire, cause et impact.",
    )

    map_data = filtered.copy()
    render_status(f"Situation observée le {updated_display}.", "good")

    if map_data.empty:
        st.info("Aucune panne à afficher selon les filtres actuels.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Pannes uniques", format_int(unique_outage_count(map_data)))
        k2.metric("Clients représentés", format_int(safe_numeric_sum(map_data, "customers_affected")))
        k3.metric(
            "Municipalités",
            format_int(map_data["municipality_label"].nunique() if "municipality_label" in map_data.columns else 0),
        )
        geo_rate = len(get_geo(map_data)) / max(len(map_data), 1) * 100
        k4.metric("Observations géocodées", format_pct(geo_rate))

        render_section_header("Carte interactive", "Le cadrage initial couvre le Québec")
        render_clean_map(map_data, height=700)

        summary_col, cause_col_ui = st.columns([1, 1], gap="large")
        with summary_col:
            render_section_header("Municipalités les plus touchées", "Top 8")
            if {"municipality_label", "customers_affected"}.issubset(map_data.columns):
                top_mun = (
                    map_data.groupby("municipality_label", as_index=False)
                    .agg(clients_affectes=("customers_affected", "sum"))
                    .sort_values("clients_affectes", ascending=False)
                )
                render_compact_ranking(top_mun, "municipality_label", "clients_affectes", rows=8)

        with cause_col_ui:
            render_section_header("Répartition des causes", "Lorsque la cause a été observée")
            cause_col = get_cause_column(map_data)
            if cause_col:
                cause_summary = (
                    map_data[cause_col].fillna("Inconnue").value_counts().rename_axis("cause").reset_index(name="pannes")
                )
                render_cause_donut(cause_summary, "cause")

        with st.expander("Voir les données de la carte", expanded=False):
            table_cols = [
                "customers_affected", "municipality_label", "mrc_name", "region_name",
                "status_fr", get_cause_column(map_data), "active_capture_at", "start_time", "estimated_restore",
            ]
            table_cols = [col for col in table_cols if col]
            table_data = map_data.sort_values("customers_affected", ascending=False) if "customers_affected" in map_data.columns else map_data
            show_table(table_data, table_cols, height=520)

    render_full_data_access()


# =============================================================================
# Analyse territoriale
# =============================================================================

elif page == "Analyse territoriale":
    render_page_header(
        "Territoires",
        "Analyse territoriale",
        "Comparer l’impact des pannes actives à différents niveaux géographiques.",
    )

    analysis_level = st.radio(
        "Niveau d’analyse", ["Régions", "MRC", "Municipalités"], horizontal=True, key="territory_level"
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
        aggregations = {"clients_affectes": ("customers_affected", "sum"), "clients_max": ("customers_affected", "max")}
        if "outage_id" in filtered.columns:
            aggregations["pannes"] = ("outage_id", "nunique")
        ranking = (
            filtered.dropna(subset=[group_col]).groupby(group_col, as_index=False).agg(**aggregations)
            .sort_values("clients_affectes", ascending=False)
        )
        if "pannes" not in ranking.columns:
            ranking["pannes"] = 0

        top_name = ranking.iloc[0][group_col] if not ranking.empty else "—"
        top_clients = ranking.iloc[0]["clients_affectes"] if not ranking.empty else 0
        total_clients = ranking["clients_affectes"].sum() if not ranking.empty else 0
        top3_share = ranking.head(3)["clients_affectes"].sum() / total_clients * 100 if total_clients > 0 else 0

        k1, k2, k3 = st.columns(3)
        k1.metric(f"{group_label} la plus touchée", str(top_name))
        k2.metric("Clients dans ce territoire", format_int(top_clients))
        k3.metric("Part des 3 premiers", format_pct(top3_share))

        chart_col, table_col = st.columns([1.25, 1], gap="large")
        with chart_col:
            render_section_header(f"Clients affectés par {group_label.lower()}", "Classement")
            render_horizontal_ranking(ranking, group_col, "clients_affectes", height=530, max_rows=15)
        with table_col:
            render_section_header("Lecture détaillée", "Pannes et impact maximum")
            detail = ranking.head(15).rename(columns={
                group_col: group_label, "pannes": "Pannes", "clients_affectes": "Clients affectés", "clients_max": "Impact max",
            })
            st.dataframe(detail, width="stretch", hide_index=True, height=530)


# =============================================================================
# Causes
# =============================================================================

elif page == "Causes":
    render_page_header(
        "Origine",
        "Causes observées",
        "Analyse des causes capturées dans les snapshots. Certaines causes peuvent être publiées tardivement et ne jamais être observées avant le rétablissement.",
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
                pannes=(("outage_id", "nunique") if "outage_id" in cause_data.columns else (cause_col, "size")),
                clients_affectes=(("customers_affected", "sum") if "customers_affected" in cause_data.columns else (cause_col, "size")),
            )
            .sort_values("pannes", ascending=False)
        )

        known_df = cause_data[cause_data[cause_col] != "Inconnue"]
        known_rate = unique_outage_count(known_df) / max(unique_outage_count(cause_data), 1) * 100
        unknown_count = unique_outage_count(cause_data[cause_data[cause_col] == "Inconnue"])
        known_summary = summary[summary[cause_col] != "Inconnue"].sort_values("pannes", ascending=False)
        top_known = known_summary.iloc[0][cause_col] if not known_summary.empty else "Non disponible"

        k1, k2, k3 = st.columns(3)
        k1.metric("Pannes avec cause observée", format_pct(known_rate))
        k2.metric("Cause non observée", format_int(unknown_count))
        k3.metric("Cause connue la plus fréquente", str(top_known))

        left, right = st.columns([0.9, 1.35], gap="large")
        with left:
            render_section_header("Répartition", "Part des pannes actives")
            render_cause_donut(summary, cause_col)
        with right:
            render_section_header("Fréquence × impact", "Taille : clients moyens par panne")
            render_cause_impact(summary, cause_col)

        render_status(
            "Une cause absente signifie qu’elle n’a pas été observée dans les snapshots disponibles; elle ne constitue pas automatiquement une erreur de données.",
            "warning",
        )


# =============================================================================
# Surveillance
# =============================================================================

elif page == "Surveillance":
    render_page_header(
        "Suivi",
        "Pannes à surveiller",
        "Priorisation à partir de l’impact et de la durée observée. Les ETA ne sont présentés que lorsqu’ils ont été capturés.",
    )

    priority = filtered.copy()
    if "customers_affected" in priority.columns:
        priority["customers_affected"] = pd.to_numeric(priority["customers_affected"], errors="coerce").fillna(0)
        priority = priority.sort_values("customers_affected", ascending=False)

    major = priority[priority["customers_affected"] >= major_threshold] if "customers_affected" in priority.columns else pd.DataFrame()
    longest_hours = (
        pd.to_numeric(priority["observed_duration_hours"], errors="coerce").max()
        if "observed_duration_hours" in priority.columns and not priority.empty else 0
    )
    eta_observed = (
        priority["estimated_restore"].notna().mean() * 100
        if "estimated_restore" in priority.columns and not priority.empty else 0
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pannes majeures", format_int(unique_outage_count(major)))
    k2.metric("Clients concernés", format_int(safe_numeric_sum(major, "customers_affected")))
    k3.metric("Durée observée max", f"{float(longest_hours or 0):.1f} h")
    k4.metric("ETA observé", format_pct(eta_observed), help="Part des pannes actives pour lesquelles un ETA a été capturé dans les snapshots.")

    if major.empty:
        render_status("Aucune panne majeure active selon le seuil sélectionné.", "good")
    else:
        render_status(
            f"{unique_outage_count(major)} panne(s) dépassent le seuil de {format_int(major_threshold)} clients.",
            "danger",
        )

    left, right = st.columns([1.15, 1], gap="large")
    with left:
        render_section_header("Plus forts impacts", "Pannes actives")
        major_cols = [
            "customers_affected", "municipality_label", "region_name", "analysis_cause_label_fr",
            "status_fr", "observed_duration_hours", "estimated_restore",
        ]
        show_table(priority.head(15), major_cols, height=455)

    with right:
        render_section_header("Durées observées les plus longues", "Indépendamment de l’impact")
        render_duration_dotplot(priority, rows=10)

    render_section_header("Dernières pannes détectées", "Première apparition dans les snapshots")
    recent = recent_outages.copy() if using_supabase() else latest.copy()
    if "first_capture_at" in recent.columns:
        recent = recent.sort_values("first_capture_at", ascending=False)
    recent_cols = [
        "customers_affected", "municipality_label", "region_name", "mrc_name", "status_fr",
        "analysis_cause_label_fr", "first_capture_at", "estimated_restore",
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
