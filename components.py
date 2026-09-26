"""Composants visuels réutilisables du dashboard."""

from __future__ import annotations

import copy
import html

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.config import (
    ACCENT_COLOR,
    CAUSE_COLORS,
    DATA_REQUEST_URL,
    MAP_MARKER_MAX_SIZE,
    MAP_MARKER_MIN_SIZE,
    MAP_STYLE,
    PLOT_TEMPLATE,
)
from dashboard.view_helpers import format_int, get_cause_column, get_geo

def get_data_request_url() -> str:
    """Retourner le lien public du formulaire de demande d'accès."""
    return str(DATA_REQUEST_URL or "").strip()


def render_full_data_access() -> None:
    """Présenter l'accès aux données sans générer d'export côté Supabase."""
    render_section_header("Accès aux données", "Sur demande seulement")
    st.info(
        "Le téléchargement direct est désactivé afin de préserver les ressources "
        "du tableau de bord et de la base de données. Pour obtenir le jeu de données, "
        "remplissez le formulaire de demande."
    )

    request_url = get_data_request_url()

    if not request_url:
        st.warning("Le formulaire de demande n'est pas configuré.")
        return

    st.link_button(
        "📝 Faire une demande d'accès aux données",
        request_url,
        width="stretch",
    )
    st.caption(
        "Le formulaire s'ouvre dans Google Forms. "
    )


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
        geo["taille_carte"] = customers.clip(lower=1).pow(0.35)
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
        center={"lat": 48.4, "lon": -71.8},
        zoom=4.65,
        height=height,
        labels={
            "analysis_cause_label_fr": "Cause",
            "history_cause_label_fr": "Cause",
            "latest_raw_cause_label_fr": "Cause",
            "taille_carte": "Importance visuelle",
        },
    )
    # Points principaux
    fig.update_traces(
        marker=dict(
            sizemin=MAP_MARKER_MIN_SIZE,
        ),
        opacity=0.90,
    )

    # -------------------------------------------------------------------------
    # Halo lumineux autour des points
    # -------------------------------------------------------------------------

    main_traces = list(fig.data)

    for trace in main_traces:
        halo = copy.deepcopy(trace)

        # Ne pas afficher le halo dans la légende
        halo.showlegend = False

        # Le halo ne doit pas avoir son propre tooltip
        halo.hoverinfo = "skip"
        halo.hovertemplate = None

    # Halo très transparent
        halo.opacity = 0.16

    # Agrandir légèrement la couche située derrière le point
        if halo.marker.size is not None:
            halo.marker.size = [
                float(size) * 1.6
                for size in halo.marker.size
            ]

        halo.marker.sizemin = MAP_MARKER_MIN_SIZE + 3

        fig.add_trace(halo)

    # Placer les halos derrière les vrais points
    trace_count = len(main_traces)

    fig.data = (
        tuple(fig.data[trace_count:])
        + tuple(fig.data[:trace_count])
        )
    fig.update_layout(
        template=PLOT_TEMPLATE,
        map_style=MAP_STYLE,
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=5, b=0),
        hoverlabel=dict(
            bgcolor="#111827",
            bordercolor="#334155",
            font=dict(
                color="#f8fafc",
                size=13,
                family="Inter, Segoe UI, Arial",
                ),
            ),
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


def render_compact_ranking(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    rows: int = 6,
    value_suffix: str = "",
) -> None:
    """Afficher un classement compact pour varier le rythme visuel du dashboard."""
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info("Aucune donnée disponible selon les filtres actuels.")
        return

    ordered = df.sort_values(value_col, ascending=False).head(rows)
    items = []
    for rank, (_, row) in enumerate(ordered.iterrows(), start=1):
        label = html.escape(str(row.get(label_col, "Non disponible")))
        value = html.escape(format_int(row.get(value_col, 0)))
        items.append(
            '<div class="priority-row">'
            '<div>'
            f'<div class="priority-name"><span class="muted">{rank:02d}</span> &nbsp;{label}</div>'
            '</div>'
            f'<div class="priority-value">{value}{html.escape(value_suffix)}</div>'
            '</div>'
        )
    st.markdown('<div class="priority-list">' + ''.join(items) + '</div>', unsafe_allow_html=True)


def render_cause_donut(summary: pd.DataFrame, cause_col: str) -> None:
    """Répartition compacte des pannes par cause."""
    if summary.empty:
        st.info("Aucune donnée de cause disponible.")
        return
    fig = px.pie(
        summary,
        names=cause_col,
        values="pannes",
        hole=0.64,
        color=cause_col,
        color_discrete_map=CAUSE_COLORS,
    )
    fig.update_traces(
        textposition="inside",
        textinfo="percent",
        hovertemplate="%{label}<br>Pannes : %{value:,.0f}<br>Part : %{percent}<extra></extra>",
    )
    fig.update_layout(
        template=PLOT_TEMPLATE,
        paper_bgcolor="rgba(0,0,0,0)",
        height=390,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=True,
        legend=dict(title=None, orientation="h", y=-0.08, x=0),
        font=dict(family="Inter, Segoe UI, Arial", color="#cbd5e1"),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_cause_impact(summary: pd.DataFrame, cause_col: str) -> None:
    """Comparer fréquence et impact sans ajouter un second diagramme à barres."""
    if summary.empty:
        st.info("Aucune donnée de cause disponible.")
        return
    chart = summary.copy()
    chart["clients_moyens"] = chart["clients_affectes"] / chart["pannes"].clip(lower=1)
    fig = px.scatter(
        chart,
        x="pannes",
        y="clients_affectes",
        size="clients_moyens",
        size_max=44,
        color=cause_col,
        color_discrete_map=CAUSE_COLORS,
        hover_name=cause_col,
        hover_data={"pannes": True, "clients_affectes": ":,.0f", "clients_moyens": ":,.0f"},
        labels={
            "pannes": "Nombre de pannes",
            "clients_affectes": "Clients affectés",
            "clients_moyens": "Clients moyens / panne",
        },
    )
    fig.update_traces(marker=dict(opacity=0.88, line=dict(width=1, color="rgba(255,255,255,0.18)")))
    fig = clean_chart_layout(fig, height=430, show_legend=True)
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_duration_dotplot(df: pd.DataFrame, rows: int = 10) -> None:
    """Afficher les plus longues durées observées sous forme de dot plot."""
    if df is None or df.empty or "observed_duration_hours" not in df.columns:
        st.info("La durée observée n’est pas disponible dans cette source.")
        return
    chart = df.dropna(subset=["observed_duration_hours"]).copy()
    chart["observed_duration_hours"] = pd.to_numeric(
        chart["observed_duration_hours"], errors="coerce"
    )
    chart = chart.dropna(subset=["observed_duration_hours"])
    chart = chart.sort_values("observed_duration_hours", ascending=False).head(rows)
    if chart.empty or "municipality_label" not in chart.columns:
        st.info("Aucune durée disponible.")
        return
    chart = chart.sort_values("observed_duration_hours")
    fig = px.scatter(
        chart,
        x="observed_duration_hours",
        y="municipality_label",
        size="customers_affected" if "customers_affected" in chart.columns else None,
        size_max=22,
        labels={"observed_duration_hours": "Durée observée, h", "municipality_label": ""},
        hover_data=[c for c in ["customers_affected", "region_name", "status_fr"] if c in chart.columns],
    )
    fig.update_traces(marker=dict(size=12 if "customers_affected" not in chart.columns else None))
    fig = clean_chart_layout(fig, height=430)
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
