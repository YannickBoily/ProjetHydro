"""Pedagogical near-live presentation of the promoted three-model XGBoost AFT release."""

from __future__ import annotations

import html

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components import clean_chart_layout, render_page_header, render_section_header
from dashboard.ml_live_inference import infer_active_outages, production_model_status
from dashboard.view_helpers import format_int, format_pct, show_table
from dashboard.display_formatting import (
    format_duration_hours,
    format_gap_minutes_fr,
    format_quebec_datetime,
    format_quebec_datetime_short,
    humanize_dataframe,
)


def _nested(mapping: dict, *keys, default=None):
    current = mapping
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _metric_hours(value) -> str:
    return format_duration_hours(value, missing="n/a")


def _pct_rate(value) -> str:
    try:
        if value is None or pd.isna(value):
            return "n/a"
        return format_pct(float(value) * 100.0)
    except (TypeError, ValueError):
        return "n/a"


def _format_duration(value) -> str:
    return format_duration_hours(value)


def _format_local_time(value) -> str:
    return format_quebec_datetime_short(value)


def _format_gap_minutes(value) -> str:
    return format_gap_minutes_fr(value)


def _render_takeaway_card(model_mae, hydro_mae, reduction) -> None:
    model_text = _metric_hours(model_mae)
    hydro_text = _metric_hours(hydro_mae)
    reduction_text = format_pct(float(reduction)) if reduction is not None else "n/a"
    st.markdown(
        f"""
        <div style="
            border:1px solid rgba(56,189,248,.28);
            background:linear-gradient(135deg, rgba(56,189,248,.11), rgba(15,23,42,.36));
            border-radius:16px;
            padding:18px 20px;
            margin:4px 0 22px 0;
        ">
          <div style="font-size:.78rem; letter-spacing:.08em; text-transform:uppercase; color:#7dd3fc; font-weight:700;">
            À retenir
          </div>
          <div style="font-size:1.12rem; line-height:1.55; margin-top:6px; color:#e2e8f0;">
            Sur la période de test gardée hors de l’entraînement, la prévision <strong>mise à jour pendant la panne</strong> réduit l'erreur moyenne de
            <strong>{html.escape(reduction_text)}</strong> par rapport à la dernière estimation Hydro-Québec
            (<strong>{html.escape(model_text)}</strong> contre <strong>{html.escape(hydro_text)}</strong>).
          </div>
          <div style="font-size:.86rem; color:#94a3b8; margin-top:7px;">
            Il s'agit d'une performance moyenne sur les données de test, pas d'une garantie pour une panne individuelle.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_how_it_works() -> None:
    render_section_header("Comment lire les trois estimations", "Du début de la panne jusqu’aux mises à jour successives")
    c1, c2, c3 = st.columns(3, gap="medium")

    with c1:
        st.markdown("#### 1 · Au début, sans estimation Hydro-Québec")
        st.caption(
            "Le modèle estime la durée avec les caractéristiques observées de la panne. "
            "Cette version montre ce que nos données apportent à elles seules."
        )

    with c2:
        st.markdown("#### 2 · Avec la première estimation Hydro-Québec")
        st.caption(
            "La première heure de rétablissement prévue par Hydro-Québec est ajoutée comme information. "
            "Le modèle apprend à la recalibrer."
        )

    with c3:
        st.markdown("#### 3 · Pendant la panne : prévision mise à jour")
        st.caption(
            "À chaque nouveau relevé, le modèle utilise uniquement l'information déjà connue et la dernière estimation Hydro-Québec disponible "
            "pour réestimer le temps restant."
        )


def _initial_comparison_frame(metadata: dict) -> pd.DataFrame:
    benchmark = _nested(metadata, "benchmarks", "initial_hydro_first_eta", default={}) or {}
    models = benchmark.get("models") or {}
    no_eta = models.get("initial_no_eta") or {}
    first_eta = models.get("initial_first_eta") or {}
    hydro = benchmark.get("hydro_quebec") or {}
    return pd.DataFrame(
        [
            {"Approche": "Modèle sans estimation Hydro-Québec", "Erreur moyenne (h)": no_eta.get("midpoint_mae_hours"), "Source": "Modèle ML"},
            {"Approche": "Première estimation Hydro-Québec", "Erreur moyenne (h)": hydro.get("midpoint_mae_hours"), "Source": "Hydro-Québec"},
            {"Approche": "Modèle avec la première estimation Hydro-Québec", "Erreur moyenne (h)": first_eta.get("midpoint_mae_hours"), "Source": "Modèle ML"},
        ]
    ).dropna(subset=["Erreur moyenne (h)"])


def _live_comparison_frame(metadata: dict) -> pd.DataFrame:
    live_hq = _nested(metadata, "benchmarks", "live_hydro_latest_eta", "episode_weighted", default={}) or {}
    return pd.DataFrame(
        [
            {"Approche": "Dernière estimation Hydro-Québec", "Erreur moyenne (h)": live_hq.get("hydro_mae_hours"), "Source": "Hydro-Québec"},
            {"Approche": "Prévision du modèle mise à jour", "Erreur moyenne (h)": live_hq.get("model_mae_hours"), "Source": "Modèle ML"},
        ]
    ).dropna(subset=["Erreur moyenne (h)"])


def _render_comparison_chart(frame: pd.DataFrame, *, height: int) -> None:
    if frame.empty:
        st.info("Les métriques de comparaison ne sont pas disponibles pour cette version du modèle.")
        return

    chart = frame.sort_values("Erreur moyenne (h)", ascending=False).copy()
    chart["Durée lisible"] = chart["Erreur moyenne (h)"].map(format_duration_hours)
    fig = px.bar(
        chart,
        x="Erreur moyenne (h)",
        y="Approche",
        color="Source",
        orientation="h",
        text="Durée lisible",
        custom_data=["Durée lisible"],
        color_discrete_map={"Hydro-Québec": "#64748b", "Modèle ML": "#38bdf8"},
    )
    fig.update_traces(
        texttemplate="%{text}",
        textposition="inside",
        hovertemplate="%{y}<br>Erreur moyenne : %{customdata[0]}<extra></extra>",
    )
    max_hours = float(chart["Erreur moyenne (h)"].max()) if not chart.empty else 0.0
    fig = clean_chart_layout(fig, height=height, show_legend=True)
    if max_hours > 0:
        tickvals = [max_hours * i / 4 for i in range(5)]
        fig.update_xaxes(
            tickvals=tickvals,
            ticktext=[format_duration_hours(value) for value in tickvals],
            title="Erreur moyenne · plus bas = meilleur",
        )
    fig.update_layout(yaxis_title=None)
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def _build_public_table(predicted: pd.DataFrame) -> pd.DataFrame:
    """Build the simplified table intended for non-technical readers."""
    if predicted is None or predicted.empty:
        return pd.DataFrame()

    table = pd.DataFrame(index=predicted.index)

    customers = pd.to_numeric(predicted.get("customers_affected"), errors="coerce")
    table["Clients affectés"] = customers.round().astype("Int64")
    table["Municipalité"] = predicted.get("municipality_label", pd.Series("—", index=predicted.index)).fillna("—")
    table["Cause"] = predicted.get("analysis_cause_label_fr", pd.Series("—", index=predicted.index)).fillna("—")

    age = pd.to_numeric(predicted.get("outage_age_hours_at_capture"), errors="coerce")
    table["Panne active depuis"] = age.map(_format_duration)

    hydro_eta = predicted.get("latest_estimated_restore", pd.Series(index=predicted.index, dtype="object"))
    ml_eta = predicted.get("live_predicted_restore_time", pd.Series(index=predicted.index, dtype="object"))
    gap = pd.to_numeric(predicted.get("live_vs_hydro_minutes"), errors="coerce")
    remaining = pd.to_numeric(predicted.get("live_predicted_remaining_hours"), errors="coerce")

    table["Rétablissement prévu par Hydro-Québec"] = hydro_eta.map(_format_local_time)
    table["Rétablissement prévu par le modèle"] = ml_eta.map(_format_local_time)
    table["Écart avec Hydro-Québec"] = gap.map(_format_gap_minutes)
    table["Temps restant prévu"] = remaining.map(_format_duration)

    # Most operationally important rows first, while keeping unavailable predictions visible at the end.
    table["__sort_customers"] = customers.fillna(-1)
    table["__sort_ml"] = ml_eta.notna().astype(int)
    table = table.sort_values(["__sort_ml", "__sort_customers"], ascending=[False, False])
    return table.drop(columns=["__sort_customers", "__sort_ml"]).reset_index(drop=True)




def _text_value(value, fallback: str) -> str:
    if value is None or pd.isna(value):
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _outage_option_label(row: pd.Series) -> str:
    municipality = _text_value(row.get("municipality_label"), "Municipalité inconnue")
    cause = _text_value(row.get("analysis_cause_label_fr"), "Cause inconnue")
    customers = pd.to_numeric(pd.Series([row.get("customers_affected")]), errors="coerce").iloc[0]
    age = pd.to_numeric(pd.Series([row.get("outage_age_hours_at_capture")]), errors="coerce").iloc[0]
    customer_text = f"{int(customers):,} clients".replace(",", " ") if pd.notna(customers) else "impact inconnu"
    age_text = _format_duration(age)
    outage_id = _text_value(row.get("outage_id"), "")
    suffix = outage_id[-6:]
    return f"{municipality} · {customer_text} · {age_text} · {cause}" + (f" · #{suffix}" if suffix else "")


def _render_selected_outage(predicted: pd.DataFrame) -> None:
    if predicted is None or predicted.empty or "outage_id" not in predicted.columns:
        st.info("Aucune panne active à sélectionner selon les filtres actuels.")
        return

    work = predicted.copy()
    work["outage_id"] = work["outage_id"].astype(str)
    work["__customers"] = pd.to_numeric(work.get("customers_affected"), errors="coerce").fillna(-1)
    work = work.sort_values("__customers", ascending=False)
    labels = {row["outage_id"]: _outage_option_label(row) for _, row in work.iterrows()}
    options = work["outage_id"].tolist()

    selected_id = st.selectbox(
        "Choisir une panne active",
        options,
        format_func=lambda outage_id: labels.get(outage_id, outage_id),
        key="ml_selected_outage_id",
        help="Les pannes sont classées par nombre de clients affectés. Les filtres du dashboard s'appliquent aussi ici.",
    )
    row = work.loc[work["outage_id"] == selected_id].iloc[0]

    municipality = _text_value(row.get("municipality_label"), "Municipalité inconnue")
    region = _text_value(row.get("region_name"), "Région inconnue")
    cause = _text_value(row.get("analysis_cause_label_fr"), "Cause inconnue")
    customers = pd.to_numeric(pd.Series([row.get("customers_affected")]), errors="coerce").iloc[0]
    age = pd.to_numeric(pd.Series([row.get("outage_age_hours_at_capture")]), errors="coerce").iloc[0]
    hydro_eta = _format_local_time(row.get("latest_estimated_restore"))
    ml_eta = _format_local_time(row.get("live_predicted_restore_time"))
    remaining = _format_duration(row.get("live_predicted_remaining_hours"))
    gap = _format_gap_minutes(row.get("live_vs_hydro_minutes"))
    live_available = bool(row.get("live_prediction_available")) if pd.notna(row.get("live_prediction_available")) else False

    st.markdown(f"### {html.escape(municipality)}")
    st.caption(
        f"{html.escape(region)} · {html.escape(cause)} · "
        f"{format_int(customers) if pd.notna(customers) else 'n/a'} clients affectés · active depuis {_format_duration(age)}"
    )

    c1, c2, c3 = st.columns([1, 1, 1], gap="medium")
    with c1:
        st.metric("Rétablissement prévu par Hydro-Québec", hydro_eta)
        st.caption("Dernière heure de rétablissement prévue observée dans les relevés.")
    with c2:
        st.metric("Rétablissement prévu par le modèle", ml_eta if live_available else "Pas encore disponible")
        st.caption("Réestimation du temps restant avec l'information déjà connue.")
    with c3:
        st.metric("Écart avec Hydro-Québec", gap if live_available else "—")
        st.caption("Indique si le modèle prévoit un rétablissement plus tôt ou plus tard.")

    if live_available:
        st.markdown(
            f"**Lecture rapide :** le modèle estime encore **{remaining}** avant le rétablissement, "
            f"soit **{gap}** que la dernière estimation Hydro-Québec observée."
        )
        p1, p2, p3, p4 = st.columns(4)
        for widget, threshold in zip((p1, p2, p3, p4), (1, 2, 4, 8)):
            value = pd.to_numeric(
                pd.Series([row.get(f"live_probability_remaining_over_{threshold}h")]),
                errors="coerce",
            ).iloc[0]
            widget.metric(
                f"Encore active dans plus de {threshold} h",
                format_pct(value * 100.0) if pd.notna(value) else "n/a",
                help=f"Probabilité estimée par le modèle que cette panne soit encore active dans plus de {threshold} heure(s).",
            )
    else:
        st.info(
            "La prévision mise à jour n'est pas encore disponible pour cette panne. Cela arrive notamment lorsqu'aucune estimation Hydro-Québec "
            "n'a encore été observée dans l'épisode courant. Les estimations du début de la panne restent disponibles ci-dessous."
        )

    with st.expander("Voir comment l'estimation a évolué depuis le début de la panne", expanded=False):
        first_eta = _format_local_time(row.get("first_hydro_eta"))
        initial_no_eta = _format_local_time(row.get("initial_no_eta_predicted_restore_time"))
        initial_first_eta = _format_local_time(row.get("initial_first_eta_predicted_restore_time"))
        e1, e2, e3, e4 = st.columns(4)
        e1.metric("Modèle au départ · sans estimation Hydro-Québec", initial_no_eta)
        e2.metric("Première estimation Hydro-Québec", first_eta)
        e3.metric("Modèle au départ · avec estimation Hydro-Québec", initial_first_eta)
        e4.metric("Prévision du modèle maintenant", ml_eta if live_available else "—")
        st.caption(
            "Ces valeurs correspondent à des moments différents : les deux estimations initiales sont figées au début de la panne, "
            "alors que la prévision actuelle est recalculée avec les relevés disponibles jusqu’à maintenant."
        )


def render_ml_overview_teaser() -> None:
    """Show the value of the ML work on the overview without running live inference."""
    status = production_model_status()
    if not status.get("available"):
        return
    metadata = status.get("metadata") or {}
    live_hq = _nested(metadata, "benchmarks", "live_hydro_latest_eta", "episode_weighted", default={}) or {}
    model_mae = live_hq.get("model_mae_hours")
    hydro_mae = live_hq.get("hydro_mae_hours")
    reduction = live_hq.get("mae_reduction_percent")
    closer = live_hq.get("model_closer_rate")

    with st.container(border=True):
        top, action = st.columns([3.2, 1], gap="large")
        with top:
            st.markdown("### 🤖 Prévisions ML de rétablissement")
            st.caption(
                "Une couche ML complète les estimations officielles en recalculant le temps restant à partir des relevés déjà observés."
            )
        with action:
            def _open_ml_page():
                st.session_state["navigation_page"] = "Prévisions ML"
            st.button(
                "Voir les prévisions →",
                key="overview_open_ml",
                width="stretch",
                on_click=_open_ml_page,
            )

        m1, m2, m3 = st.columns(3)
        m1.metric("Erreur moyenne du modèle", _metric_hours(model_mae))
        m2.metric(
            "Amélioration par rapport à Hydro-Québec",
            format_pct(float(reduction)) if reduction is not None else "n/a",
        )
        m3.metric("Modèle plus proche", _pct_rate(closer))
        st.caption(
            "Résultats de la période de test de la version actuellement utilisée. La page d'accueil ne calcule aucune prévision individuelle : "
            "elles sont calculées seulement dans la page Prévisions ML."
        )

def render_ml_page(filtered: pd.DataFrame, *, ml_history: pd.DataFrame | None = None) -> None:
    """Render current inference from a promoted three-model release."""
    render_page_header(
        "Prévisions assistées",
        "Prévisions de rétablissement assistées par ML",
        "Choisissez une panne active pour comparer rapidement l'estimation officielle d'Hydro-Québec et la prévision mise à jour du modèle.",
    )

    status = production_model_status()
    if not status["available"]:
        st.warning(
            "Aucune version des trois modèles n'est actuellement publiée pour Streamlit. Le dashboard n'utilise jamais automatiquement "
            "un nouveau candidat d'entraînement."
        )
        st.code(
            "python scripts/promote_three_model_release.py ml/releases/aft-3model-20261015-c783b769",
            language="powershell",
        )
        return

    metadata = status["metadata"]
    manifest = status["manifest"]
    release_id = status.get("release_id") or "n/a"
    cutoff = manifest.get("dataset_cutoff_utc") or _nested(metadata, "dataset", "cadence", "max_captured_at")

    if ml_history is None or ml_history.empty:
        st.info(
            "Les modèles sont disponibles, mais l'historique du présent épisode des pannes actives n'est pas chargé. "
            "La prévision mise à jour nécessite Supabase afin de reconstruire uniquement les observations déjà connues."
        )
        return

    try:
        predicted = infer_active_outages(filtered, ml_history)
    except Exception as exc:
        st.error(f"Impossible de calculer les prévisions ML actuelles : {exc}")
        return

    live_available = predicted.get(
        "live_prediction_available", pd.Series(False, index=predicted.index)
    ).fillna(False).astype(bool)
    live_count = int(live_available.sum())
    current_capture = pd.to_datetime(
        predicted.get("ml_data_as_of", pd.Series(index=predicted.index, dtype="object")),
        errors="coerce",
        utc=True,
    ).max()

    live_metrics = _nested(metadata, "models", "live_latest_eta", "episode_weighted_metrics", default={}) or {}
    live_hq = _nested(metadata, "benchmarks", "live_hydro_latest_eta", "episode_weighted", default={}) or {}
    model_live_mae = live_hq.get("model_mae_hours", live_metrics.get("midpoint_mae_hours"))
    hydro_live_mae = live_hq.get("hydro_mae_hours")
    reduction = live_hq.get("mae_reduction_percent")

    _render_takeaway_card(model_live_mae, hydro_live_mae, reduction)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pannes actives suivies", format_int(len(predicted)))
    k2.metric("Avec une prévision actuelle", format_int(live_count))
    k3.metric("Erreur moyenne du modèle", _metric_hours(model_live_mae))
    k4.metric(
        "Amélioration par rapport à Hydro-Québec",
        format_pct(float(reduction)) if reduction is not None else "n/a",
    )

    if pd.notna(current_capture):
        st.caption(
            "Prévisions actuelles calculées avec les relevés disponibles jusqu’au "
            f"**{format_quebec_datetime(current_capture)}**. "
            "La collecte étant périodique, il s'agit de près du temps réel."
        )

    render_section_header("Consulter une panne", "Sélectionnez une panne active pour voir sa prévision en un coup d'œil")
    _render_selected_outage(predicted)

    render_section_header("Toutes les pannes actives", "Hydro-Québec et le modèle côte à côte")
    st.caption(
        "L’écart est formulé en langage courant : « plus tôt » signifie que le modèle prévoit un rétablissement avant la dernière estimation Hydro-Québec connue."
    )
    public_table = _build_public_table(predicted)
    if public_table.empty:
        st.info("Aucune panne active à afficher selon les filtres actuels.")
    else:
        st.dataframe(
            public_table,
            width="stretch",
            height=min(525, 76 + 35 * min(len(public_table), 13)),
            hide_index=True,
        )

    _render_how_it_works()

    render_section_header("Comment le modèle a été validé", "Comparaisons sur une période de test gardée hors de l'entraînement")
    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("### Au début de la panne")
        st.caption("Erreur sur la durée totale prévue au premier relevé. Plus bas = meilleur.")
        _render_comparison_chart(_initial_comparison_frame(metadata), height=310)
    with right:
        st.markdown("### Pendant la panne")
        st.caption("Erreur sur le temps restant, aux mêmes moments pour Hydro-Québec et le modèle. Plus bas = meilleur.")
        _render_comparison_chart(_live_comparison_frame(metadata), height=310)

    no_eta_pair = _nested(
        metadata, "benchmarks", "initial_hydro_first_eta", "models", "initial_no_eta", default={}
    ) or {}
    first_eta_pair = _nested(
        metadata, "benchmarks", "initial_hydro_first_eta", "models", "initial_first_eta", default={}
    ) or {}
    hydro_initial = _nested(metadata, "benchmarks", "initial_hydro_first_eta", "hydro_quebec", default={}) or {}

    render_section_header("À retenir", "Les principaux résultats de la version actuelle")
    t1, t2, t3 = st.columns(3, gap="medium")
    with t1:
        st.markdown("#### L’estimation Hydro-Québec apporte une information utile")
        st.caption(
            f"Au départ, le modèle sans estimation Hydro-Québec obtient {_metric_hours(no_eta_pair.get('midpoint_mae_hours'))}. "
            f"Avec la première estimation Hydro-Québec, l’erreur passe à {_metric_hours(first_eta_pair.get('midpoint_mae_hours'))}."
        )
    with t2:
        st.markdown("#### La prévision mise à jour affine l’estimation")
        st.caption(
            f"Pendant la panne, l’erreur moyenne est de {_metric_hours(model_live_mae)} pour le modèle, "
            f"contre {_metric_hours(hydro_live_mae)} pour la dernière estimation Hydro-Québec aux mêmes moments."
        )
    with t3:
        st.markdown("#### Une aide, pas une promesse")
        st.caption(
            f"Le modèle est plus proche dans {_pct_rate(live_hq.get('model_closer_rate'))} des comparaisons pondérées par épisode. "
            "Une panne individuelle peut toutefois évoluer différemment."
        )

    with st.expander("Voir le détail technique des trois modèles", expanded=False):
        st.markdown(
            f"**Identifiant de la version :** `{release_id}`  \n"
            f"**Version technique du modèle :** `{status.get('model_version') or 'n/a'}`  \n"
            + (f"**Données d'entraînement jusqu'au :** `{format_quebec_datetime(cutoff)}`  \n" if cutoff else "")
        )
        st.markdown(
            f"""
            **Au départ, sans estimation Hydro-Québec — erreur moyenne :** {_metric_hours(no_eta_pair.get('midpoint_mae_hours'))}  
            **Première estimation Hydro-Québec — erreur moyenne :** {_metric_hours(hydro_initial.get('midpoint_mae_hours'))}  
            **Au départ, avec la première estimation Hydro-Québec — erreur moyenne :** {_metric_hours(first_eta_pair.get('midpoint_mae_hours'))}  
            **Prévision mise à jour — erreur moyenne :** {_metric_hours(model_live_mae)}  
            **Dernière estimation Hydro-Québec aux mêmes moments — erreur moyenne :** {_metric_hours(hydro_live_mae)}  
            **Modèle plus proche :** {_pct_rate(live_hq.get('model_closer_rate'))}
            """
        )

        display = predicted.copy()
        for col in (
            "live_probability_remaining_over_1h",
            "live_probability_remaining_over_2h",
            "live_probability_remaining_over_4h",
            "live_probability_remaining_over_8h",
        ):
            if col in display:
                values = pd.to_numeric(display[col], errors="coerce") * 100.0
                display[col] = values.map(lambda value: format_pct(value) if pd.notna(value) else "—")

        display = humanize_dataframe(
            display,
            [
                "customers_affected",
                "municipality_label",
                "region_name",
                "analysis_cause_label_fr",
                "outage_age_hours_at_capture",
                "first_hydro_eta",
                "initial_no_eta_predicted_restore_time",
                "initial_first_eta_predicted_restore_time",
                "latest_estimated_restore",
                "live_predicted_restore_time",
                "live_predicted_remaining_hours",
                "live_vs_hydro_minutes",
                "live_probability_remaining_over_1h",
                "live_probability_remaining_over_2h",
                "live_probability_remaining_over_4h",
                "ml_data_as_of",
            ],
        )
        show_table(display, height=430)

        st.markdown(
            """
            **Au départ, sans estimation Hydro-Québec.** Prédiction faite avec le premier relevé du présent épisode sans utiliser l'estimation d'Hydro-Québec.

            **Au départ, avec la première estimation Hydro-Québec.** La première estimation déjà connue est ajoutée comme variable; une révision future ne peut pas modifier cette prédiction initiale.

            **Pendant la panne.** Le modèle utilise uniquement l'historique déjà observé dans le présent épisode et prédit le temps restant.

            Le modèle est entraîné hors ligne. Ouvrir Streamlit ne réentraîne rien : l'application charge uniquement la version publiée du modèle pour calculer les prévisions.
            """
        )

    st.info(
        "Les prévisions ML sont expérimentales et servent à l'analyse. L'heure officielle de rétablissement demeure celle publiée par Hydro-Québec."
    )
