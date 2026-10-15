"""Near-live presentation of the promoted three-model XGBoost AFT release."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components import clean_chart_layout, render_page_header, render_section_header
from dashboard.ml_live_inference import infer_active_outages, production_model_status
from dashboard.view_helpers import format_int, format_pct, show_table


def _nested(mapping: dict, *keys, default=None):
    current = mapping
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _metric_hours(value) -> str:
    try:
        if value is None or pd.isna(value):
            return "n/a"
        return f"{float(value):.2f} h"
    except (TypeError, ValueError):
        return "n/a"


def _pct_rate(value) -> str:
    try:
        if value is None or pd.isna(value):
            return "n/a"
        return format_pct(float(value) * 100.0)
    except (TypeError, ValueError):
        return "n/a"


def _comparison_frame(metadata: dict) -> pd.DataFrame:
    initial_hq = _nested(metadata, "benchmarks", "initial_hydro_first_eta", default={}) or {}
    live_hq = _nested(metadata, "benchmarks", "live_hydro_latest_eta", "episode_weighted", default={}) or {}
    no_eta = (initial_hq.get("models") or {}).get("initial_no_eta") or {}
    first_eta = (initial_hq.get("models") or {}).get("initial_first_eta") or {}
    hydro_initial = initial_hq.get("hydro_quebec") or {}
    return pd.DataFrame(
        [
            {
                "Moment": "Initial",
                "Estimation": "AFT sans ETA Hydro",
                "MAE (h)": no_eta.get("midpoint_mae_hours"),
            },
            {
                "Moment": "Initial",
                "Estimation": "ETA Hydro initiale",
                "MAE (h)": hydro_initial.get("midpoint_mae_hours"),
            },
            {
                "Moment": "Initial",
                "Estimation": "AFT + première ETA",
                "MAE (h)": first_eta.get("midpoint_mae_hours"),
            },
            {
                "Moment": "Live",
                "Estimation": "Dernière ETA Hydro",
                "MAE (h)": live_hq.get("hydro_mae_hours"),
            },
            {
                "Moment": "Live",
                "Estimation": "AFT live + dernière ETA",
                "MAE (h)": live_hq.get("model_mae_hours"),
            },
        ]
    ).dropna(subset=["MAE (h)"])


def render_ml_page(filtered: pd.DataFrame, *, ml_history: pd.DataFrame | None = None) -> None:
    """Render current inference from a promoted three-model release."""
    render_page_header(
        "Machine learning",
        "Prévisions ML près du temps réel",
        "Trois modèles XGBoost AFT séparent la valeur de nos variables, de la première ETA Hydro-Québec "
        "et de la dernière ETA disponible pendant une panne active.",
    )

    status = production_model_status()
    if not status["available"]:
        st.warning(
            "Aucune release trois-modèles n'est promue pour Streamlit. Le dashboard n'utilise jamais automatiquement "
            "un nouveau candidat d'entraînement."
        )
        st.code(
            "python scripts/promote_three_model_release.py "
            "ml/releases/aft-3model-20261015-c783b769",
            language="powershell",
        )
        st.caption(
            "Après la promotion, commit/push le dossier ml/artifacts/three_model_production pour que Streamlit Cloud "
            "dispose des modèles."
        )
        return

    metadata = status["metadata"]
    manifest = status["manifest"]
    release_id = status.get("release_id") or "n/a"
    cutoff = manifest.get("dataset_cutoff_utc") or _nested(metadata, "dataset", "cadence", "max_captured_at")

    st.caption(
        f"Release servie : `{release_id}` · modèle : `{status.get('model_version') or 'n/a'}`"
        + (f" · données d'entraînement jusqu'au {cutoff}" if cutoff else "")
    )

    if ml_history is None or ml_history.empty:
        st.info(
            "Les modèles sont disponibles, mais l'historique du présent épisode des pannes actives n'est pas chargé. "
            "La prédiction live nécessite Supabase afin de reconstruire uniquement les observations déjà connues."
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

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pannes actives", format_int(len(predicted)))
    k2.metric(
        "Avec prévision live",
        format_int(live_count),
        help="Le modèle live devient disponible dès qu'une ETA Hydro a été observée dans l'épisode courant.",
    )
    k3.metric("MAE live test", _metric_hours(live_hq.get("model_mae_hours", live_metrics.get("midpoint_mae_hours"))))
    reduction = live_hq.get("mae_reduction_percent")
    k4.metric(
        "Gain vs dernière ETA HQ",
        format_pct(float(reduction)) if reduction is not None else "n/a",
        help="Réduction de MAE episode-weighted sur le test temporel de la release, pas une garantie pour une panne individuelle.",
    )

    if pd.notna(current_capture):
        st.caption(
            "Prévisions actuelles calculées à partir des snapshots disponibles jusqu'à "
            f"**{current_capture.tz_convert('America/Toronto').strftime('%Y-%m-%d %H:%M %Z')}**. "
            "La collecte étant périodique, il s'agit de near-live et non de temps réel à la seconde."
        )

    render_section_header(
        "Pannes actives",
        "Estimations recalculées à l'ouverture de la page avec la release figée",
    )

    display = predicted.copy()
    for col in (
        "live_probability_remaining_over_1h",
        "live_probability_remaining_over_2h",
        "live_probability_remaining_over_4h",
        "live_probability_remaining_over_8h",
    ):
        if col in display:
            display[col] = pd.to_numeric(display[col], errors="coerce") * 100.0

    if "live_predicted_remaining_hours" in display:
        display = display.sort_values(
            ["live_prediction_available", "live_predicted_remaining_hours"],
            ascending=[False, False],
            na_position="last",
        )

    show_table(
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
        height=560,
    )

    st.caption(
        "Écart ML − Hydro : une valeur négative signifie que le modèle prévoit un rétablissement plus tôt que la "
        "dernière ETA Hydro connue; une valeur positive signifie plus tard."
    )

    left, right = st.columns([1.1, 1], gap="large")
    with left:
        render_section_header("Valeur ajoutée des trois approches", "Même release, tests temporels préservés")
        comparison = _comparison_frame(metadata)
        if not comparison.empty:
            fig = px.bar(
                comparison,
                x="MAE (h)",
                y="Estimation",
                color="Moment",
                orientation="h",
                text_auto=".2f",
            )
            fig = clean_chart_layout(fig, height=390)
            fig.update_layout(yaxis={"categoryorder": "total descending"})
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
            st.caption(
                "Les métriques initiales portent sur la durée totale. Les métriques live portent sur le temps restant; "
                "elles servent surtout à comparer ML live et dernière ETA Hydro au même moment."
            )

    with right:
        render_section_header("Validation de la release", "Résultats à retenir")
        no_eta_pair = _nested(
            metadata, "benchmarks", "initial_hydro_first_eta", "models", "initial_no_eta", default={}
        ) or {}
        first_eta_pair = _nested(
            metadata, "benchmarks", "initial_hydro_first_eta", "models", "initial_first_eta", default={}
        ) or {}
        hydro_initial = _nested(metadata, "benchmarks", "initial_hydro_first_eta", "hydro_quebec", default={}) or {}
        st.markdown(
            f"""
            **Initial sans ETA — MAE sur cas comparables HQ :** {_metric_hours(no_eta_pair.get('midpoint_mae_hours'))}  
            **ETA Hydro initiale :** {_metric_hours(hydro_initial.get('midpoint_mae_hours'))}  
            **Initial + première ETA :** {_metric_hours(first_eta_pair.get('midpoint_mae_hours'))}  
            **Live + dernière ETA :** {_metric_hours(live_hq.get('model_mae_hours'))}  
            **Dernière ETA Hydro, mêmes moments live :** {_metric_hours(live_hq.get('hydro_mae_hours'))}  
            **ML live plus proche :** {_pct_rate(live_hq.get('model_closer_rate'))}
            """
        )

    live_rows = predicted.loc[live_available].copy()
    if not live_rows.empty:
        render_section_header("Risque restant", "Probabilités moyennes des pannes live actuellement prédites")
        p1, p2, p3, p4 = st.columns(4)
        for widget, threshold in zip((p1, p2, p3, p4), (1, 2, 4, 8)):
            col = f"live_probability_remaining_over_{threshold}h"
            value = pd.to_numeric(live_rows.get(col), errors="coerce").mean()
            widget.metric(
                f"P(reste > {threshold} h)",
                format_pct(value * 100) if pd.notna(value) else "n/a",
            )

    with st.expander("Comment les trois prévisions sont calculées", expanded=False):
        st.markdown(
            """
            **Initial sans ETA.** Prédiction faite avec le premier snapshot du présent épisode sans utiliser
            l'estimation Hydro-Québec. Elle mesure ce que les autres variables apportent à elles seules.

            **Initial + première ETA.** Même moment de prédiction, mais l'ETA visible au premier snapshot est ajoutée
            comme feature. Une révision publiée plus tard ne peut pas modifier cette prédiction initiale.

            **Live + dernière ETA.** L'historique du présent épisode est reconstruit jusqu'au dernier snapshot connu.
            Les ETA manquantes sont propagées seulement vers le futur à l'intérieur de l'épisode. Le modèle prédit le
            **temps restant**, puis l'application le convertit en heure de rétablissement estimée.

            Le modèle est entraîné hors ligne. Ouvrir Streamlit ne réentraîne rien : l'application charge la release
            promue et exécute uniquement l'inférence XGBoost sur les valeurs actuellement présentes dans Supabase.

            **Limite.** Ces estimations sont expérimentales et ne remplacent pas l'heure officielle publiée par
            Hydro-Québec.
            """
        )
