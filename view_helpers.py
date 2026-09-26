"""Helpers de transformation et de présentation du dashboard."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from dashboard.config import (
    CAUSE_TRANSLATIONS,
    CHECK_TRANSLATIONS,
    COLUMN_LABELS,
    QUALITY_DESCRIPTION_FR,
    QUALITY_TRANSLATIONS,
    QUEBEC_TIMEZONE,
    STATUS_TRANSLATIONS,
)

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
            "L'accès au jeu complet peut être demandé avec le formulaire"
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
        "Téléchargement direct désactivé. Utilisez le formulaire de demande "
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
