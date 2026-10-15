"""Near-live inference for the promoted three-model AFT bundle.

Training is intentionally separate from inference.  Streamlit loads a promoted
release once, builds features from snapshots already present in Supabase, and
runs very small XGBoost predictions on demand.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import streamlit as st
except ImportError:  # Allows model/inference unit tests without the UI dependency.
    st = None

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRODUCTION_DIR = ROOT / "ml" / "artifacts" / "three_model_production"


def _artifact_fingerprint(directory: Path) -> str:
    files = [
        directory / "three_model_metadata.json",
        directory / "production_manifest.json",
        directory / "initial_no_eta" / "candidate_model.json",
        directory / "initial_first_eta" / "candidate_model.json",
        directory / "live_latest_eta" / "candidate_model.json",
    ]
    parts = []
    for path in files:
        if not path.exists():
            return "missing"
        stat = path.stat()
        parts.append(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}")
    return "|".join(parts)


def production_model_status(directory: Path = DEFAULT_PRODUCTION_DIR) -> dict[str, Any]:
    directory = Path(directory)
    metadata_path = directory / "three_model_metadata.json"
    manifest_path = directory / "production_manifest.json"
    available = _artifact_fingerprint(directory) != "missing"
    if not available:
        return {
            "available": False,
            "directory": str(directory),
            "release_id": None,
            "model_version": None,
            "metadata": {},
            "manifest": {},
        }
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "available": True,
        "directory": str(directory),
        "release_id": manifest.get("production_release_id"),
        "model_version": metadata.get("model_version"),
        "metadata": metadata,
        "manifest": manifest,
    }


def _load_bundle_impl(directory: str, fingerprint: str):
    # Lazy import: XGBoost is loaded only when inference actually needs it.
    from ml.three_model_predict import load_three_model_bundle

    if fingerprint == "missing":
        raise FileNotFoundError("No promoted three-model artifact is available.")
    return load_three_model_bundle(Path(directory), kind="candidate")


if st is not None:
    _load_bundle_cached = st.cache_resource(show_spinner=False)(_load_bundle_impl)
else:
    _load_bundle_cached = lru_cache(maxsize=4)(_load_bundle_impl)


def load_production_bundle(directory: Path = DEFAULT_PRODUCTION_DIR):
    directory = Path(directory)
    fingerprint = _artifact_fingerprint(directory)
    return _load_bundle_cached(str(directory), fingerprint)


def _utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True, format="mixed")


def infer_active_outages(
    active: pd.DataFrame,
    current_episode_history: pd.DataFrame,
    *,
    artifact_dir: Path = DEFAULT_PRODUCTION_DIR,
) -> pd.DataFrame:
    """Attach the three current model estimates to active outages.

    ``current_episode_history`` must contain only the *current continuous episode*
    for every active outage and must include an ``episode_id`` column.  This is
    exactly what ``load_supabase_active_ml_history`` returns.
    """
    if active is None or active.empty:
        return pd.DataFrame() if active is None else active.copy()

    result = active.copy()
    history = current_episode_history.copy()
    if history.empty:
        return result

    result["outage_id"] = result["outage_id"].astype(str)
    history["outage_id"] = history["outage_id"].astype(str)
    active_ids = set(result["outage_id"])
    history = history.loc[history["outage_id"].isin(active_ids)].copy()
    if history.empty:
        return result

    history["captured_at"] = _utc(history["captured_at"])
    history["start_time"] = _utc(history["start_time"])
    if "estimated_restore" in history:
        history["estimated_restore"] = _utc(history["estimated_restore"])
    history = history.dropna(subset=["episode_id", "outage_id", "captured_at", "start_time"])
    history = history.sort_values(["episode_id", "captured_at"], kind="stable")
    if history.empty:
        return result

    bundle = load_production_bundle(artifact_dir)
    from ml.three_model_predict import predict_initial, predict_live_from_history

    first = history.groupby("episode_id", sort=False).head(1).copy().reset_index(drop=True)
    initial = predict_initial(first, bundle=bundle)
    for name in ("initial_no_eta", "initial_first_eta"):
        duration_col = f"{name}_predicted_duration_hours"
        restore_col = f"{name}_predicted_restore_time"
        initial[restore_col] = initial["start_time"] + pd.to_timedelta(initial[duration_col], unit="h")
    initial["first_hydro_eta"] = initial.get("estimated_restore")

    live = predict_live_from_history(history, bundle=bundle).rename(
        columns={
            "predicted_remaining_hours": "live_predicted_remaining_hours",
            "predicted_restore_time": "live_predicted_restore_time",
            "probability_remaining_over_1h": "live_probability_remaining_over_1h",
            "probability_remaining_over_2h": "live_probability_remaining_over_2h",
            "probability_remaining_over_4h": "live_probability_remaining_over_4h",
            "probability_remaining_over_8h": "live_probability_remaining_over_8h",
        }
    )

    initial_cols = [
        "outage_id",
        "episode_id",
        "first_hydro_eta",
        "initial_no_eta_predicted_duration_hours",
        "initial_no_eta_predicted_restore_time",
        "initial_no_eta_probability_over_2h",
        "initial_no_eta_probability_over_4h",
        "initial_no_eta_probability_over_8h",
        "initial_first_eta_predicted_duration_hours",
        "initial_first_eta_predicted_restore_time",
        "initial_first_eta_probability_over_2h",
        "initial_first_eta_probability_over_4h",
        "initial_first_eta_probability_over_8h",
    ]
    initial = initial[[c for c in initial_cols if c in initial.columns]]

    live_cols = [
        "outage_id",
        "episode_id",
        "captured_at",
        "latest_estimated_restore",
        "live_prediction_available",
        "live_predicted_remaining_hours",
        "live_predicted_restore_time",
        "live_probability_remaining_over_1h",
        "live_probability_remaining_over_2h",
        "live_probability_remaining_over_4h",
        "live_probability_remaining_over_8h",
    ]
    live = live[[c for c in live_cols if c in live.columns]]

    predictions = initial.merge(
        live,
        on=["outage_id", "episode_id"],
        how="left",
        validate="one_to_one",
    )
    status = production_model_status(artifact_dir)
    predictions["ml_release_id"] = status.get("release_id")
    predictions["ml_model_version"] = status.get("model_version")
    predictions["ml_data_as_of"] = predictions.get("captured_at")

    latest_eta = _utc(predictions.get("latest_estimated_restore", pd.Series(index=predictions.index, dtype="object")))
    live_restore = _utc(predictions.get("live_predicted_restore_time", pd.Series(index=predictions.index, dtype="object")))
    predictions["live_vs_hydro_minutes"] = (
        (live_restore - latest_eta).dt.total_seconds() / 60.0
    )
    predictions["live_vs_hydro_absolute_gap_minutes"] = predictions["live_vs_hydro_minutes"].abs()

    # Merge on outage_id because active contains exactly one row per active outage.
    result = result.merge(
        predictions.drop(columns=["episode_id"], errors="ignore"),
        on="outage_id",
        how="left",
        suffixes=("", "_ml"),
        validate="one_to_one",
    )
    return result
