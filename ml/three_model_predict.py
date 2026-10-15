"""Inference helpers for the three-model high-frequency AFT release."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

from ml.observed_history import annotate_observed_history
from ml.three_model_survival import (
    annotate_latest_eta_history,
    build_live_latest_eta_feature_frame,
    initial_feature_frame,
)
from ml.train_survival_model_v3 import survival_probability

DEFAULT_DIR = Path(__file__).resolve().parent / "artifacts" / "three_model_candidate"


@dataclass(frozen=True)
class ThreeModelBundle:
    models: dict[str, xgb.Booster]
    metadata: dict
    kind: str


def load_three_model_bundle(artifact_dir=DEFAULT_DIR, *, kind="candidate") -> ThreeModelBundle:
    if kind not in {"candidate", "evaluation"}:
        raise ValueError("kind must be 'candidate' or 'evaluation'")
    directory = Path(artifact_dir)
    metadata = json.loads((directory / "three_model_metadata.json").read_text(encoding="utf-8"))
    models = {}
    filename = f"{kind}_model.json"
    for name in ("initial_no_eta", "initial_first_eta", "live_latest_eta"):
        model = xgb.Booster()
        model.load_model(directory / name / filename)
        models[name] = model
    return ThreeModelBundle(models=models, metadata=metadata, kind=kind)


def _predict(model, features, expected_names):
    if list(features.columns) != list(expected_names):
        raise ValueError("Feature schema does not match the model artifact.")
    matrix = xgb.DMatrix(features, feature_names=list(features.columns))
    return np.clip(model.predict(matrix), 1e-6, None)


def predict_initial(frame: pd.DataFrame, *, bundle: ThreeModelBundle | None = None) -> pd.DataFrame:
    """Predict both initial models for one row per first observation."""
    bundle = bundle or load_three_model_bundle()
    out = frame.copy()
    for name, use_eta in (("initial_no_eta", False), ("initial_first_eta", True)):
        features = initial_feature_frame(out, use_first_eta=use_eta)
        meta = bundle.metadata["models"][name]
        pred = _predict(bundle.models[name], features, meta["feature_names"])
        out[f"{name}_predicted_duration_hours"] = pred
        scale = float(meta["selection"]["scale"])
        for threshold in (2, 4, 8):
            out[f"{name}_probability_over_{threshold}h"] = survival_probability(
                float(threshold), pred, scale=scale
            )
    return out


def predict_live_from_history(
    history: pd.DataFrame,
    *,
    as_of=None,
    bundle: ThreeModelBundle | None = None,
) -> pd.DataFrame:
    """Predict remaining time from each episode's latest observed history row.

    ``history`` must contain a continuous ``episode_id`` and only observations
    actually available by ``as_of``. A prior ETA is carried forward causally if
    the current snapshot has a null ETA. Episodes for which no ETA has ever been
    observed yet are returned with no live prediction.
    """
    bundle = bundle or load_three_model_bundle()
    observed = history.copy()
    if as_of is not None:
        cutoff = pd.Timestamp(as_of)
        cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
        captures = pd.to_datetime(observed["captured_at"], utc=True, errors="coerce", format="mixed")
        observed = observed.loc[captures.le(cutoff)].copy()
    if observed.empty:
        raise ValueError("No observed snapshots at the requested time.")

    annotated = annotate_observed_history(observed)
    annotated = annotate_latest_eta_history(annotated)
    current = annotated.groupby("episode_id", sort=False).tail(1).reset_index(drop=True)
    eligible = current["has_latest_hydro_eta"].to_numpy(dtype=bool)

    output_cols = [c for c in ("episode_id", "outage_id", "captured_at", "latest_estimated_restore") if c in current]
    out = current[output_cols].copy()
    out["live_prediction_available"] = eligible
    out["predicted_remaining_hours"] = np.nan
    out["predicted_restore_time"] = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns, UTC]")
    for threshold in (1, 2, 4, 8):
        out[f"probability_remaining_over_{threshold}h"] = np.nan

    if eligible.any():
        features = build_live_latest_eta_feature_frame(current.loc[eligible])
        meta = bundle.metadata["models"]["live_latest_eta"]
        pred = _predict(bundle.models["live_latest_eta"], features, meta["feature_names"])
        out.loc[eligible, "predicted_remaining_hours"] = pred
        out.loc[eligible, "predicted_restore_time"] = (
            current.loc[eligible, "captured_at"] + pd.to_timedelta(pred, unit="h")
        ).to_numpy()
        scale = float(meta["selection"]["scale"])
        for threshold in (1, 2, 4, 8):
            out.loc[eligible, f"probability_remaining_over_{threshold}h"] = survival_probability(
                float(threshold), pred, scale=scale
            )
    return out
