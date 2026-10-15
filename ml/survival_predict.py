"""Load the versioned XGBoost AFT model and generate survival predictions."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

from ml.features import build_feature_frame

ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts"
METADATA_PATH = ARTIFACT_DIR / "duration_survival_metadata.json"


@dataclass(frozen=True)
class SurvivalModelBundle:
    model: xgb.Booster
    metadata: dict


def load_survival_model() -> SurvivalModelBundle:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    model = xgb.Booster()
    model.load_model(ARTIFACT_DIR / metadata["artifacts"]["model"])
    return SurvivalModelBundle(model=model, metadata=metadata)


def _normal_cdf(values: np.ndarray) -> np.ndarray:
    erf = np.vectorize(math.erf)
    return 0.5 * (1.0 + erf(values / math.sqrt(2.0)))


def survival_probability(
    hours: float | np.ndarray,
    predicted_median_hours: np.ndarray,
    *,
    scale: float,
) -> np.ndarray:
    """Return P(T > hours) for the normal AFT distribution."""
    t = np.asarray(hours, dtype=float)
    median = np.clip(np.asarray(predicted_median_hours, dtype=float), 1e-9, None)
    t = np.clip(t, 1e-9, None)
    z = (np.log(t) - np.log(median)) / float(scale)
    return np.clip(1.0 - _normal_cdf(z), 0.0, 1.0)


def predict_survival(
    df: pd.DataFrame,
    bundle: SurvivalModelBundle | None = None,
) -> pd.DataFrame:
    if bundle is None:
        bundle = load_survival_model()

    features = build_feature_frame(df, include_hydro_eta=True)
    dmatrix = xgb.DMatrix(features, feature_names=list(features.columns))
    median = np.clip(bundle.model.predict(dmatrix), 1e-6, None)

    scale = float(bundle.metadata["aft"]["scale"])
    # Standard-normal quantiles for 10% and 90%. In the AFT formulation,
    # log(T) = log(median) + scale * Z.
    z10 = -1.2815515655446004
    z90 = 1.2815515655446004
    lower = median * np.exp(scale * z10)
    upper = median * np.exp(scale * z90)

    result = pd.DataFrame(index=df.index)
    result["predicted_duration_hours"] = median
    result["prediction_lower_hours"] = lower
    result["prediction_upper_hours"] = upper
    for threshold in (2, 4, 8):
        result[f"probability_over_{threshold}h"] = survival_probability(
            threshold,
            median,
            scale=scale,
        )
    return result
