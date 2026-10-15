"""Inference helpers for the experimental AFT v3 candidate artifact."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

from ml.features import build_feature_frame

DEFAULT_ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts" / "v3_candidate"
DEFAULT_METADATA_PATH = DEFAULT_ARTIFACT_DIR / "duration_survival_v3_metadata.json"


@dataclass(frozen=True)
class SurvivalV3Bundle:
    model: xgb.Booster
    metadata: dict


def load_survival_v3_candidate(
    metadata_path: Path = DEFAULT_METADATA_PATH,
) -> SurvivalV3Bundle:
    metadata_path = Path(metadata_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    model = xgb.Booster()
    model.load_model(metadata_path.parent / metadata["saved_candidate"]["model"])
    return SurvivalV3Bundle(model=model, metadata=metadata)


def _normal_cdf(values: np.ndarray) -> np.ndarray:
    erf = np.vectorize(math.erf)
    return 0.5 * (1.0 + erf(values / math.sqrt(2.0)))


def predict_survival_v3(
    frame: pd.DataFrame,
    bundle: SurvivalV3Bundle | None = None,
) -> pd.DataFrame:
    if bundle is None:
        bundle = load_survival_v3_candidate()
    features = build_feature_frame(frame, include_hydro_eta=True)
    matrix = xgb.DMatrix(features, feature_names=list(features.columns))
    median = np.clip(bundle.model.predict(matrix), 1e-6, None)
    scale = float(bundle.metadata["aft"]["scale"])

    result = pd.DataFrame(index=frame.index)
    result["predicted_duration_hours"] = median
    z10 = -1.2815515655446004
    z90 = 1.2815515655446004
    result["prediction_lower_hours"] = median * np.exp(scale * z10)
    result["prediction_upper_hours"] = median * np.exp(scale * z90)
    for threshold in (2, 4, 8):
        z = (np.log(float(threshold)) - np.log(median)) / scale
        result[f"probability_over_{threshold}h"] = np.clip(
            1.0 - _normal_cdf(z), 0.0, 1.0
        )
    return result
