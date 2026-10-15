"""Paired comparison against Hydro's ETA available at first observation."""
from __future__ import annotations

import numpy as np
import pandas as pd


def hydro_evaluation_rows(test: pd.DataFrame, prediction: np.ndarray) -> pd.DataFrame:
    prediction = np.asarray(prediction, dtype=float)
    if prediction.shape != (len(test),):
        raise ValueError("One model prediction is required per test episode.")
    out = test.copy()
    eta = pd.to_datetime(
        out.get("estimated_restore", pd.Series(pd.NaT, index=out.index)),
        utc=True, errors="coerce", format="mixed",
    )
    start = pd.to_datetime(out["start_time"], utc=True)
    capture = pd.to_datetime(out["first_capture"], utc=True)
    hq = (eta - start).dt.total_seconds().to_numpy() / 3600.0
    lower = out["duration_lower_hours"].to_numpy(dtype=float)
    upper = out["duration_upper_hours"].to_numpy(dtype=float)
    completed = np.isfinite(upper)
    valid_eta = np.isfinite(hq) & (hq >= 0)
    paired = completed & valid_eta & np.isfinite(prediction)
    midpoint = np.where(completed, (lower + upper) / 2, np.nan)
    out["hydro_eta_at_first_capture"] = eta
    out["hydro_predicted_duration_hours"] = hq
    out["hydro_eta_available"] = eta.notna()
    out["hydro_eta_valid"] = valid_eta
    out["hydro_eta_already_past_at_capture"] = eta.notna() & (eta < capture)
    out["hydro_comparison_eligible"] = paired
    out["model_predicted_duration_hours"] = prediction
    out["observed_midpoint_hours"] = midpoint
    for name, values in (("model", prediction), ("hydro", hq)):
        out[f"{name}_absolute_midpoint_error_hours"] = np.where(
            paired, np.abs(values - midpoint), np.nan
        )
        out[f"{name}_distance_to_observed_interval_hours"] = np.where(
            paired, np.maximum(np.maximum(lower - values, values - upper), 0), np.nan
        )
    return out


def hydro_benchmark(test: pd.DataFrame, prediction: np.ndarray) -> dict:
    rows = hydro_evaluation_rows(test, prediction)
    common = rows.loc[rows["hydro_comparison_eligible"]]
    completed = np.isfinite(rows["duration_upper_hours"].to_numpy(dtype=float))
    result = {
        "prediction_moment": "first_capture",
        "target": "total_duration_since_start_time",
        "model_uses_hydro_eta_as_feature": True,
        "test_rows": len(rows),
        "completed_test_rows": int(completed.sum()),
        "eta_available_test_rows": int(rows["hydro_eta_available"].sum()),
        "eta_available_rate_all_test": float(rows["hydro_eta_available"].mean()) if len(rows) else None,
        "eta_before_start_excluded_rows": int((rows["hydro_eta_available"] & ~rows["hydro_eta_valid"]).sum()),
        "eta_already_past_at_capture_rows": int(rows["hydro_eta_already_past_at_capture"].sum()),
        "paired_completed_rows": len(common),
        "paired_coverage_completed": float(len(common) / completed.sum()) if completed.sum() else None,
        "status": "ok" if len(common) else "no_comparable_completed_episodes",
        "model": None,
        "hydro_quebec": None,
        "paired_comparison": None,
    }
    if common.empty:
        return result
    for name, key in (("model", "model"), ("hydro", "hydro_quebec")):
        error = common[f"{name}_absolute_midpoint_error_hours"].to_numpy()
        distance = common[f"{name}_distance_to_observed_interval_hours"].to_numpy()
        result[key] = {
            "rows": len(common),
            "midpoint_mae_hours": float(error.mean()),
            "midpoint_median_absolute_error_hours": float(np.median(error)),
            "within_1h_rate": float((error <= 1).mean()),
            "within_2h_rate": float((error <= 2).mean()),
            "within_4h_rate": float((error <= 4).mean()),
            "prediction_inside_observed_interval_rate": float((distance == 0).mean()),
            "mean_distance_to_observed_interval_hours": float(distance.mean()),
        }
    model_error = common["model_absolute_midpoint_error_hours"].to_numpy()
    hydro_error = common["hydro_absolute_midpoint_error_hours"].to_numpy()
    ties = np.isclose(model_error, hydro_error, rtol=0, atol=1e-9)
    hq_mae = float(hydro_error.mean())
    result["paired_comparison"] = {
        "mae_gain_hours_positive_is_better": float((hydro_error - model_error).mean()),
        "mae_reduction_percent": float(100 * (hq_mae - model_error.mean()) / hq_mae) if hq_mae > 0 else None,
        "model_closer_rate": float(((model_error < hydro_error) & ~ties).mean()),
        "hydro_closer_rate": float(((hydro_error < model_error) & ~ties).mean()),
        "tie_rate": float(ties.mean()),
    }
    return result
