"""Train three leakage-safe high-frequency XGBoost AFT outage models.

Models
------
1. initial_no_eta
   Predict total outage duration at first observation without Hydro ETA.
2. initial_first_eta
   Predict total outage duration at first observation using only the ETA visible
   on that first snapshot.
3. live_latest_eta
   Predict remaining duration at every eligible live snapshot using the latest
   Hydro ETA that had actually been observed by that moment.

All three share the same purged temporal episode partitions. Hyperparameters are
selected on validation only; test is never used for model selection or refit.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import xgboost as xgb

from ml.high_frequency_survival import (
    DEFAULT_MAX_INTERVAL_MINUTES,
    DEFAULT_REGIME_START,
    cadence_summary,
    known_threshold_labels,
)
from ml.three_model_survival import (
    SharedEpisodeSplit,
    build_live_latest_eta_feature_frame,
    build_live_latest_eta_landmarks,
    build_shared_episode_split,
    episode_equal_weights,
    initial_feature_frame,
    make_aft_matrix,
    summarize_point_errors,
)
from ml.train_survival_model_v3 import AFT_DISTRIBUTION, aft_params, survival_probability

MODEL_VERSION = "aft-hf-three-model-v1"
INITIAL_SCALES = (0.3, 0.5, 0.7, 1.0)
INITIAL_DEPTHS = (3, 4, 5, 6)
INITIAL_MIN_CHILD_WEIGHTS = (3, 5, 10)
LIVE_SCALES = (0.5, 0.7, 1.0)
LIVE_DEPTHS = (3, 4, 5)
LIVE_MIN_CHILD_WEIGHTS = (5, 10)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _eval_nloglik(model: xgb.Booster, dtest: xgb.DMatrix, rounds: int) -> float:
    sliced = model[:rounds]
    return float(sliced.eval(dtest).rsplit(":", 1)[-1])


def tune_model(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    *,
    feature_builder: Callable[[pd.DataFrame], pd.DataFrame],
    lower_col: str,
    upper_col: str,
    episode_weighted: bool,
    scales: tuple[float, ...],
    depths: tuple[int, ...],
    min_child_weights: tuple[float, ...],
    max_rounds: int,
    early_stopping_rounds: int,
    progress_name: str,
):
    dtrain = make_aft_matrix(
        train,
        feature_builder=feature_builder,
        lower_col=lower_col,
        upper_col=upper_col,
        episode_weighted=episode_weighted,
    )
    dvalidation = make_aft_matrix(
        validation,
        feature_builder=feature_builder,
        lower_col=lower_col,
        upper_col=upper_col,
        episode_weighted=episode_weighted,
    )
    combinations = list(itertools.product(scales, depths, min_child_weights))
    search_rows = []
    best = None
    for number, (scale, depth, child) in enumerate(combinations, 1):
        model = xgb.train(
            aft_params(scale=scale, max_depth=depth, min_child_weight=child),
            dtrain,
            num_boost_round=max_rounds,
            evals=[(dvalidation, "validation")],
            early_stopping_rounds=early_stopping_rounds,
            verbose_eval=False,
        )
        row = {
            "validation_aft_nloglik": float(model.best_score),
            "scale": float(scale),
            "max_depth": int(depth),
            "min_child_weight": float(child),
            "best_rounds": int(model.best_iteration + 1),
        }
        search_rows.append(row)
        if best is None or row["validation_aft_nloglik"] < best[0]["validation_aft_nloglik"]:
            best = (row, model)
        print(
            f"{progress_name}: configuration {number}/{len(combinations)}, "
            f"validation={row['validation_aft_nloglik']:.5f}",
            flush=True,
        )
    assert best is not None
    selected = best[0]
    evaluation_model = best[1][: int(selected["best_rounds"])]
    return selected, evaluation_model, search_rows


def fit_candidate(
    refit: pd.DataFrame,
    *,
    selected: dict,
    feature_builder: Callable[[pd.DataFrame], pd.DataFrame],
    lower_col: str,
    upper_col: str,
    episode_weighted: bool,
) -> xgb.Booster:
    return xgb.train(
        aft_params(
            scale=float(selected["scale"]),
            max_depth=int(selected["max_depth"]),
            min_child_weight=float(selected["min_child_weight"]),
        ),
        make_aft_matrix(
            refit,
            feature_builder=feature_builder,
            lower_col=lower_col,
            upper_col=upper_col,
            episode_weighted=episode_weighted,
        ),
        num_boost_round=int(selected["best_rounds"]),
        verbose_eval=False,
    )


def initial_threshold_metrics(test: pd.DataFrame, prediction: np.ndarray, *, scale: float) -> dict:
    output = {}
    for threshold in (2.0, 4.0, 8.0):
        prob = survival_probability(threshold, prediction, scale=scale)
        known, truth = known_threshold_labels(test, threshold)
        output[f"over_{int(threshold)}h"] = {
            "known_rows": int(known.sum()),
            "mean_predicted_probability_all_rows": float(prob.mean()),
            "observed_rate_known_rows": float(truth.mean()) if len(truth) else None,
            "brier_score_known_labels": float(np.mean((prob[known] - truth) ** 2)) if len(truth) else None,
        }
    return output


def live_threshold_metrics(test: pd.DataFrame, prediction: np.ndarray, *, scale: float) -> dict:
    lower = test["remaining_lower_hours"].to_numpy(dtype=float)
    upper = test["remaining_upper_hours"].to_numpy(dtype=float)
    output = {}
    for threshold in (1.0, 2.0, 4.0, 8.0):
        prob = survival_probability(threshold, prediction, scale=scale)
        known = (lower > threshold) | (upper <= threshold)
        truth = (lower[known] > threshold).astype(float)
        if not known.any():
            output[f"over_{int(threshold)}h"] = {"known_rows": 0}
            continue
        weights = episode_equal_weights(test.loc[known])
        output[f"over_{int(threshold)}h"] = {
            "known_rows": int(known.sum()),
            "episode_weighted_predicted_probability": float(np.average(prob[known], weights=weights)),
            "episode_weighted_observed_rate": float(np.average(truth, weights=weights)),
            "episode_weighted_brier": float(np.average((prob[known] - truth) ** 2, weights=weights)),
        }
    return output


def median_baseline(train: pd.DataFrame, test: pd.DataFrame) -> tuple[dict, float]:
    finite_train = np.isfinite(train["duration_upper_hours"].to_numpy(dtype=float))
    midpoint_train = (
        train.loc[finite_train, "duration_lower_hours"].to_numpy(dtype=float)
        + train.loc[finite_train, "duration_upper_hours"].to_numpy(dtype=float)
    ) / 2.0
    median = float(np.median(midpoint_train))
    pred = np.full(len(test), median, dtype=float)
    return summarize_point_errors(
        test,
        pred,
        lower_col="duration_lower_hours",
        upper_col="duration_upper_hours",
    ), median


def _paired_metrics(error: np.ndarray, distance: np.ndarray) -> dict:
    return {
        "rows": int(len(error)),
        "midpoint_mae_hours": float(error.mean()),
        "midpoint_median_absolute_error_hours": float(np.median(error)),
        "within_1h_rate": float((error <= 1).mean()),
        "within_2h_rate": float((error <= 2).mean()),
        "within_4h_rate": float((error <= 4).mean()),
        "prediction_inside_observed_interval_rate": float((distance == 0).mean()),
        "mean_distance_to_observed_interval_hours": float(distance.mean()),
    }


def build_initial_prediction_rows(
    test: pd.DataFrame,
    pred_no_eta: np.ndarray,
    pred_first_eta: np.ndarray,
    *,
    scale_no_eta: float,
    scale_first_eta: float,
    baseline_median: float,
) -> tuple[pd.DataFrame, dict]:
    out = test.copy()
    lower = out["duration_lower_hours"].to_numpy(dtype=float)
    upper = out["duration_upper_hours"].to_numpy(dtype=float)
    completed = np.isfinite(upper)
    midpoint = np.where(completed, (lower + upper) / 2.0, np.nan)
    eta = pd.to_datetime(out.get("estimated_restore"), utc=True, errors="coerce", format="mixed")
    start = pd.to_datetime(out["start_time"], utc=True)
    capture = pd.to_datetime(out["first_capture"], utc=True)
    hydro_duration = (eta - start).dt.total_seconds().to_numpy(dtype=float) / 3600.0
    valid_hydro = eta.notna().to_numpy() & np.isfinite(hydro_duration) & (hydro_duration >= 0)
    paired = completed & valid_hydro

    out["observed_midpoint_hours"] = midpoint
    out["initial_no_eta_predicted_duration_hours"] = pred_no_eta
    out["initial_first_eta_predicted_duration_hours"] = pred_first_eta
    out["median_baseline_duration_hours"] = baseline_median
    out["hydro_first_eta"] = eta
    out["hydro_first_eta_predicted_duration_hours"] = hydro_duration
    out["hydro_first_eta_available"] = eta.notna()
    out["hydro_first_eta_valid"] = valid_hydro
    out["hydro_first_eta_already_past_at_capture"] = eta.notna() & (eta < capture)
    out["hydro_comparison_eligible"] = paired

    for model_name, pred, scale in (
        ("initial_no_eta", pred_no_eta, scale_no_eta),
        ("initial_first_eta", pred_first_eta, scale_first_eta),
    ):
        out[f"{model_name}_absolute_error_hours"] = np.where(completed, np.abs(pred - midpoint), np.nan)
        out[f"{model_name}_distance_to_interval_hours"] = np.where(
            completed, np.maximum(np.maximum(lower - pred, pred - upper), 0), np.nan
        )
        for threshold in (2, 4, 8):
            out[f"{model_name}_probability_over_{threshold}h"] = survival_probability(
                float(threshold), pred, scale=scale
            )
    out["hydro_first_eta_absolute_error_hours"] = np.where(
        paired, np.abs(hydro_duration - midpoint), np.nan
    )
    out["hydro_first_eta_distance_to_interval_hours"] = np.where(
        paired,
        np.maximum(np.maximum(lower - hydro_duration, hydro_duration - upper), 0),
        np.nan,
    )

    pair_rows = out.loc[paired]
    comparison = {
        "paired_completed_rows": int(len(pair_rows)),
        "paired_coverage_completed": float(len(pair_rows) / completed.sum()) if completed.sum() else None,
        "hydro_eta_available_test_rows": int(eta.notna().sum()),
        "hydro_eta_available_rate_all_test": float(eta.notna().mean()) if len(out) else None,
        "hydro_eta_already_past_at_capture_rows": int(out["hydro_first_eta_already_past_at_capture"].sum()),
        "models": {},
    }
    if len(pair_rows):
        hydro_error = pair_rows["hydro_first_eta_absolute_error_hours"].to_numpy(dtype=float)
        hydro_distance = pair_rows["hydro_first_eta_distance_to_interval_hours"].to_numpy(dtype=float)
        comparison["hydro_quebec"] = _paired_metrics(hydro_error, hydro_distance)
        for model_name in ("initial_no_eta", "initial_first_eta"):
            model_error = pair_rows[f"{model_name}_absolute_error_hours"].to_numpy(dtype=float)
            model_distance = pair_rows[f"{model_name}_distance_to_interval_hours"].to_numpy(dtype=float)
            ties = np.isclose(model_error, hydro_error, rtol=0, atol=1e-9)
            model_stats = _paired_metrics(model_error, model_distance)
            model_stats["mae_gain_vs_hydro_hours"] = float((hydro_error - model_error).mean())
            model_stats["mae_reduction_vs_hydro_percent"] = float(
                100 * (hydro_error.mean() - model_error.mean()) / hydro_error.mean()
            ) if hydro_error.mean() > 0 else None
            model_stats["model_closer_rate"] = float(((model_error < hydro_error) & ~ties).mean())
            model_stats["hydro_closer_rate"] = float(((hydro_error < model_error) & ~ties).mean())
            model_stats["tie_rate"] = float(ties.mean())
            comparison["models"][model_name] = model_stats
    else:
        comparison["hydro_quebec"] = None
    return out, comparison


def build_live_prediction_rows(
    test: pd.DataFrame,
    prediction: np.ndarray,
    *,
    scale: float,
) -> tuple[pd.DataFrame, dict]:
    out = test.copy()
    lower = out["remaining_lower_hours"].to_numpy(dtype=float)
    upper = out["remaining_upper_hours"].to_numpy(dtype=float)
    completed = np.isfinite(upper)
    midpoint = out["remaining_midpoint_hours"].to_numpy(dtype=float)
    hydro_remaining = out["latest_hydro_remaining_hours"].to_numpy(dtype=float)
    paired = completed & np.isfinite(hydro_remaining) & np.isfinite(prediction)

    out["live_latest_eta_predicted_remaining_hours"] = prediction
    out["live_latest_eta_predicted_restore_time"] = out["captured_at"] + pd.to_timedelta(prediction, unit="h")
    out["hydro_latest_eta_predicted_remaining_hours"] = hydro_remaining
    out["hydro_latest_eta_predicted_restore_time"] = out["latest_estimated_restore"]
    out["hydro_comparison_eligible"] = paired
    out["live_latest_eta_absolute_error_hours"] = np.where(
        completed, np.abs(prediction - midpoint), np.nan
    )
    out["hydro_latest_eta_absolute_error_hours"] = np.where(
        paired, np.abs(hydro_remaining - midpoint), np.nan
    )
    out["live_latest_eta_distance_to_interval_hours"] = np.where(
        completed, np.maximum(np.maximum(lower - prediction, prediction - upper), 0), np.nan
    )
    out["hydro_latest_eta_distance_to_interval_hours"] = np.where(
        paired, np.maximum(np.maximum(lower - hydro_remaining, hydro_remaining - upper), 0), np.nan
    )
    for threshold in (1, 2, 4, 8):
        out[f"live_probability_remaining_over_{threshold}h"] = survival_probability(
            float(threshold), prediction, scale=scale
        )

    common = out.loc[paired].copy()
    comparison = {
        "paired_completed_snapshot_rows": int(len(common)),
        "paired_completed_episodes": int(common["episode_id"].nunique()) if len(common) else 0,
        "overdue_eta_rows": int(out["latest_hydro_eta_overdue"].sum()),
        "row_weighted": None,
        "episode_weighted": None,
    }
    if len(common):
        model_error = common["live_latest_eta_absolute_error_hours"].to_numpy(dtype=float)
        hydro_error = common["hydro_latest_eta_absolute_error_hours"].to_numpy(dtype=float)
        model_distance = common["live_latest_eta_distance_to_interval_hours"].to_numpy(dtype=float)
        hydro_distance = common["hydro_latest_eta_distance_to_interval_hours"].to_numpy(dtype=float)

        def compare(weights: np.ndarray) -> dict:
            ties = np.isclose(model_error, hydro_error, rtol=0, atol=1e-9)
            hq_mae = float(np.average(hydro_error, weights=weights))
            model_mae = float(np.average(model_error, weights=weights))
            return {
                "model_mae_hours": model_mae,
                "hydro_mae_hours": hq_mae,
                "model_mean_distance_to_interval_hours": float(np.average(model_distance, weights=weights)),
                "hydro_mean_distance_to_interval_hours": float(np.average(hydro_distance, weights=weights)),
                "mae_gain_hours": hq_mae - model_mae,
                "mae_reduction_percent": float(100 * (hq_mae - model_mae) / hq_mae) if hq_mae > 0 else None,
                "model_closer_rate": float(np.average((model_error < hydro_error) & ~ties, weights=weights)),
                "hydro_closer_rate": float(np.average((hydro_error < model_error) & ~ties, weights=weights)),
                "tie_rate": float(np.average(ties, weights=weights)),
            }

        comparison["row_weighted"] = compare(np.ones(len(common)))
        comparison["episode_weighted"] = compare(episode_equal_weights(common))
    return out, comparison


def elapsed_bucket_metrics(live_rows: pd.DataFrame) -> list[dict]:
    completed = live_rows.loc[
        live_rows["hydro_comparison_eligible"] & live_rows["remaining_midpoint_hours"].notna()
    ].copy()
    if completed.empty:
        return []
    bins = [-np.inf, 1, 2, 4, 8, np.inf]
    labels = ["0-1h", "1-2h", "2-4h", "4-8h", "8h+"]
    completed["elapsed_bucket"] = pd.cut(
        completed["elapsed_since_first_capture_hours"], bins=bins, labels=labels, right=False
    )
    rows = []
    for label, part in completed.groupby("elapsed_bucket", observed=True):
        if part.empty:
            continue
        weights = episode_equal_weights(part)
        ml = part["live_latest_eta_absolute_error_hours"].to_numpy(dtype=float)
        hq = part["hydro_latest_eta_absolute_error_hours"].to_numpy(dtype=float)
        rows.append({
            "elapsed_bucket": str(label),
            "rows": int(len(part)),
            "episodes": int(part["episode_id"].nunique()),
            "model_episode_weighted_mae_hours": float(np.average(ml, weights=weights)),
            "hydro_episode_weighted_mae_hours": float(np.average(hq, weights=weights)),
            "model_closer_rate": float(np.average(ml < hq, weights=weights)),
        })
    return rows


def train_and_save_variant(
    *,
    name: str,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
    artifact_dir: Path,
    feature_builder: Callable[[pd.DataFrame], pd.DataFrame],
    lower_col: str,
    upper_col: str,
    episode_weighted: bool,
    scales: tuple[float, ...],
    depths: tuple[int, ...],
    child_weights: tuple[float, ...],
    max_rounds: int,
    early_stopping_rounds: int,
) -> tuple[dict, np.ndarray, list[str]]:
    selected, evaluation_model, search = tune_model(
        train,
        validation,
        feature_builder=feature_builder,
        lower_col=lower_col,
        upper_col=upper_col,
        episode_weighted=episode_weighted,
        scales=scales,
        depths=depths,
        min_child_weights=child_weights,
        max_rounds=max_rounds,
        early_stopping_rounds=early_stopping_rounds,
        progress_name=name,
    )
    dtest = make_aft_matrix(
        test,
        feature_builder=feature_builder,
        lower_col=lower_col,
        upper_col=upper_col,
        episode_weighted=episode_weighted,
    )
    prediction = evaluation_model.predict(dtest)
    test_nloglik = _eval_nloglik(evaluation_model, dtest, int(selected["best_rounds"]))

    model_dir = artifact_dir / name
    model_dir.mkdir(parents=True, exist_ok=True)
    evaluation_model.save_model(model_dir / "evaluation_model.json")
    refit = pd.concat([train, validation], ignore_index=True)
    candidate = fit_candidate(
        refit,
        selected=selected,
        feature_builder=feature_builder,
        lower_col=lower_col,
        upper_col=upper_col,
        episode_weighted=episode_weighted,
    )
    candidate.save_model(model_dir / "candidate_model.json")
    pd.DataFrame(search).sort_values("validation_aft_nloglik").to_csv(
        model_dir / "validation_search.csv", index=False
    )
    feature_names = list(feature_builder(train.iloc[: min(len(train), 100)]).columns)
    info = {
        "selection": selected,
        "test_aft_nloglik": test_nloglik,
        "train_rows": int(len(train)),
        "validation_rows": int(len(validation)),
        "test_rows": int(len(test)),
        "candidate_refit_rows": int(len(refit)),
        "evaluation_fit_scope": "train_only_with_validation_for_early_stopping",
        "candidate_fit_scope": "train_plus_validation_only_test_preserved",
        "episode_weighted_fit": bool(episode_weighted),
        "feature_names": feature_names,
        "files": {
            "evaluation_model": f"{name}/evaluation_model.json",
            "candidate_model": f"{name}/candidate_model.json",
            "validation_search": f"{name}/validation_search.csv",
        },
    }
    return info, prediction, feature_names


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "artifacts" / "three_model_candidate",
    )
    parser.add_argument("--regime-start", default=DEFAULT_REGIME_START)
    parser.add_argument("--max-interval-minutes", type=float, default=DEFAULT_MAX_INTERVAL_MINUTES)
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--initial-max-rounds", type=int, default=900)
    parser.add_argument("--live-max-rounds", type=int, default=700)
    parser.add_argument("--smoke-test", action="store_true", help="Developer check: tiny grid/round count; never use for a release.")
    args = parser.parse_args()

    raw = pd.read_csv(args.dataset, low_memory=False)
    initial_scales = (0.7,) if args.smoke_test else INITIAL_SCALES
    initial_depths = (3,) if args.smoke_test else INITIAL_DEPTHS
    initial_children = (5,) if args.smoke_test else INITIAL_MIN_CHILD_WEIGHTS
    live_scales = (0.7,) if args.smoke_test else LIVE_SCALES
    live_depths = (3,) if args.smoke_test else LIVE_DEPTHS
    live_children = (5,) if args.smoke_test else LIVE_MIN_CHILD_WEIGHTS
    initial_max_rounds = min(args.initial_max_rounds, 80) if args.smoke_test else args.initial_max_rounds
    live_max_rounds = min(args.live_max_rounds, 80) if args.smoke_test else args.live_max_rounds
    split = build_shared_episode_split(
        raw,
        regime_start=args.regime_start,
        max_interval_minutes=args.max_interval_minutes,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
    )
    live, live_report = build_live_latest_eta_landmarks(raw, split)
    live_train = live.loc[live["split"].eq("train")].reset_index(drop=True)
    live_validation = live.loc[live["split"].eq("validation")].reset_index(drop=True)
    live_test = live.loc[live["split"].eq("test")].reset_index(drop=True)

    args.artifact_dir.mkdir(parents=True, exist_ok=True)

    no_eta_builder = lambda frame: initial_feature_frame(frame, use_first_eta=False)
    first_eta_builder = lambda frame: initial_feature_frame(frame, use_first_eta=True)

    no_eta_info, no_eta_pred, _ = train_and_save_variant(
        name="initial_no_eta",
        train=split.train,
        validation=split.validation,
        test=split.test,
        artifact_dir=args.artifact_dir,
        feature_builder=no_eta_builder,
        lower_col="duration_lower_hours",
        upper_col="duration_upper_hours",
        episode_weighted=False,
        scales=initial_scales,
        depths=initial_depths,
        child_weights=initial_children,
        max_rounds=initial_max_rounds,
        early_stopping_rounds=60,
    )
    first_eta_info, first_eta_pred, _ = train_and_save_variant(
        name="initial_first_eta",
        train=split.train,
        validation=split.validation,
        test=split.test,
        artifact_dir=args.artifact_dir,
        feature_builder=first_eta_builder,
        lower_col="duration_lower_hours",
        upper_col="duration_upper_hours",
        episode_weighted=False,
        scales=initial_scales,
        depths=initial_depths,
        child_weights=initial_children,
        max_rounds=initial_max_rounds,
        early_stopping_rounds=60,
    )
    live_info, live_pred, _ = train_and_save_variant(
        name="live_latest_eta",
        train=live_train,
        validation=live_validation,
        test=live_test,
        artifact_dir=args.artifact_dir,
        feature_builder=build_live_latest_eta_feature_frame,
        lower_col="remaining_lower_hours",
        upper_col="remaining_upper_hours",
        episode_weighted=True,
        scales=live_scales,
        depths=live_depths,
        child_weights=live_children,
        max_rounds=live_max_rounds,
        early_stopping_rounds=50,
    )

    baseline_metrics, baseline_median = median_baseline(split.train, split.test)
    initial_rows, initial_hydro = build_initial_prediction_rows(
        split.test,
        no_eta_pred,
        first_eta_pred,
        scale_no_eta=float(no_eta_info["selection"]["scale"]),
        scale_first_eta=float(first_eta_info["selection"]["scale"]),
        baseline_median=baseline_median,
    )
    live_rows, live_hydro = build_live_prediction_rows(
        live_test,
        live_pred,
        scale=float(live_info["selection"]["scale"]),
    )

    no_eta_metrics = summarize_point_errors(
        split.test,
        no_eta_pred,
        lower_col="duration_lower_hours",
        upper_col="duration_upper_hours",
    )
    first_eta_metrics = summarize_point_errors(
        split.test,
        first_eta_pred,
        lower_col="duration_lower_hours",
        upper_col="duration_upper_hours",
    )
    live_row_metrics = summarize_point_errors(
        live_test,
        live_pred,
        lower_col="remaining_lower_hours",
        upper_col="remaining_upper_hours",
        midpoint_col="remaining_midpoint_hours",
        episode_weighted=False,
    )
    live_episode_metrics = summarize_point_errors(
        live_test,
        live_pred,
        lower_col="remaining_lower_hours",
        upper_col="remaining_upper_hours",
        midpoint_col="remaining_midpoint_hours",
        episode_weighted=True,
    )

    initial_rows.to_csv(args.artifact_dir / "initial_test_predictions.csv", index=False)
    live_rows.to_csv(args.artifact_dir / "live_test_predictions.csv", index=False)
    elapsed_rows = elapsed_bucket_metrics(live_rows)
    pd.DataFrame(elapsed_rows).to_csv(args.artifact_dir / "live_elapsed_bucket_metrics.csv", index=False)

    metadata = {
        "model_version": MODEL_VERSION,
        "status": "candidate_not_deployed",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "filename": args.dataset.name,
            "sha256": sha256_file(args.dataset),
            "raw_rows": int(len(raw)),
            "unique_outages": int(raw["outage_id"].nunique()),
            "regime_start": pd.Timestamp(args.regime_start).isoformat(),
            "max_completed_interval_minutes": float(args.max_interval_minutes),
            "cadence": cadence_summary(raw, regime_start=args.regime_start),
        },
        "shared_split": {
            "method": "purged_elapsed_wall_clock_plus_outage_id_cross_partition_purge",
            "validation_start": split.validation_start.isoformat(),
            "test_start": split.test_start.isoformat(),
            "train_episodes": int(len(split.train)),
            "validation_episodes": int(len(split.validation)),
            "test_episodes": int(len(split.test)),
            "purged_train_rows": int(split.purged_train_rows),
            "purged_validation_rows": int(split.purged_validation_rows),
            "excluded_cross_partition_outages": int(split.excluded_cross_partition_outages),
            "excluded_cross_partition_episodes": int(split.excluded_cross_partition_episodes),
        },
        "models": {
            "initial_no_eta": {
                **no_eta_info,
                "prediction_moment": "first_observation",
                "target": "total_duration_since_reported_start",
                "hydro_eta_feature": "none",
                "metrics": no_eta_metrics,
                "threshold_probabilities": initial_threshold_metrics(
                    split.test, no_eta_pred, scale=float(no_eta_info["selection"]["scale"])
                ),
            },
            "initial_first_eta": {
                **first_eta_info,
                "prediction_moment": "first_observation",
                "target": "total_duration_since_reported_start",
                "hydro_eta_feature": "eta_visible_on_first_snapshot_only",
                "metrics": first_eta_metrics,
                "threshold_probabilities": initial_threshold_metrics(
                    split.test, first_eta_pred, scale=float(first_eta_info["selection"]["scale"])
                ),
            },
            "live_latest_eta": {
                **live_info,
                "prediction_moment": "each_observed_snapshot_after_an_eta_has_been_seen",
                "target": "remaining_duration_from_current_capture",
                "hydro_eta_feature": "latest_eta_observed_by_current_snapshot_forward_fill_only",
                "row_weighted_metrics": live_row_metrics,
                "episode_weighted_metrics": live_episode_metrics,
                "threshold_probabilities": live_threshold_metrics(
                    live_test, live_pred, scale=float(live_info["selection"]["scale"])
                ),
                "elapsed_bucket_metrics": elapsed_rows,
                "landmark_report": live_report,
            },
        },
        "benchmarks": {
            "median_baseline": {**baseline_metrics, "median_hours": baseline_median},
            "initial_hydro_first_eta": initial_hydro,
            "live_hydro_latest_eta": live_hydro,
        },
        "methodology": {
            "test_used_for_model_selection": False,
            "test_used_for_candidate_refit": False,
            "live_episode_weighting": "each episode has total weight 1 within each fit/evaluation subset",
            "live_eta_forward_fill": "within episode, past-to-future only; never backfilled from future",
            "point_target_for_completed_intervals": "interval midpoint for descriptive MAE only; AFT is trained on censoring bounds",
        },
        "files": {
            "initial_test_predictions": "initial_test_predictions.csv",
            "live_test_predictions": "live_test_predictions.csv",
            "live_elapsed_bucket_metrics": "live_elapsed_bucket_metrics.csv",
        },
    }
    metadata_path = args.artifact_dir / "three_model_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    summary = {
        "shared_split": metadata["shared_split"],
        "initial_no_eta": {
            "selected": no_eta_info["selection"],
            "metrics": no_eta_metrics,
        },
        "initial_first_eta": {
            "selected": first_eta_info["selection"],
            "metrics": first_eta_metrics,
        },
        "live_latest_eta": {
            "selected": live_info["selection"],
            "episode_weighted_metrics": live_episode_metrics,
            "row_weighted_metrics": live_row_metrics,
        },
        "benchmarks": metadata["benchmarks"],
        "saved_artifacts": str(args.artifact_dir.resolve()),
    }
    print(json.dumps(summary, indent=2))
    print(f"Saved three-model metadata: {metadata_path}")


if __name__ == "__main__":
    main()
