"""Train/evaluate a cadence-aware XGBoost AFT v3 candidate model.

This script is intentionally separate from the production AFT v2 model. It is
safe to rerun later on a larger raw snapshot export. By default it:

1. restricts training to the stable ~20-minute collection regime;
2. keeps only narrow completed censoring intervals (plus right-censored rows);
3. uses a purged chronological train/validation/test split;
4. selects hyperparameters using validation AFT negative log-likelihood only;
5. evaluates the selected configuration exactly once on the untouched test set;
6. saves a *candidate* artifact trained on train+validation, leaving test rows
   out of the saved candidate model.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

from ml.hydro_benchmark import hydro_benchmark, hydro_evaluation_rows
from ml.features import AUGMENTED_FEATURE_NAMES, build_feature_frame
from ml.survival_predict import load_survival_model, predict_survival
from ml.high_frequency_survival import (
    DEFAULT_MAX_INTERVAL_MINUTES,
    DEFAULT_REGIME_START,
    cadence_summary,
    known_threshold_labels,
    purged_elapsed_time_split,
    select_high_frequency_cohort,
)

MODEL_VERSION = "xgb-aft-duration-v3-candidate"
AFT_DISTRIBUTION = "normal"
DEFAULT_SCALES = (0.3, 0.5, 0.7, 1.0)
DEFAULT_DEPTHS = (3, 4, 5, 6)
DEFAULT_MIN_CHILD_WEIGHTS = (3, 5, 10)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_dmatrix(frame: pd.DataFrame, *, with_labels: bool = True) -> xgb.DMatrix:
    features = build_feature_frame(frame, include_hydro_eta=True)
    matrix = xgb.DMatrix(features, feature_names=list(features.columns))
    if with_labels:
        matrix.set_float_info(
            "label_lower_bound", frame["duration_lower_hours"].to_numpy(dtype=float)
        )
        matrix.set_float_info(
            "label_upper_bound", frame["duration_upper_hours"].to_numpy(dtype=float)
        )
    return matrix


def aft_params(*, scale: float, max_depth: int, min_child_weight: float) -> dict:
    return {
        "objective": "survival:aft",
        "eval_metric": "aft-nloglik",
        "aft_loss_distribution": AFT_DISTRIBUTION,
        "aft_loss_distribution_scale": float(scale),
        "tree_method": "hist",
        "learning_rate": 0.03,
        "max_depth": int(max_depth),
        "min_child_weight": float(min_child_weight),
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "reg_lambda": 2.0,
        "reg_alpha": 0.05,
        "seed": 42,
        "nthread": 4,
    }


def _normal_cdf(values: np.ndarray) -> np.ndarray:
    erf = np.vectorize(math.erf)
    return 0.5 * (1.0 + erf(values / math.sqrt(2.0)))


def survival_probability(hours: float, median: np.ndarray, *, scale: float) -> np.ndarray:
    z = (np.log(hours) - np.log(np.clip(median, 1e-9, None))) / float(scale)
    return np.clip(1.0 - _normal_cdf(z), 0.0, 1.0)


def point_metrics(test: pd.DataFrame, prediction: np.ndarray) -> dict[str, float]:
    finite = np.isfinite(test["duration_upper_hours"].to_numpy(dtype=float))
    lower = test.loc[finite, "duration_lower_hours"].to_numpy(dtype=float)
    upper = test.loc[finite, "duration_upper_hours"].to_numpy(dtype=float)
    midpoint = (lower + upper) / 2.0
    pred = prediction[finite]
    error = np.abs(pred - midpoint)
    return {
        "finite_test_rows": int(finite.sum()),
        "right_censored_test_rows": int((~finite).sum()),
        "midpoint_mae_hours": float(error.mean()),
        "midpoint_median_absolute_error_hours": float(np.median(error)),
        "within_1h_rate": float((error <= 1.0).mean()),
        "within_2h_rate": float((error <= 2.0).mean()),
        "within_4h_rate": float((error <= 4.0).mean()),
        "prediction_inside_observed_interval_rate": float(
            ((pred >= lower) & (pred <= upper)).mean()
        ),
    }


def threshold_metrics(
    test: pd.DataFrame, prediction: np.ndarray, *, scale: float
) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    for threshold in (2.0, 4.0, 8.0):
        probabilities = survival_probability(threshold, prediction, scale=scale)
        known, truth = known_threshold_labels(test, threshold)
        output[f"over_{int(threshold)}h"] = {
            "known_test_rows": int(known.sum()),
            "mean_predicted_probability_all_test_rows": float(probabilities.mean()),
            "observed_rate_known_rows": float(truth.mean()) if len(truth) else None,
            "brier_score_known_labels": float(
                np.mean((probabilities[known] - truth) ** 2)
            )
            if len(truth)
            else None,
        }
    return output



def frozen_v2_benchmark(test: pd.DataFrame) -> dict | None:
    """Evaluate the frozen production v2 on the exact same v3 test rows."""
    try:
        bundle = load_survival_model()
        result = predict_survival(test, bundle=bundle)
    except (FileNotFoundError, KeyError, ValueError):
        return None

    prediction = result["predicted_duration_hours"].to_numpy(dtype=float)
    output = {
        "model_version": bundle.metadata.get("model_version"),
        "survival_model": point_metrics(test, prediction),
        "threshold_probabilities": {},
    }
    lower = test["duration_lower_hours"].to_numpy(dtype=float)
    upper = test["duration_upper_hours"].to_numpy(dtype=float)
    for threshold in (2, 4, 8):
        probabilities = result[f"probability_over_{threshold}h"].to_numpy(dtype=float)
        known = (lower > threshold) | (upper <= threshold)
        truth = (lower[known] > threshold).astype(float)
        output["threshold_probabilities"][f"over_{threshold}h"] = {
            "known_test_rows": int(known.sum()),
            "mean_predicted_probability_all_test_rows": float(probabilities.mean()),
            "observed_rate_known_rows": float(truth.mean()) if len(truth) else None,
            "brier_score_known_labels": float(
                np.mean((probabilities[known] - truth) ** 2)
            ) if len(truth) else None,
        }
    return output

def median_baseline_metrics(train: pd.DataFrame, test: pd.DataFrame) -> dict[str, float]:
    train_finite = np.isfinite(train["duration_upper_hours"].to_numpy(dtype=float))
    train_midpoint = (
        train.loc[train_finite, "duration_lower_hours"].to_numpy(dtype=float)
        + train.loc[train_finite, "duration_upper_hours"].to_numpy(dtype=float)
    ) / 2.0
    test_finite = np.isfinite(test["duration_upper_hours"].to_numpy(dtype=float))
    test_midpoint = (
        test.loc[test_finite, "duration_lower_hours"].to_numpy(dtype=float)
        + test.loc[test_finite, "duration_upper_hours"].to_numpy(dtype=float)
    ) / 2.0
    median = float(np.median(train_midpoint))
    error = np.abs(test_midpoint - median)
    return {
        "median_hours": median,
        "midpoint_mae_hours": float(error.mean()),
        "midpoint_median_absolute_error_hours": float(np.median(error)),
    }


def tune_on_validation(train: pd.DataFrame, validation: pd.DataFrame):
    dtrain = make_dmatrix(train)
    dvalidation = make_dmatrix(validation)
    search_rows = []
    best = None

    for scale, depth, min_child in itertools.product(
        DEFAULT_SCALES, DEFAULT_DEPTHS, DEFAULT_MIN_CHILD_WEIGHTS
    ):
        model = xgb.train(
            aft_params(
                scale=scale,
                max_depth=depth,
                min_child_weight=min_child,
            ),
            dtrain,
            num_boost_round=900,
            evals=[(dvalidation, "validation")],
            early_stopping_rounds=60,
            verbose_eval=False,
        )
        row = {
            "validation_aft_nloglik": float(model.best_score),
            "scale": float(scale),
            "max_depth": int(depth),
            "min_child_weight": float(min_child),
            "best_rounds": int(model.best_iteration + 1),
        }
        search_rows.append(row)
        if best is None or row["validation_aft_nloglik"] < best[0]["validation_aft_nloglik"]:
            best = (row, model)

    assert best is not None
    return best[0], best[1], search_rows


def _test_nloglik(model: xgb.Booster, dtest: xgb.DMatrix, best_rounds: int) -> float:
    # Slice to the selected boosting rounds so the test metric cannot influence
    # model selection or early stopping.
    sliced = model[:best_rounds]
    text = sliced.eval(dtest)
    return float(text.rsplit(":", 1)[-1])


def save_evaluation_rows(
    test: pd.DataFrame,
    prediction: np.ndarray,
    *,
    scale: float,
    path: Path,
) -> None:
    columns = [
        "episode_id",
        "outage_id",
        "first_capture",
        "duration_lower_hours",
        "duration_upper_hours",
        "is_right_censored",
        "interval_width_hours",
    ]
    out = test[[c for c in columns if c in test.columns]].copy()
    out["predicted_median_hours"] = prediction
    finite = np.isfinite(out["duration_upper_hours"].to_numpy(dtype=float))
    out["observed_midpoint_hours"] = np.nan
    out.loc[finite, "observed_midpoint_hours"] = (
        out.loc[finite, "duration_lower_hours"].to_numpy(dtype=float)
        + out.loc[finite, "duration_upper_hours"].to_numpy(dtype=float)
    ) / 2.0
    out["absolute_midpoint_error_hours"] = np.abs(
        out["predicted_median_hours"] - out["observed_midpoint_hours"]
    )
    for threshold in (2, 4, 8):
        out[f"probability_over_{threshold}h"] = survival_probability(
            float(threshold), prediction, scale=scale
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path, help="CSV export of raw outage snapshots")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "artifacts" / "v3_candidate",
    )
    parser.add_argument("--regime-start", default=DEFAULT_REGIME_START)
    parser.add_argument(
        "--max-interval-minutes", type=float, default=DEFAULT_MAX_INTERVAL_MINUTES
    )
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument(
        "--refit-all",
        action="store_true",
        help=(
            "After evaluation, refit the saved candidate on train+validation+test. "
            "Default is safer: save a model trained on train+validation only."
        ),
    )
    args = parser.parse_args()

    raw = pd.read_csv(args.dataset, low_memory=False)
    cohort = select_high_frequency_cohort(
        raw,
        regime_start=args.regime_start,
        max_interval_minutes=args.max_interval_minutes,
    )
    split = purged_elapsed_time_split(
        cohort,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
    )

    selected, evaluation_model, search_rows = tune_on_validation(
        split.train, split.validation
    )
    best_rounds = int(selected["best_rounds"])
    scale = float(selected["scale"])

    dtest = make_dmatrix(split.test)
    prediction = evaluation_model.predict(
        dtest, iteration_range=(0, best_rounds)
    )

    metrics = {
        "test_aft_nloglik": _test_nloglik(evaluation_model, dtest, best_rounds),
        "survival_model": point_metrics(split.test, prediction),
        "threshold_probabilities": threshold_metrics(
            split.test, prediction, scale=scale
        ),
        "hydro_quebec_same_test": hydro_benchmark(split.test, prediction),
        "median_baseline": median_baseline_metrics(split.train, split.test),
        "frozen_v2_same_test": frozen_v2_benchmark(split.test),
    }

    # The candidate artifact excludes the untouched test set by default. This
    # lets the same export remain a genuine holdout for qualitative inspection.
    if args.refit_all:
        refit_frame = cohort
        refit_scope = "all_high_frequency_rows_including_test"
    else:
        refit_frame = pd.concat([split.train, split.validation], ignore_index=True)
        refit_scope = "train_plus_validation_only_test_preserved"

    candidate_model = xgb.train(
        aft_params(
            scale=scale,
            max_depth=int(selected["max_depth"]),
            min_child_weight=float(selected["min_child_weight"]),
        ),
        make_dmatrix(refit_frame),
        num_boost_round=best_rounds,
        verbose_eval=False,
    )

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    model_name = "duration_xgb_aft_v3_candidate.json"
    model_path = args.artifact_dir / model_name
    candidate_model.save_model(model_path)

    predictions_name = "aft_v3_test_predictions.csv"
    save_evaluation_rows(
        split.test,
        prediction,
        scale=scale,
        path=args.artifact_dir / predictions_name,
    )

    hydro_comparison_name = "aft_v3_hydro_comparison.csv"
    hydro_evaluation_rows(split.test, prediction).to_csv(
        args.artifact_dir / hydro_comparison_name, index=False
    )

    search_name = "aft_v3_validation_search.csv"
    pd.DataFrame(search_rows).sort_values("validation_aft_nloglik").to_csv(
        args.artifact_dir / search_name, index=False
    )

    metadata = {
        "model_version": MODEL_VERSION,
        "status": "candidate_not_deployed",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "filename": args.dataset.name,
            "sha256": sha256_file(args.dataset),
            "raw_rows": int(len(raw)),
            "unique_outages": int(raw["outage_id"].nunique()),
            "high_frequency_episodes": int(len(cohort)),
            "high_frequency_completed_episodes": int((~cohort["is_right_censored"]).sum()),
            "high_frequency_right_censored_episodes": int(cohort["is_right_censored"].sum()),
            "first_capture_min": cohort["first_capture"].min().isoformat(),
            "first_capture_max": cohort["first_capture"].max().isoformat(),
            "regime_start": pd.Timestamp(args.regime_start).isoformat(),
            "max_completed_interval_minutes": float(args.max_interval_minutes),
            "cadence": cadence_summary(raw, regime_start=args.regime_start),
        },
        "split": {
            "method": "purged_elapsed_wall_clock_time",
            "train_fraction": float(args.train_fraction),
            "validation_fraction": float(args.validation_fraction),
            "test_fraction": float(1 - args.train_fraction - args.validation_fraction),
            "validation_start": split.validation_start.isoformat(),
            "test_start": split.test_start.isoformat(),
            "train_rows": int(len(split.train)),
            "validation_rows": int(len(split.validation)),
            "test_rows": int(len(split.test)),
            "purged_train_rows": int(split.purged_train_rows),
            "purged_validation_rows": int(split.purged_validation_rows),
        },
        "features": {
            "production_features": list(AUGMENTED_FEATURE_NAMES),
            "uses_hydro_estimated_restore": True,
            "future_information_used_as_feature": False,
            "initial_snapshot_nulls_preserved": True,
        },
        "selection": {
            "metric": "validation aft-nloglik",
            "selected": selected,
            "grid_size": int(len(search_rows)),
            "test_used_for_model_selection": False,
        },
        "aft": {
            "distribution": AFT_DISTRIBUTION,
            "scale": scale,
            "best_rounds": best_rounds,
            "max_depth": int(selected["max_depth"]),
            "min_child_weight": float(selected["min_child_weight"]),
        },
        "metrics": metrics,
        "saved_candidate": {
            "refit_scope": refit_scope,
            "rows": int(len(refit_frame)),
            "model": model_name,
            "test_predictions": predictions_name,
            "validation_search": search_name,
            "hydro_comparison": hydro_comparison_name,
        },
        "limitations": [
            "This v3 candidate is trained only on the stable high-frequency collection regime.",
            "The current high-frequency cohort spans only a few days, so weather/event diversity is still limited.",
            "Completed labels remain interval-censored even though the intervals are much narrower than in the historical dataset.",
            "Hydro's initial estimated restoration time is a model feature and must remain separately identified as an official source.",
            "Do not promote v3 solely because of one small test period; rerun the same script after more high-frequency data accumulate.",
        ],
    }
    metadata_path = args.artifact_dir / "duration_survival_v3_metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(json.dumps({
        "cohort": {
            "episodes": metadata["dataset"]["high_frequency_episodes"],
            "completed": metadata["dataset"]["high_frequency_completed_episodes"],
            "right_censored": metadata["dataset"]["high_frequency_right_censored_episodes"],
            "median_snapshot_gap_minutes": metadata["dataset"]["cadence"]["median_gap_minutes"],
        },
        "split": metadata["split"],
        "selected": selected,
        "metrics": metrics,
        "saved_candidate": metadata["saved_candidate"],
    }, indent=2))
    print(f"Saved AFT v3 candidate metadata: {metadata_path}")


if __name__ == "__main__":
    main()
