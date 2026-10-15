"""Shared leakage-safe helpers for the three-model high-frequency AFT workflow.

The workflow intentionally separates three questions:

* ``initial_no_eta``: total outage duration from the first observed snapshot,
  without using Hydro-Quebec's ETA.
* ``initial_first_eta``: total outage duration from the first observed snapshot,
  using only the ETA visible on that first snapshot.
* ``live_latest_eta``: remaining outage duration at each observed snapshot,
  using the latest ETA that had actually been observed by that moment.

No feature in this module is backfilled from a future snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

import numpy as np
import pandas as pd
import xgboost as xgb

from ml.dataset import _parse_datetime_columns
from ml.features import build_feature_frame
from ml.high_frequency_survival import (
    DEFAULT_MAX_INTERVAL_MINUTES,
    DEFAULT_REGIME_START,
    TemporalSplit,
    purged_elapsed_time_split,
    select_high_frequency_cohort,
)
from ml.observed_history import annotate_observed_history

MIN_AFT_HOURS = 1e-6


@dataclass(frozen=True)
class SharedEpisodeSplit:
    cohort: pd.DataFrame
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    validation_start: pd.Timestamp
    test_start: pd.Timestamp
    purged_train_rows: int
    purged_validation_rows: int
    excluded_cross_partition_outages: int
    excluded_cross_partition_episodes: int


def build_shared_episode_split(
    raw: pd.DataFrame,
    *,
    regime_start: str | pd.Timestamp = DEFAULT_REGIME_START,
    max_interval_minutes: float = DEFAULT_MAX_INTERVAL_MINUTES,
    train_fraction: float = 0.60,
    validation_fraction: float = 0.20,
) -> SharedEpisodeSplit:
    """Create one temporal episode split shared by all three models.

    The base split is the existing purged elapsed-wall-clock split.  We then
    remove every ``outage_id`` that would appear in more than one partition.
    This is stricter than merely keeping ``episode_id`` disjoint and prevents a
    reappearing source outage from leaking information across partitions.
    """
    cohort = select_high_frequency_cohort(
        raw,
        regime_start=regime_start,
        max_interval_minutes=max_interval_minutes,
    )
    base = purged_elapsed_time_split(
        cohort,
        train_fraction=train_fraction,
        validation_fraction=validation_fraction,
    )

    assignments = pd.concat(
        [
            base.train[["episode_id", "outage_id"]].assign(split="train"),
            base.validation[["episode_id", "outage_id"]].assign(split="validation"),
            base.test[["episode_id", "outage_id"]].assign(split="test"),
        ],
        ignore_index=True,
    )
    partition_count = assignments.groupby("outage_id")["split"].nunique()
    conflicts = set(partition_count[partition_count > 1].index.astype(str))

    def filtered(frame: pd.DataFrame) -> pd.DataFrame:
        if not conflicts:
            return frame.copy().reset_index(drop=True)
        return frame.loc[~frame["outage_id"].astype(str).isin(conflicts)].copy().reset_index(drop=True)

    train = filtered(base.train)
    validation = filtered(base.validation)
    test = filtered(base.test)
    if min(len(train), len(validation), len(test)) == 0:
        raise ValueError("Shared split became empty after outage-level leakage purge.")

    kept_ids = set(pd.concat([train, validation, test])["episode_id"].astype(str))
    kept_cohort = cohort.loc[cohort["episode_id"].astype(str).isin(kept_ids)].copy()

    return SharedEpisodeSplit(
        cohort=kept_cohort.reset_index(drop=True),
        train=train,
        validation=validation,
        test=test,
        validation_start=base.validation_start,
        test_start=base.test_start,
        purged_train_rows=base.purged_train_rows,
        purged_validation_rows=base.purged_validation_rows,
        excluded_cross_partition_outages=len(conflicts),
        excluded_cross_partition_episodes=int(len(assignments) - len(pd.concat([train, validation, test]))),
    )


def _reconstruct_snapshot_episodes(raw: pd.DataFrame) -> pd.DataFrame:
    """Recreate the same continuous ``episode_id`` used by survival_dataset."""
    df = _parse_datetime_columns(raw)
    df = df.dropna(subset=["outage_id", "snapshot_id", "captured_at", "start_time"]).copy()
    if df.duplicated(["outage_id", "snapshot_id"]).any():
        raise ValueError("Duplicate (outage_id, snapshot_id) rows; resolve duplicates first.")

    snapshot_times = df.groupby("snapshot_id")["captured_at"].min().sort_values()
    snapshot_order = pd.Series(range(len(snapshot_times)), index=snapshot_times.index)
    df["snapshot_ord"] = df["snapshot_id"].map(snapshot_order).astype(int)
    df = df.sort_values(["outage_id", "snapshot_ord", "captured_at"], kind="stable")
    gap = df.groupby("outage_id")["snapshot_ord"].diff()
    episode_no = gap.fillna(1).gt(1).groupby(df["outage_id"]).cumsum().astype(int)
    df["episode_id"] = df["outage_id"].astype(str) + "::" + episode_no.astype(str)
    return df


def annotate_latest_eta_history(df: pd.DataFrame) -> pd.DataFrame:
    """Add latest-known ETA features using past-to-future information only.

    ``df`` must already contain causal history columns from
    :func:`annotate_observed_history` and be ordered or orderable by episode/time.
    """
    out = df.sort_values(["episode_id", "captured_at"], kind="stable").copy()
    group = out.groupby("episode_id", sort=False)
    out["latest_estimated_restore"] = group["estimated_restore"].ffill()
    out["has_latest_hydro_eta"] = out["latest_estimated_restore"].notna()
    out["latest_hydro_remaining_hours"] = (
        out["latest_estimated_restore"] - out["captured_at"]
    ).dt.total_seconds() / 3600.0
    out["latest_hydro_eta_overdue"] = (
        out["has_latest_hydro_eta"]
        & (out["latest_estimated_restore"] < out["captured_at"])
    )

    current_eta = out["estimated_restore"]
    previous_known_eta = group["estimated_restore"].ffill().groupby(out["episode_id"]).shift()
    eta_value_changed = current_eta.notna() & (
        previous_known_eta.isna() | current_eta.ne(previous_known_eta)
    )
    eta_change_capture = out["captured_at"].where(eta_value_changed)
    out["latest_eta_observed_at"] = eta_change_capture.groupby(out["episode_id"]).ffill()
    out["latest_eta_age_hours"] = (
        out["captured_at"] - out["latest_eta_observed_at"]
    ).dt.total_seconds() / 3600.0
    return out


def build_live_latest_eta_landmarks(
    raw: pd.DataFrame,
    split: SharedEpisodeSplit,
) -> tuple[pd.DataFrame, dict]:
    """Build causal live landmarks with the latest ETA known at each snapshot.

    The target is *remaining* duration from ``captured_at``.  ``estimated_restore``
    is never backfilled from the future: ``latest_estimated_restore`` is a
    group-wise forward fill within the same continuous episode.
    """
    assignments = pd.concat(
        [
            split.train[["episode_id", "outage_id"]].assign(split="train"),
            split.validation[["episode_id", "outage_id"]].assign(split="validation"),
            split.test[["episode_id", "outage_id"]].assign(split="test"),
        ],
        ignore_index=True,
    )
    labels = split.cohort[
        [
            "episode_id",
            "first_capture",
            "last_capture",
            "next_snapshot",
            "is_right_censored",
            "interval_width_hours",
        ]
    ].copy()

    df = _reconstruct_snapshot_episodes(raw)
    df = df.merge(assignments[["episode_id", "split"]], on="episode_id", validate="many_to_one")
    df = df.merge(labels, on="episode_id", validate="many_to_one")
    df = df.sort_values(["episode_id", "captured_at"], kind="stable").reset_index(drop=True)

    # Existing history features use only current and past rows.
    df = annotate_observed_history(df)
    # Crucial live semantics: carry the most recently observed ETA forward, never
    # backward. A future revision remains invisible until its own snapshot.
    df = annotate_latest_eta_history(df)

    lower = (df["last_capture"] - df["captured_at"]).dt.total_seconds() / 3600.0
    upper = (df["next_snapshot"] - df["captured_at"]).dt.total_seconds() / 3600.0
    upper = upper.where(~df["is_right_censored"], np.inf)
    if (lower < -1e-9).any() or ((upper < lower) & np.isfinite(upper)).any():
        raise ValueError("Invalid live remaining-time censoring bounds.")

    df["remaining_lower_hours"] = lower.clip(lower=MIN_AFT_HOURS)
    finite_upper = np.isfinite(upper)
    df["remaining_upper_hours"] = np.where(
        finite_upper,
        np.maximum(upper.to_numpy(dtype=float), MIN_AFT_HOURS),
        np.inf,
    )
    df["remaining_midpoint_hours"] = np.where(
        finite_upper,
        (lower.to_numpy(dtype=float) + upper.to_numpy(dtype=float)) / 2.0,
        np.nan,
    )

    # Model C is explicitly the model "with the latest ETA available".  Before
    # any ETA has been observed, this model is not applicable.
    eligible = df.loc[df["has_latest_hydro_eta"]].copy().reset_index(drop=True)
    if eligible.empty:
        raise ValueError("No live landmark has a causally available Hydro ETA.")

    if eligible.groupby("outage_id")["split"].nunique().max() != 1:
        raise AssertionError("An outage appears in more than one live partition.")

    report = {
        "target": "remaining_duration_from_current_capture",
        "eta_semantics": "latest_observed_eta_forward_filled_within_episode_only",
        "future_eta_backfill": False,
        "rows_before_eta_filter": int(len(df)),
        "rows_with_latest_eta": int(len(eligible)),
        "episodes_with_latest_eta": int(eligible["episode_id"].nunique()),
        "partitions": {},
    }
    for name in ("train", "validation", "test"):
        part = eligible.loc[eligible["split"].eq(name)]
        if part.empty:
            raise ValueError(f"No eligible live rows in {name}.")
        report["partitions"][name] = {
            "rows": int(len(part)),
            "episodes": int(part["episode_id"].nunique()),
            "completed_rows": int(np.isfinite(part["remaining_upper_hours"]).sum()),
            "right_censored_episodes": int(
                part.loc[part["is_right_censored"], "episode_id"].nunique()
            ),
            "overdue_eta_rows": int(part["latest_hydro_eta_overdue"].sum()),
        }
    return eligible, report


LIVE_HISTORY_FEATURES = (
    "observation_number",
    "elapsed_since_first_capture_hours",
    "customers_change_from_previous",
    "status_change_count",
    "hours_in_current_status",
    "has_working_status_seen",
    "hours_since_first_working_status",
    "has_known_current_status",
    "customers_change_from_initial",
    "customers_fraction_of_initial",
    "customers_fraction_of_peak_so_far",
    "customers_decline_fraction_from_peak",
    "hydro_eta_revision_count",
    "hydro_latest_revision_hours",
    "latest_hydro_eta_overdue",
    "latest_eta_age_hours",
)
LIVE_COUNT_FEATURES = (
    "customers_initial_observed",
    "customers_peak_observed_so_far",
)


def build_live_latest_eta_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Feature matrix for model C, using the causal latest ETA."""
    required = {"latest_estimated_restore", "has_latest_hydro_eta", *LIVE_HISTORY_FEATURES, *LIVE_COUNT_FEATURES}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"Missing live feature columns: {missing}")

    # Reuse the proven base feature engineering, but explicitly substitute the
    # causally-carried latest ETA for the raw current-row ETA.
    current = frame.copy()
    current["estimated_restore"] = current["latest_estimated_restore"]
    features = build_feature_frame(current, include_hydro_eta=True)
    features = features.rename(
        columns={
            "estimated_restore_remaining_h": "latest_estimated_restore_remaining_h",
            "has_estimated_restore": "has_latest_estimated_restore",
        }
    )

    for name in LIVE_HISTORY_FEATURES:
        features[name] = pd.to_numeric(frame[name], errors="coerce").astype(float)
    for name in LIVE_COUNT_FEATURES:
        values = pd.to_numeric(frame[name], errors="coerce").clip(lower=0)
        features[f"log1p_{name}"] = np.log1p(values)
    return features.astype(float)


def initial_feature_frame(frame: pd.DataFrame, *, use_first_eta: bool) -> pd.DataFrame:
    return build_feature_frame(frame, include_hydro_eta=use_first_eta)


def episode_equal_weights(frame: pd.DataFrame) -> np.ndarray:
    """Each episode has total weight one within the supplied subset."""
    counts = frame.groupby("episode_id")["episode_id"].transform("size")
    return (1.0 / counts).to_numpy(dtype=float)


def make_aft_matrix(
    frame: pd.DataFrame,
    *,
    feature_builder: Callable[[pd.DataFrame], pd.DataFrame],
    lower_col: str,
    upper_col: str,
    with_labels: bool = True,
    episode_weighted: bool = False,
) -> xgb.DMatrix:
    features = feature_builder(frame)
    matrix = xgb.DMatrix(features, feature_names=list(features.columns))
    if with_labels:
        matrix.set_float_info("label_lower_bound", frame[lower_col].to_numpy(dtype=float))
        matrix.set_float_info("label_upper_bound", frame[upper_col].to_numpy(dtype=float))
        if episode_weighted:
            matrix.set_weight(episode_equal_weights(frame))
    return matrix


def absolute_interval_distance(pred: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> np.ndarray:
    return np.maximum(np.maximum(lower - pred, pred - upper), 0.0)


def summarize_point_errors(
    frame: pd.DataFrame,
    prediction: np.ndarray,
    *,
    lower_col: str,
    upper_col: str,
    midpoint_col: str | None = None,
    episode_weighted: bool = False,
) -> dict:
    prediction = np.asarray(prediction, dtype=float)
    upper_all = frame[upper_col].to_numpy(dtype=float)
    mask = np.isfinite(upper_all) & np.isfinite(prediction)
    known = frame.loc[mask]
    result = {
        "rows": int(len(frame)),
        "completed_rows": int(mask.sum()),
        "right_censored_rows": int((~np.isfinite(upper_all)).sum()),
    }
    if known.empty:
        return {**result, "midpoint_mae_hours": None}

    lower = known[lower_col].to_numpy(dtype=float)
    upper = known[upper_col].to_numpy(dtype=float)
    if midpoint_col and midpoint_col in known:
        midpoint = known[midpoint_col].to_numpy(dtype=float)
    else:
        midpoint = (lower + upper) / 2.0
    pred = prediction[mask]
    error = np.abs(pred - midpoint)
    distance = absolute_interval_distance(pred, lower, upper)
    weights = episode_equal_weights(known) if episode_weighted else np.ones(len(known))

    return {
        **result,
        "midpoint_mae_hours": float(np.average(error, weights=weights)),
        "midpoint_median_absolute_error_hours": float(np.median(error)),
        "within_1h_rate": float(np.average(error <= 1.0, weights=weights)),
        "within_2h_rate": float(np.average(error <= 2.0, weights=weights)),
        "within_4h_rate": float(np.average(error <= 4.0, weights=weights)),
        "prediction_inside_observed_interval_rate": float(np.average(distance == 0, weights=weights)),
        "mean_distance_to_observed_interval_hours": float(np.average(distance, weights=weights)),
        "episode_weighted": bool(episode_weighted),
    }
