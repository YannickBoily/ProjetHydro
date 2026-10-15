"""High-frequency cohort helpers for ProjetHydro AFT v3 experiments."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from ml.survival_dataset import build_survival_episode_dataset

DEFAULT_REGIME_START = "2026-09-29T00:20:41Z"
DEFAULT_TARGET_CADENCE_MINUTES = 20.0
DEFAULT_MAX_INTERVAL_MINUTES = 25.0


@dataclass(frozen=True)
class TemporalSplit:
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    validation_start: pd.Timestamp
    test_start: pd.Timestamp
    purged_train_rows: int
    purged_validation_rows: int


def select_high_frequency_cohort(
    raw: pd.DataFrame,
    *,
    regime_start: str | pd.Timestamp = DEFAULT_REGIME_START,
    max_interval_minutes: float = DEFAULT_MAX_INTERVAL_MINUTES,
) -> pd.DataFrame:
    """Build AFT episodes restricted to the stable high-frequency regime.

    Episodes must begin after ``regime_start``. Completed episodes are kept only
    when their censoring interval is no wider than ``max_interval_minutes``.
    Right-censored episodes are retained because AFT can use them directly.
    """
    episodes = build_survival_episode_dataset(raw)
    start = pd.Timestamp(regime_start)
    if start.tzinfo is None:
        start = start.tz_localize("UTC")
    else:
        start = start.tz_convert("UTC")

    max_width_hours = float(max_interval_minutes) / 60.0
    selected = episodes[
        (episodes["first_capture"] >= start)
        & (
            episodes["is_right_censored"]
            | (episodes["interval_width_hours"] <= max_width_hours)
        )
    ].copy()
    return selected.sort_values("first_capture").reset_index(drop=True)


def cadence_summary(raw: pd.DataFrame, *, regime_start: str | pd.Timestamp) -> dict:
    """Summarize represented snapshot cadence after the v3 regime start."""
    frame = raw.copy()
    frame["captured_at"] = pd.to_datetime(
        frame["captured_at"], errors="coerce", utc=True, format="mixed"
    )
    snapshot_times = (
        frame.dropna(subset=["snapshot_id", "captured_at"])
        .groupby("snapshot_id")["captured_at"]
        .min()
        .sort_values()
    )
    start = pd.Timestamp(regime_start)
    if start.tzinfo is None:
        start = start.tz_localize("UTC")
    else:
        start = start.tz_convert("UTC")
    selected = snapshot_times[snapshot_times >= start]
    gaps = selected.diff().dropna().dt.total_seconds() / 60.0
    if gaps.empty:
        return {
            "snapshot_count": int(len(selected)),
            "gap_count": 0,
            "median_gap_minutes": None,
            "p95_gap_minutes": None,
            "share_gaps_le_25m": None,
            "share_gaps_le_45m": None,
        }
    return {
        "snapshot_count": int(len(selected)),
        "gap_count": int(len(gaps)),
        "median_gap_minutes": float(gaps.median()),
        "p95_gap_minutes": float(gaps.quantile(0.95)),
        "share_gaps_le_25m": float((gaps <= 25.0).mean()),
        "share_gaps_le_45m": float((gaps <= 45.0).mean()),
    }


def purged_elapsed_time_split(
    episodes: pd.DataFrame,
    *,
    train_fraction: float = 0.60,
    validation_fraction: float = 0.20,
) -> TemporalSplit:
    """Split chronologically and purge labels crossing split boundaries.

    Boundaries are based on elapsed wall-clock time, not row quantiles. An
    episode assigned to train/validation is kept only if its censoring interval
    is fully known before the next partition begins. This prevents a label from
    peeking across a temporal boundary.
    """
    if episodes.empty:
        raise ValueError("Cannot split an empty high-frequency cohort.")
    if not (0 < train_fraction < 1):
        raise ValueError("train_fraction must be between 0 and 1.")
    if not (0 < validation_fraction < 1):
        raise ValueError("validation_fraction must be between 0 and 1.")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train_fraction + validation_fraction must be < 1.")

    ordered = episodes.sort_values("first_capture").copy()
    start = ordered["first_capture"].min()
    end = ordered["first_capture"].max()
    if pd.isna(start) or pd.isna(end) or start >= end:
        raise ValueError("High-frequency cohort does not span enough time to split.")

    span = end - start
    validation_start = start + span * train_fraction
    test_start = start + span * (train_fraction + validation_fraction)

    train_candidates = ordered[ordered["first_capture"] < validation_start]
    train = train_candidates[
        train_candidates["next_snapshot"].notna()
        & (train_candidates["next_snapshot"] <= validation_start)
    ].copy()

    validation_candidates = ordered[
        (ordered["first_capture"] >= validation_start)
        & (ordered["first_capture"] < test_start)
    ]
    validation = validation_candidates[
        validation_candidates["next_snapshot"].notna()
        & (validation_candidates["next_snapshot"] <= test_start)
    ].copy()

    test = ordered[ordered["first_capture"] >= test_start].copy()

    if min(len(train), len(validation), len(test)) == 0:
        raise ValueError(
            "Purged temporal split produced an empty train/validation/test partition."
        )

    return TemporalSplit(
        train=train,
        validation=validation,
        test=test,
        validation_start=validation_start,
        test_start=test_start,
        purged_train_rows=int(len(train_candidates) - len(train)),
        purged_validation_rows=int(len(validation_candidates) - len(validation)),
    )


def known_threshold_labels(frame: pd.DataFrame, threshold_hours: float):
    """Return mask/truth for rows whose threshold outcome is known despite censoring."""
    lower = frame["duration_lower_hours"].to_numpy(dtype=float)
    upper = frame["duration_upper_hours"].to_numpy(dtype=float)
    known = (lower > threshold_hours) | (upper <= threshold_hours)
    truth = (lower[known] > threshold_hours).astype(float)
    return known, truth
