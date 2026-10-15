"""Build leakage-safe interval/right-censored outage episodes for survival AFT."""

from __future__ import annotations

import numpy as np
import pandas as pd

from ml.dataset import _parse_datetime_columns

DEFAULT_MAX_START_LAG_HOURS = 24.0
DEFAULT_MAX_DURATION_HOURS = 72.0
MIN_DURATION_HOURS = 1.0 / 60.0


def build_survival_episode_dataset(
    raw: pd.DataFrame,
    *,
    max_start_lag_hours: float = DEFAULT_MAX_START_LAG_HOURS,
    max_duration_hours: float = DEFAULT_MAX_DURATION_HOURS,
) -> pd.DataFrame:
    """Collapse snapshots into continuous outage episodes with censored labels.

    Completed episodes receive an interval-censored label [lower, upper], where
    lower is the last observed presence and upper is the next represented
    successful snapshot. Episodes still present at the end of the dataset are
    right-censored and receive [lower, +inf]. Only values available at the first
    observation are retained as model features.
    """
    required = {
        "outage_id",
        "snapshot_id",
        "captured_at",
        "start_time",
        "customers_affected",
        "cause_label",
        "status",
        "lon",
        "lat",
    }
    missing = sorted(required.difference(raw.columns))
    if missing:
        raise ValueError(f"Missing required snapshot columns: {', '.join(missing)}")

    df = _parse_datetime_columns(raw)
    df = df.dropna(subset=["outage_id", "snapshot_id", "captured_at", "start_time"]).copy()

    snapshot_times = df.groupby("snapshot_id")["captured_at"].min().sort_values()
    snapshot_order = pd.Series(range(len(snapshot_times)), index=snapshot_times.index)
    df["snapshot_ord"] = df["snapshot_id"].map(snapshot_order).astype(int)
    df = df.sort_values(["outage_id", "snapshot_ord", "captured_at"])

    gap = df.groupby("outage_id")["snapshot_ord"].diff()
    df["episode_no"] = gap.fillna(1).gt(1).groupby(df["outage_id"]).cumsum().astype(int)
    df["episode_id"] = df["outage_id"].astype(str) + "::" + df["episode_no"].astype(str)

    agg_spec = {
        "outage_id": ("outage_id", "first"),
        "first_snapshot_id": ("snapshot_id", "first"),
        "first_ord": ("snapshot_ord", "min"),
        "last_ord": ("snapshot_ord", "max"),
        "first_capture": ("captured_at", "min"),
        "last_capture": ("captured_at", "max"),
        "n_observations": ("snapshot_id", "size"),
        "start_time": ("start_time", "first"),
        "customers_affected": ("customers_affected", "first"),
        "cause_label": ("cause_label", "first"),
        "status": ("status", "first"),
        "lon": ("lon", "first"),
        "lat": ("lat", "first"),
    }
    if "municipality_id" in df.columns:
        agg_spec["municipality_id"] = ("municipality_id", "first")
    if "estimated_restore" in df.columns:
        agg_spec["estimated_restore"] = ("estimated_restore", "first")

    # pandas GroupBy.first skips missing values and could pull a future ETA
    # (or other feature) from a later snapshot. Preserve the actual first row.
    initial_features = {
        "customers_affected", "cause_label", "status", "lon", "lat",
        "municipality_id", "estimated_restore",
    }
    for name in initial_features.intersection(agg_spec):
        agg_spec[name] = (name, lambda values: values.iloc[0])
    episodes = df.groupby("episode_id", as_index=False).agg(**agg_spec)

    ordered_times = snapshot_times.reset_index(drop=True)
    next_values = [
        ordered_times.iloc[ordinal + 1]
        if ordinal + 1 < len(ordered_times)
        else pd.NaT
        for ordinal in episodes["last_ord"].astype(int)
    ]
    episodes["next_snapshot"] = pd.to_datetime(next_values, utc=True)

    episodes["start_lag_hours"] = (
        episodes["first_capture"] - episodes["start_time"]
    ).dt.total_seconds() / 3600.0
    episodes["duration_lower_hours"] = (
        episodes["last_capture"] - episodes["start_time"]
    ).dt.total_seconds() / 3600.0
    episodes["duration_upper_hours"] = (
        episodes["next_snapshot"] - episodes["start_time"]
    ).dt.total_seconds() / 3600.0
    episodes.loc[episodes["next_snapshot"].isna(), "duration_upper_hours"] = np.inf

    valid = (
        episodes["start_lag_hours"].between(0, max_start_lag_hours)
        & episodes["duration_lower_hours"].ge(0)
        & episodes["duration_lower_hours"].le(max_duration_hours)
        & (
            np.isinf(episodes["duration_upper_hours"])
            | episodes["duration_upper_hours"].le(max_duration_hours)
        )
    )
    episodes = episodes.loc[valid].copy()

    episodes["duration_lower_hours"] = episodes["duration_lower_hours"].clip(
        lower=MIN_DURATION_HOURS
    )
    finite_upper = np.isfinite(episodes["duration_upper_hours"])
    episodes.loc[finite_upper, "duration_upper_hours"] = episodes.loc[
        finite_upper, "duration_upper_hours"
    ].clip(lower=MIN_DURATION_HOURS)

    episodes["is_right_censored"] = ~finite_upper
    episodes["interval_width_hours"] = np.where(
        finite_upper,
        episodes["duration_upper_hours"] - episodes["duration_lower_hours"],
        np.nan,
    )
    episodes["midpoint_duration_hours"] = np.where(
        finite_upper,
        (episodes["duration_lower_hours"] + episodes["duration_upper_hours"]) / 2.0,
        np.nan,
    )
    return episodes.reset_index(drop=True)
