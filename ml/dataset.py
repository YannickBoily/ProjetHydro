"""Build leakage-safe outage episodes and regression targets from snapshots."""

from __future__ import annotations

import numpy as np
import pandas as pd

DEFAULT_MAX_EXIT_INTERVAL_HOURS = 3.0
DEFAULT_MAX_START_LAG_HOURS = 24.0
DEFAULT_MAX_TARGET_HOURS = 72.0


def _parse_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    parsed = df.copy()
    for column in ("start_time", "estimated_restore", "captured_at", "ingested_at"):
        if column in parsed.columns:
            parsed[column] = pd.to_datetime(
                parsed[column], errors="coerce", utc=True, format="mixed"
            )
    return parsed


def build_episode_dataset(
    raw: pd.DataFrame,
    *,
    max_exit_interval_hours: float = DEFAULT_MAX_EXIT_INTERVAL_HOURS,
    max_start_lag_hours: float = DEFAULT_MAX_START_LAG_HOURS,
    max_target_hours: float = DEFAULT_MAX_TARGET_HOURS,
) -> pd.DataFrame:
    """Collapse snapshots into continuous outage episodes with bounded targets.

    An episode ends as soon as an outage is absent from the next represented
    successful snapshot. Because the exact restoration instant is not present in
    the source, the target is the midpoint between the last observed presence
    and the next represented snapshot. Episodes with a wide exit interval are
    excluded so the label uncertainty remains bounded.
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

    snapshot_times = (
        df.groupby("snapshot_id", as_index=True)["captured_at"]
        .min()
        .sort_values()
    )
    snapshot_order = pd.Series(range(len(snapshot_times)), index=snapshot_times.index)
    df["snapshot_ord"] = df["snapshot_id"].map(snapshot_order).astype(int)
    df = df.sort_values(["outage_id", "snapshot_ord", "captured_at"])

    gap = df.groupby("outage_id")["snapshot_ord"].diff()
    df["episode_no"] = (
        gap.fillna(1).gt(1).groupby(df["outage_id"]).cumsum().astype(int)
    )
    df["episode_id"] = df["outage_id"].astype(str) + "::" + df["episode_no"].astype(str)

    episodes = (
        df.groupby("episode_id", as_index=False)
        .agg(
            outage_id=("outage_id", "first"),
            first_snapshot_id=("snapshot_id", "first"),
            first_ord=("snapshot_ord", "min"),
            last_ord=("snapshot_ord", "max"),
            first_capture=("captured_at", "min"),
            last_capture=("captured_at", "max"),
            n_observations=("snapshot_id", "size"),
            start_time=("start_time", "first"),
            customers_affected=("customers_affected", "first"),
            cause_label=("cause_label", "first"),
            status=("status", "first"),
            municipality_id=("municipality_id", "first") if "municipality_id" in df.columns else ("outage_id", "size"),
            lon=("lon", "first"),
            lat=("lat", "first"),
            estimated_restore=("estimated_restore", "first") if "estimated_restore" in df.columns else ("captured_at", "max"),
        )
    )

    ordered_times = snapshot_times.reset_index(drop=True)
    next_values = []
    for ordinal in episodes["last_ord"].astype(int):
        next_values.append(
            ordered_times.iloc[ordinal + 1] if ordinal + 1 < len(ordered_times) else pd.NaT
        )
    episodes["next_snapshot"] = pd.to_datetime(next_values, utc=True)

    episodes["start_lag_hours"] = (
        episodes["first_capture"] - episodes["start_time"]
    ).dt.total_seconds() / 3600.0
    episodes["exit_interval_hours"] = (
        episodes["next_snapshot"] - episodes["last_capture"]
    ).dt.total_seconds() / 3600.0
    episodes["duration_lower_hours"] = (
        episodes["last_capture"] - episodes["start_time"]
    ).dt.total_seconds() / 3600.0
    episodes["duration_upper_hours"] = (
        episodes["next_snapshot"] - episodes["start_time"]
    ).dt.total_seconds() / 3600.0
    episodes["target_duration_hours"] = (
        episodes["duration_lower_hours"] + episodes["duration_upper_hours"]
    ) / 2.0
    episodes["target_uncertainty_hours"] = episodes["exit_interval_hours"] / 2.0

    valid = (
        episodes["next_snapshot"].notna()
        & episodes["start_lag_hours"].between(0, max_start_lag_hours)
        & episodes["exit_interval_hours"].between(0, max_exit_interval_hours)
        & episodes["duration_lower_hours"].ge(0)
        & episodes["target_duration_hours"].between(0, max_target_hours)
    )

    return episodes.loc[valid].reset_index(drop=True)
