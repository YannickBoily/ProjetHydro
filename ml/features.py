"""Feature engineering shared by XGBoost training and production inference."""

from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd

QUEBEC_TIMEZONE = "America/Toronto"

CAUSE_CATEGORIES = (
    "unknown",
    "other",
    "equipment",
    "vegetation",
    "accident",
    "weather",
    "animal",
)
STATUS_CATEGORIES = (
    "new",
    "assigned",
    "en_route",
    "working",
)

BASE_NUMERIC_FEATURES = (
    "log_customers",
    "lon",
    "lat",
    "lag_start_to_capture_h",
    "capture_hour_sin",
    "capture_hour_cos",
    "start_hour_sin",
    "start_hour_cos",
    "weekday_sin",
    "weekday_cos",
    "month_sin",
    "month_cos",
)

CORE_FEATURE_NAMES = (
    *BASE_NUMERIC_FEATURES,
    *(f"cause__{value}" for value in CAUSE_CATEGORIES),
    *(f"status__{value}" for value in STATUS_CATEGORIES),
)

AUGMENTED_FEATURE_NAMES = (
    *BASE_NUMERIC_FEATURES,
    "estimated_restore_remaining_h",
    "has_estimated_restore",
    *(f"cause__{value}" for value in CAUSE_CATEGORIES),
    *(f"status__{value}" for value in STATUS_CATEGORIES),
)


def _first_existing_column(df: pd.DataFrame, names: Iterable[str]) -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series(index=df.index, dtype="object")


def _parse_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True, format="mixed")


def _cyclical(values: pd.Series, period: float) -> tuple[pd.Series, pd.Series]:
    radians = 2 * math.pi * pd.to_numeric(values, errors="coerce") / period
    return np.sin(radians), np.cos(radians)


def build_feature_frame(df: pd.DataFrame, *, include_hydro_eta: bool = True) -> pd.DataFrame:
    """Build a fixed numeric feature matrix for outage-duration inference.

    The function only uses values that can be known at the first observation of
    an outage episode. It is intentionally shared by training and production to
    prevent feature skew.
    """
    if df.empty:
        names = AUGMENTED_FEATURE_NAMES if include_hydro_eta else CORE_FEATURE_NAMES
        return pd.DataFrame(columns=list(names), index=df.index, dtype=float)

    customers = pd.to_numeric(_first_existing_column(df, ["customers_affected", "customers_initial"]), errors="coerce")
    lon = pd.to_numeric(_first_existing_column(df, ["lon"]), errors="coerce")
    lat = pd.to_numeric(_first_existing_column(df, ["lat"]), errors="coerce")
    start = _parse_utc(_first_existing_column(df, ["start_time"]))
    capture = _parse_utc(
        _first_existing_column(
            df,
            ["captured_at", "first_capture", "first_capture_at", "active_capture_at"],
        )
    )
    estimated_restore = _parse_utc(
        _first_existing_column(df, ["estimated_restore", "est_restore_initial"])
    )

    capture_local = capture.dt.tz_convert(QUEBEC_TIMEZONE)
    start_local = start.dt.tz_convert(QUEBEC_TIMEZONE)

    capture_hour = capture_local.dt.hour + capture_local.dt.minute / 60.0
    start_hour = start_local.dt.hour + start_local.dt.minute / 60.0
    weekday = capture_local.dt.weekday
    month = capture_local.dt.month

    capture_hour_sin, capture_hour_cos = _cyclical(capture_hour, 24.0)
    start_hour_sin, start_hour_cos = _cyclical(start_hour, 24.0)
    weekday_sin, weekday_cos = _cyclical(weekday, 7.0)
    month_sin, month_cos = _cyclical(month, 12.0)

    matrix = pd.DataFrame(index=df.index)
    matrix["log_customers"] = np.log1p(customers.clip(lower=0))
    matrix["lon"] = lon
    matrix["lat"] = lat
    matrix["lag_start_to_capture_h"] = (capture - start).dt.total_seconds() / 3600.0
    matrix["capture_hour_sin"] = capture_hour_sin
    matrix["capture_hour_cos"] = capture_hour_cos
    matrix["start_hour_sin"] = start_hour_sin
    matrix["start_hour_cos"] = start_hour_cos
    matrix["weekday_sin"] = weekday_sin
    matrix["weekday_cos"] = weekday_cos
    matrix["month_sin"] = month_sin
    matrix["month_cos"] = month_cos

    if include_hydro_eta:
        remaining = (estimated_restore - capture).dt.total_seconds() / 3600.0
        matrix["estimated_restore_remaining_h"] = remaining
        matrix["has_estimated_restore"] = estimated_restore.notna().astype(float)

    cause = (
        _first_existing_column(
            df,
            ["cause_label", "cause_initial", "analysis_cause_label", "latest_raw_cause_label"],
        )
        .fillna("unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    status = (
        _first_existing_column(df, ["status", "status_initial"])
        .fillna("unknown")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    for category in CAUSE_CATEGORIES:
        matrix[f"cause__{category}"] = (cause == category).astype(float)
    for category in STATUS_CATEGORIES:
        matrix[f"status__{category}"] = (status == category).astype(float)

    feature_names = AUGMENTED_FEATURE_NAMES if include_hydro_eta else CORE_FEATURE_NAMES
    return matrix.reindex(columns=list(feature_names)).astype(float)
