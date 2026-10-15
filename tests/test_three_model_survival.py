import numpy as np
import pandas as pd

from ml.three_model_survival import (
    build_live_latest_eta_feature_frame,
    episode_equal_weights,
)


def _history_frame():
    return pd.DataFrame({
        "episode_id": ["a", "a", "a"],
        "outage_id": ["x", "x", "x"],
        "captured_at": pd.to_datetime([
            "2026-10-01T10:00:00Z",
            "2026-10-01T10:20:00Z",
            "2026-10-01T10:40:00Z",
        ]),
        "start_time": pd.to_datetime([
            "2026-10-01T09:50:00Z",
            "2026-10-01T09:50:00Z",
            "2026-10-01T09:50:00Z",
        ]),
        "estimated_restore": pd.to_datetime([
            "2026-10-01T12:00:00Z",
            None,
            "2026-10-01T13:00:00Z",
        ], utc=True),
        "latest_estimated_restore": pd.to_datetime([
            "2026-10-01T12:00:00Z",
            "2026-10-01T12:00:00Z",
            "2026-10-01T13:00:00Z",
        ]),
        "has_latest_hydro_eta": [True, True, True],
        "customers_affected": [100, 90, 80],
        "cause_label": ["weather", "weather", "weather"],
        "status": ["new", "working", "working"],
        "lon": [-73.5, -73.5, -73.5],
        "lat": [45.5, 45.5, 45.5],
        "observation_number": [1, 2, 3],
        "elapsed_since_first_capture_hours": [0, 1/3, 2/3],
        "customers_change_from_previous": [np.nan, -10, -10],
        "status_change_count": [0, 1, 1],
        "hours_in_current_status": [0, 0, 1/3],
        "has_working_status_seen": [False, True, True],
        "hours_since_first_working_status": [np.nan, 0, 1/3],
        "has_known_current_status": [True, True, True],
        "customers_change_from_initial": [0, -10, -20],
        "customers_fraction_of_initial": [1, .9, .8],
        "customers_fraction_of_peak_so_far": [1, .9, .8],
        "customers_decline_fraction_from_peak": [0, .1, .2],
        "hydro_eta_revision_count": [0, 0, 1],
        "hydro_latest_revision_hours": [0, 0, 1],
        "latest_hydro_eta_overdue": [False, False, False],
        "latest_eta_age_hours": [0, 1/3, 0],
        "customers_initial_observed": [100, 100, 100],
        "customers_peak_observed_so_far": [100, 100, 100],
    })


def test_live_feature_uses_forward_filled_latest_eta_not_current_null():
    frame = _history_frame()
    features = build_live_latest_eta_feature_frame(frame)
    # At 10:20 the raw ETA is null, but the latest causally-known ETA is noon.
    assert np.isclose(features.loc[1, "latest_estimated_restore_remaining_h"], 100 / 60)
    assert features.loc[1, "has_latest_estimated_restore"] == 1.0


def test_live_feature_future_revision_not_visible_before_revision():
    frame = _history_frame()
    features = build_live_latest_eta_feature_frame(frame)
    assert np.isclose(features.loc[0, "latest_estimated_restore_remaining_h"], 2.0)
    assert np.isclose(features.loc[1, "latest_estimated_restore_remaining_h"], 100 / 60)
    # The 13:00 revision appears only at 10:40.
    assert np.isclose(features.loc[2, "latest_estimated_restore_remaining_h"], 140 / 60)


def test_episode_equal_weights_give_each_episode_total_one():
    frame = pd.DataFrame({"episode_id": ["a", "a", "a", "b"]})
    weights = episode_equal_weights(frame)
    assert np.isclose(weights[:3].sum(), 1.0)
    assert np.isclose(weights[3:].sum(), 1.0)
