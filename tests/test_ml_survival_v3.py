from pathlib import Path

import numpy as np
import pandas as pd

from ml.high_frequency_survival import (
    purged_elapsed_time_split,
    select_high_frequency_cohort,
)
from ml.train_survival_model_v3 import MODEL_VERSION, aft_params

ROOT = Path(__file__).resolve().parents[1]


def _raw_rows():
    rows = []
    captures = [
        "2026-09-29T00:20:00Z",
        "2026-09-29T00:40:00Z",
        "2026-09-29T01:00:00Z",
        "2026-09-29T01:20:00Z",
        "2026-09-29T01:40:00Z",
        "2026-09-29T02:00:00Z",
    ]
    snapshots = [f"s{i}" for i in range(len(captures))]
    # a: narrow completed interval
    for i in (0, 1):
        rows.append({
            "outage_id": "a",
            "snapshot_id": snapshots[i],
            "captured_at": captures[i],
            "start_time": "2026-09-29T00:10:00Z",
            "estimated_restore": "2026-09-29T03:00:00Z",
            "customers_affected": 10,
            "cause_label": "equipment",
            "status": "new",
            "lon": -73.5,
            "lat": 45.5,
        })
    # b: right-censored at the end
    rows.append({
        "outage_id": "b",
        "snapshot_id": snapshots[-1],
        "captured_at": captures[-1],
        "start_time": "2026-09-29T01:30:00Z",
        "estimated_restore": None,
        "customers_affected": 5,
        "cause_label": "unknown",
        "status": "new",
        "lon": -71.2,
        "lat": 46.8,
    })
    # filler outages ensure all snapshot IDs are represented globally
    for i, (sid, captured) in enumerate(zip(snapshots, captures)):
        rows.append({
            "outage_id": f"filler-{i}",
            "snapshot_id": sid,
            "captured_at": captured,
            "start_time": captured,
            "estimated_restore": None,
            "customers_affected": 1,
            "cause_label": "unknown",
            "status": "new",
            "lon": -72.0,
            "lat": 46.0,
        })
    return pd.DataFrame(rows)


def test_high_frequency_cohort_keeps_narrow_and_right_censored_rows():
    cohort = select_high_frequency_cohort(
        _raw_rows(),
        regime_start="2026-09-29T00:20:00Z",
        max_interval_minutes=25,
    )
    assert "a::0" in set(cohort["episode_id"])
    assert "b::0" in set(cohort["episode_id"])
    a = cohort.loc[cohort["episode_id"] == "a::0"].iloc[0]
    b = cohort.loc[cohort["episode_id"] == "b::0"].iloc[0]
    assert a["interval_width_hours"] <= 25 / 60
    assert bool(b["is_right_censored"])


def test_purged_split_removes_labels_that_cross_boundaries():
    start = pd.Timestamp("2026-10-01T00:00:00Z")
    rows = []
    for i in range(12):
        first = start + pd.Timedelta(hours=i)
        rows.append({
            "episode_id": f"e{i}",
            "first_capture": first,
            "next_snapshot": first + pd.Timedelta(minutes=20),
            "duration_lower_hours": 1.0,
            "duration_upper_hours": 1.3,
            "is_right_censored": False,
        })
    frame = pd.DataFrame(rows)
    split = purged_elapsed_time_split(frame, train_fraction=0.5, validation_fraction=0.25)
    assert split.train["next_snapshot"].max() <= split.validation_start
    assert split.validation["next_snapshot"].max() <= split.test_start
    assert split.test["first_capture"].min() >= split.test_start


def test_v3_is_candidate_and_does_not_replace_v2_paths():
    trainer = (ROOT / "ml" / "train_survival_model_v3.py").read_text(encoding="utf-8")
    assert MODEL_VERSION == "xgb-aft-duration-v3-candidate"
    assert "v3_candidate" in trainer
    assert "test_used_for_model_selection\": False" in trainer
    assert "duration_xgb_aft_v2.json" not in trainer


def test_v3_aft_parameters_use_survival_objective():
    params = aft_params(scale=0.5, max_depth=3, min_child_weight=5)
    assert params["objective"] == "survival:aft"
    assert params["aft_loss_distribution"] == "normal"
    assert np.isclose(params["aft_loss_distribution_scale"], 0.5)


def test_v3_candidate_artifact_loads_when_present():
    from ml.survival_predict_v3 import load_survival_v3_candidate, predict_survival_v3

    metadata = ROOT / "ml" / "artifacts" / "v3_candidate" / "duration_survival_v3_metadata.json"
    if not metadata.exists():
        return
    bundle = load_survival_v3_candidate(metadata)
    row = pd.DataFrame([{
        "customers_affected": 50,
        "start_time": "2026-10-02T12:00:00Z",
        "captured_at": "2026-10-02T12:20:00Z",
        "estimated_restore": "2026-10-02T15:00:00Z",
        "cause_label": "unknown",
        "status": "new",
        "lon": -73.0,
        "lat": 46.0,
    }])
    prediction = predict_survival_v3(row, bundle=bundle)
    assert bundle.metadata["model_version"] == "xgb-aft-duration-v3-candidate"
    assert prediction.loc[0, "predicted_duration_hours"] > 0
    assert prediction.loc[0, "probability_over_2h"] >= prediction.loc[0, "probability_over_4h"] >= prediction.loc[0, "probability_over_8h"]
