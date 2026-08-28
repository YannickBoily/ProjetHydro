from pathlib import Path

import pandas as pd

from scripts import fetch_outages


class FakeResponse:
    def __init__(self, *, text="", payload=None):
        self.text = text
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_fetch_uses_one_capture_timestamp(monkeypatch):
    payload = {
        "pannes": [
            [12, "2026-08-28T18:00:00", None, None, [-73.5, 45.5], "N", None, 21, 101],
            [34, "2026-08-28T18:05:00", None, None, [-72.5, 46.5], "L", None, 51, 202],
        ]
    }

    responses = iter(
        [
            FakeResponse(text='"123"'),
            FakeResponse(payload=payload),
        ]
    )

    monkeypatch.setattr(
        fetch_outages.requests,
        "get",
        lambda *args, **kwargs: next(responses),
    )

    df = fetch_outages.fetch_current_outages()

    assert len(df) == 2
    assert df["captured_at"].nunique() == 1
    assert list(df.columns) == fetch_outages.EXPECTED_COLUMNS
    assert set(df["cause_label"]) == {"weather", "vegetation"}


def test_append_local_history_does_not_rewrite_existing_rows(tmp_path: Path):
    history_file = tmp_path / "history.csv"

    first = pd.DataFrame(
        [
            {
                column: None
                for column in fetch_outages.EXPECTED_COLUMNS
            }
        ]
    )
    first.loc[0, "outage_id"] = "first"
    first.loc[0, "captured_at"] = "2026-08-28 18:00:00"

    second = first.copy()
    second.loc[0, "outage_id"] = "second"
    second.loc[0, "captured_at"] = "2026-08-28 19:00:00"

    fetch_outages.append_local_history(first, history_file)
    first_size = history_file.stat().st_size
    fetch_outages.append_local_history(second, history_file)

    loaded = pd.read_csv(history_file)

    assert first_size > 0
    assert loaded["outage_id"].tolist() == ["first", "second"]
