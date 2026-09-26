import importlib.util
import json
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from scripts import fetch_outages

# The execution sandbox used for this review does not ship psycopg2. The real
# project installs it from requirements.txt (including in CI), so provide a
# minimal import stub only for these pure loader/helper unit tests.
if importlib.util.find_spec("psycopg2") is None:
    psycopg2_stub = types.ModuleType("psycopg2")
    extras_stub = types.ModuleType("psycopg2.extras")
    extras_stub.execute_values = lambda *args, **kwargs: None
    psycopg2_stub.extras = extras_stub
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extras"] = extras_stub

from scripts import sync_to_supabase


class FakeResponse:
    def __init__(self, *, text="", payload=None, headers=None, json_error=None):
        self.text = text
        self._payload = payload
        self.headers = headers or {}
        self._json_error = json_error

    def raise_for_status(self):
        return None

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


def install_fake_hydro_responses(monkeypatch, payload, *, content_type="application/json"):
    session = FakeSession(
        [
            FakeResponse(text='"123"'),
            FakeResponse(
                payload=payload,
                headers={"Content-Type": content_type} if content_type else {},
            ),
        ]
    )
    monkeypatch.setattr(fetch_outages, "build_http_session", lambda: session)
    return session


def test_fetch_uses_one_capture_timestamp(monkeypatch):
    payload = {
        "pannes": [
            [12, "2026-08-28T18:00:00", None, None, [-73.5, 45.5], "N", None, 21, 101],
            [34, "2026-08-28T18:05:00", None, None, [-72.5, 46.5], "L", None, 51, 202],
        ]
    }
    install_fake_hydro_responses(monkeypatch, payload)

    df = fetch_outages.fetch_current_outages()
    metadata = df.attrs[fetch_outages.SNAPSHOT_METADATA_ATTR]

    assert len(df) == 2
    assert df["captured_at"].nunique() == 1
    assert list(df.columns) == fetch_outages.EXPECTED_COLUMNS
    assert set(df["cause_label"]) == {"weather", "vegetation"}
    assert metadata["outage_count"] == 2
    assert metadata["snapshot_id"]
    assert metadata["status"] == "success"


def test_zero_outage_snapshot_is_valid(monkeypatch):
    install_fake_hydro_responses(monkeypatch, {"pannes": []})

    df = fetch_outages.fetch_current_outages()
    metadata = df.attrs[fetch_outages.SNAPSHOT_METADATA_ATTR]

    assert df.empty
    assert list(df.columns) == fetch_outages.EXPECTED_COLUMNS
    assert metadata["outage_count"] == 0
    assert metadata["status"] == "success"
    assert metadata["captured_at"]


def test_nonempty_but_unparseable_payload_fails(monkeypatch):
    install_fake_hydro_responses(monkeypatch, {"pannes": [{"unexpected": "shape"}]})

    with pytest.raises(RuntimeError, match="aucune ligne"):
        fetch_outages.fetch_current_outages()


def test_write_empty_snapshot_persists_manifest(monkeypatch, tmp_path: Path):
    install_fake_hydro_responses(monkeypatch, {"pannes": []})
    df = fetch_outages.fetch_current_outages()

    snapshot_file = tmp_path / "current_snapshot.csv"
    fetch_outages.write_current_snapshot(df, snapshot_file)

    metadata_file = tmp_path / "current_snapshot_meta.json"
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    loaded = pd.read_csv(snapshot_file)

    assert loaded.empty
    assert list(loaded.columns) == fetch_outages.EXPECTED_COLUMNS
    assert metadata["outage_count"] == 0
    assert metadata["snapshot_id"] == df.attrs[fetch_outages.SNAPSHOT_METADATA_ATTR]["snapshot_id"]


def test_sync_loader_keeps_zero_outage_snapshot(monkeypatch, tmp_path: Path):
    install_fake_hydro_responses(monkeypatch, {"pannes": []})
    df = fetch_outages.fetch_current_outages()
    snapshot_file = tmp_path / "current_snapshot.csv"
    fetch_outages.write_current_snapshot(df, snapshot_file)

    monkeypatch.setattr(sync_to_supabase, "CURRENT_SNAPSHOT_FILE", snapshot_file)
    monkeypatch.setattr(
        sync_to_supabase,
        "CURRENT_SNAPSHOT_META_FILE",
        tmp_path / "current_snapshot_meta.json",
    )

    loaded, run = sync_to_supabase.load_current_snapshot()

    assert loaded.empty
    assert run is not None
    assert run["outage_count"] == 0
    assert run["snapshot_id"] == df.attrs[fetch_outages.SNAPSHOT_METADATA_ATTR]["snapshot_id"]



def test_sync_plan_keeps_zero_outage_snapshot_instead_of_fallback(
    monkeypatch, tmp_path: Path
):
    install_fake_hydro_responses(monkeypatch, {"pannes": []})
    df = fetch_outages.fetch_current_outages()
    snapshot_file = tmp_path / "current_snapshot.csv"
    fetch_outages.write_current_snapshot(df, snapshot_file)

    history_file = tmp_path / "hydroquebec_history.csv"
    pd.DataFrame(
        [{
            **{column: None for column in fetch_outages.EXPECTED_COLUMNS},
            "outage_id": "old-outage",
            "captured_at": "2026-01-01 00:00:00",
        }]
    ).to_csv(history_file, index=False)

    monkeypatch.setattr(sync_to_supabase, "CURRENT_SNAPSHOT_FILE", snapshot_file)
    monkeypatch.setattr(
        sync_to_supabase,
        "CURRENT_SNAPSHOT_META_FILE",
        tmp_path / "current_snapshot_meta.json",
    )
    monkeypatch.setattr(sync_to_supabase, "RAW_FILE", history_file)
    monkeypatch.setattr(sync_to_supabase, "raw_table_is_empty", lambda connection: False)

    rows, runs = sync_to_supabase.choose_raw_sync_batch(object())

    assert rows.empty
    assert len(runs) == 1
    assert runs[0]["outage_count"] == 0
    assert runs[0]["snapshot_id"] == df.attrs[fetch_outages.SNAPSHOT_METADATA_ATTR]["snapshot_id"]

def test_append_local_history_does_not_rewrite_existing_rows(tmp_path: Path):
    history_file = tmp_path / "history.csv"

    first = pd.DataFrame(
        [{column: None for column in fetch_outages.EXPECTED_COLUMNS}]
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


def test_legacy_snapshot_id_is_deterministic():
    first = sync_to_supabase.legacy_snapshot_id("2026-08-28 18:00:00")
    second = sync_to_supabase.legacy_snapshot_id(pd.Timestamp("2026-08-28T18:00:00Z"))

    assert first == second
    assert first == "legacy:20260828T180000.000000"


def test_fetch_normalizes_hydro_local_times_to_utc(monkeypatch):
    payload = {
        "pannes": [
            [
                12,
                "2026-03-30T08:42:24",
                "2026-03-30T13:45:00",
                None,
                [-73.5, 45.5],
                "N",
                None,
                21,
                101,
            ]
        ]
    }
    session = install_fake_hydro_responses(monkeypatch, payload)

    df = fetch_outages.fetch_current_outages()

    assert df.loc[0, "start_time"] == pd.Timestamp("2026-03-30T12:42:24Z")
    assert df.loc[0, "estimated_restore"] == pd.Timestamp("2026-03-30T17:45:00Z")
    assert all(call[1]["timeout"] == fetch_outages.HTTP_TIMEOUT for call in session.calls)


def test_http_session_configures_retries_and_headers():
    session = fetch_outages.build_http_session()
    retries = session.get_adapter("https://").max_retries

    assert retries.total == fetch_outages.HTTP_RETRY_TOTAL
    assert retries.status == fetch_outages.HTTP_RETRY_TOTAL
    assert set(fetch_outages.HTTP_RETRY_STATUS_CODES).issubset(set(retries.status_forcelist))
    assert "ProjetHydro" in session.headers["User-Agent"]


def test_fetch_rejects_explicit_non_json_payload(monkeypatch):
    install_fake_hydro_responses(
        monkeypatch,
        {"pannes": []},
        content_type="text/html; charset=utf-8",
    )

    with pytest.raises(RuntimeError, match="Content-Type JSON attendu"):
        fetch_outages.fetch_current_outages()


def test_sync_normalizes_legacy_mixed_timezones():
    df = pd.DataFrame(
        [
            {
                **{column: None for column in fetch_outages.EXPECTED_COLUMNS},
                "outage_id": "timezone-test",
                "start_time": "2026-03-30 08:42:24",
                "estimated_restore": "2026-03-30 13:45:00",
                "captured_at": "2026-03-30 14:24:45",
            }
        ]
    )

    normalized = sync_to_supabase.normalize_raw_dataframe(df)

    assert normalized.loc[0, "start_time"] == pd.Timestamp("2026-03-30T12:42:24Z")
    assert normalized.loc[0, "estimated_restore"] == pd.Timestamp("2026-03-30T17:45:00Z")
    assert normalized.loc[0, "captured_at"] == pd.Timestamp("2026-03-30T14:24:45Z")
