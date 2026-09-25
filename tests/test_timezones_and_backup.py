import gzip
import json
from pathlib import Path

import pandas as pd

from scripts import archive_snapshot
from scripts.time_utils import (
    QUEBEC_TIMEZONE,
    UTC_TIMEZONE,
    normalize_timestamp,
)

ROOT = Path(__file__).resolve().parents[1]


def test_quebec_naive_timestamp_respects_dst():
    summer = normalize_timestamp(
        "2026-07-15 08:00:00",
        naive_timezone=QUEBEC_TIMEZONE,
    )
    winter = normalize_timestamp(
        "2026-01-15 08:00:00",
        naive_timezone=QUEBEC_TIMEZONE,
    )

    assert summer == pd.Timestamp("2026-07-15T12:00:00Z")
    assert winter == pd.Timestamp("2026-01-15T13:00:00Z")


def test_capture_naive_timestamp_is_interpreted_as_utc():
    captured = normalize_timestamp(
        "2026-07-15 12:00:00",
        naive_timezone=UTC_TIMEZONE,
    )
    assert captured == pd.Timestamp("2026-07-15T12:00:00Z")


def test_archive_snapshot_creates_gzip_and_checksum_manifest(tmp_path: Path):
    snapshot = tmp_path / "current_snapshot.csv"
    metadata = tmp_path / "current_snapshot_meta.json"
    archive_root = tmp_path / "archive"

    pd.DataFrame(
        [
            {
                "outage_id": "abc",
                "captured_at": "2026-09-25T20:00:00+00:00",
                "customers_affected": 12,
            }
        ]
    ).to_csv(snapshot, index=False)
    metadata.write_text(
        json.dumps(
            {
                "snapshot_id": "snapshot-123",
                "captured_at": "2026-09-25T20:00:00+00:00",
                "outage_count": 1,
                "status": "success",
            }
        ),
        encoding="utf-8",
    )

    csv_gz, manifest = archive_snapshot.archive_snapshot(
        snapshot,
        metadata,
        archive_root,
    )

    assert csv_gz.exists()
    assert manifest.exists()
    with gzip.open(csv_gz, "rt", encoding="utf-8") as handle:
        restored = pd.read_csv(handle)
    archived_meta = json.loads(manifest.read_text(encoding="utf-8"))

    assert restored["outage_id"].tolist() == ["abc"]
    assert archived_meta["snapshot_id"] == "snapshot-123"
    assert len(archived_meta["source_csv_sha256"]) == 64
    assert len(archived_meta["archive_sha256"]) == 64


def test_postgres_schema_migrates_timestamps_with_correct_semantics():
    schema = (ROOT / "supabase" / "schema.sql").read_text(encoding="utf-8")
    sync = (ROOT / "scripts" / "sync_to_supabase.py").read_text(encoding="utf-8")
    analytics = (ROOT / "scripts" / "refresh_supabase_analytics.py").read_text(
        encoding="utf-8"
    )

    for text in (schema, sync):
        assert "captured_at TYPE TIMESTAMPTZ" in text
        assert "captured_at AT TIME ZONE 'UTC'" in text
        assert "start_time TYPE TIMESTAMPTZ" in text
        assert "start_time AT TIME ZONE 'America/Toronto'" in text
        assert "estimated_restore AT TIME ZONE 'America/Toronto'" in text

    assert "(r.captured_at AT TIME ZONE 'America/Toronto')::date" in analytics
    assert "timezone_migrated" in analytics


def test_duckdb_daily_summary_uses_quebec_calendar_date():
    sql = (ROOT / "sql" / "04_create_daily_summary.sql").read_text(encoding="utf-8")
    assert "timezone('America/Toronto', captured_at)" in sql
    assert "timezone('America/Toronto', first_seen_at)" in sql


def test_hourly_workflow_archives_before_supabase_sync():
    workflow = (ROOT / ".github" / "workflows" / "hydro.yml").read_text(
        encoding="utf-8"
    )

    archive_pos = workflow.index("Archive raw snapshot")
    upload_pos = workflow.index("Upload raw snapshot backup")
    sync_pos = workflow.index("Sync current snapshot to Supabase")

    assert archive_pos < upload_pos < sync_pos
    assert "actions/upload-artifact@v4" in workflow
    assert "continue-on-error: true" in workflow
    assert "retention-days: 30" in workflow


def test_archived_snapshot_can_be_restored_for_replay(tmp_path: Path):
    from scripts import restore_snapshot_archive

    snapshot = tmp_path / "source.csv"
    metadata = tmp_path / "source_meta.json"
    archive_root = tmp_path / "archive"
    restored_snapshot = tmp_path / "restored" / "current_snapshot.csv"
    restored_metadata = tmp_path / "restored" / "current_snapshot_meta.json"

    pd.DataFrame(
        [{"outage_id": "replay-me", "captured_at": "2026-09-25T20:00:00+00:00"}]
    ).to_csv(snapshot, index=False)
    metadata.write_text(
        json.dumps(
            {
                "snapshot_id": "replay-123",
                "captured_at": "2026-09-25T20:00:00+00:00",
                "outage_count": 1,
            }
        ),
        encoding="utf-8",
    )

    _, manifest = archive_snapshot.archive_snapshot(snapshot, metadata, archive_root)
    restore_snapshot_archive.restore_snapshot_archive(
        manifest,
        restored_snapshot,
        restored_metadata,
    )

    assert pd.read_csv(restored_snapshot)["outage_id"].tolist() == ["replay-me"]
    restored_meta = json.loads(restored_metadata.read_text(encoding="utf-8"))
    assert restored_meta["snapshot_id"] == "replay-123"


def test_zero_outage_snapshot_is_archived(tmp_path: Path):
    snapshot = tmp_path / "empty.csv"
    metadata = tmp_path / "empty_meta.json"
    archive_root = tmp_path / "archive"

    pd.DataFrame(columns=["outage_id", "captured_at"]).to_csv(snapshot, index=False)
    metadata.write_text(
        json.dumps(
            {
                "snapshot_id": "empty-123",
                "captured_at": "2026-09-25T21:00:00+00:00",
                "outage_count": 0,
            }
        ),
        encoding="utf-8",
    )

    csv_gz, manifest = archive_snapshot.archive_snapshot(snapshot, metadata, archive_root)

    assert csv_gz.exists()
    assert json.loads(manifest.read_text(encoding="utf-8"))["outage_count"] == 0


def test_restore_rejects_corrupted_archive(tmp_path: Path):
    from scripts import restore_snapshot_archive

    snapshot = tmp_path / "source.csv"
    metadata = tmp_path / "source_meta.json"
    archive_root = tmp_path / "archive"

    pd.DataFrame(
        [{"outage_id": "abc", "captured_at": "2026-09-25T20:00:00+00:00"}]
    ).to_csv(snapshot, index=False)
    metadata.write_text(
        json.dumps(
            {
                "snapshot_id": "corrupt-123",
                "captured_at": "2026-09-25T20:00:00+00:00",
                "outage_count": 1,
            }
        ),
        encoding="utf-8",
    )

    csv_gz, manifest = archive_snapshot.archive_snapshot(snapshot, metadata, archive_root)
    csv_gz.write_bytes(csv_gz.read_bytes() + b"corruption")

    import pytest

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        restore_snapshot_archive.restore_snapshot_archive(
            manifest,
            tmp_path / "restored.csv",
            tmp_path / "restored_meta.json",
        )
