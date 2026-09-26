from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_postgres_active_logic_uses_successful_snapshot_id():
    schema = (ROOT / "supabase" / "schema.sql").read_text(encoding="utf-8")
    refresh = (ROOT / "sql" / "postgres" / "refresh_active_outages.sql").read_text(encoding="utf-8")

    assert "INTERVAL '5 minutes'" not in schema
    assert "INTERVAL '5 minutes'" not in refresh
    assert "WHERE status = 'success'" in schema
    assert "r.snapshot_id = s.snapshot_id" in schema
    assert "r.snapshot_id = s.snapshot_id" in refresh


def test_duckdb_active_logic_uses_exact_latest_capture():
    active_sql = (ROOT / "sql" / "03_create_active_outages.sql").read_text(
        encoding="utf-8"
    )

    assert "INTERVAL '5 minutes'" not in active_sql
    assert "r.captured_at = l.max_captured_at" in active_sql
