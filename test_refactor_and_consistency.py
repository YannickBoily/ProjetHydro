from pathlib import Path

from scripts import refresh_supabase_analytics


ROOT = Path(__file__).resolve().parents[1]


def test_dashboard_is_split_into_reusable_modules():
    dashboard_dir = ROOT / "dashboard"
    main = (dashboard_dir / "streamlit_app.py").read_text(encoding="utf-8")

    for filename in ("config.py", "data_access.py", "view_helpers.py", "components.py"):
        assert (dashboard_dir / filename).exists()

    # The main file stays the orchestrator instead of owning data-access internals.
    assert "def load_supabase_active" not in main
    assert "def normalize_dataframe" not in main
    assert "def render_clean_map" not in main
    assert len(main.splitlines()) < 1700


def test_postgres_refresh_loads_versioned_sql_files():
    sql_dir = ROOT / "sql" / "postgres"
    expected = {
        "ensure_refresh_state.sql",
        "ensure_incremental_tables.sql",
        "migrate_incremental_timestamps.sql",
        "refresh_latest_incremental.sql",
        "refresh_active_outages.sql",
        "ensure_performance_indexes.sql",
        "refresh_daily_summary.sql",
        "refresh_data_quality_report.sql",
        "secure_app_tables.sql",
    }

    assert expected.issubset({path.name for path in sql_dir.glob("*.sql")})
    for filename in expected:
        sql = refresh_supabase_analytics.load_sql(filename)
        assert sql.strip()


def test_duckdb_and_postgres_duration_semantics_are_fractional_hours():
    duck_latest = (ROOT / "sql" / "02_create_latest_outages.sql").read_text(encoding="utf-8")
    duck_active = (ROOT / "sql" / "03_create_active_outages.sql").read_text(encoding="utf-8")
    pg_latest = (ROOT / "sql" / "postgres" / "refresh_latest_incremental.sql").read_text(encoding="utf-8")

    for sql in (duck_latest, duck_active):
        assert "DATE_DIFF('hour'" not in sql
        assert "DATE_DIFF('second'" in sql
        assert "/ 3600.0" in sql

    assert "EXTRACT(EPOCH FROM (s.last_capture_at - s.first_capture_at)) / 3600.0" in pg_latest
    assert "EXTRACT(EPOCH FROM (r.captured_at - r.start_time)) / 3600.0" in pg_latest
    assert "EXTRACT(EPOCH FROM (r.estimated_restore - r.captured_at)) / 3600.0" in pg_latest


def test_dependency_files_are_pinned_and_ci_uses_dev_lock():
    runtime = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    dev = (ROOT / "requirements-dev.txt").read_text(encoding="utf-8")
    geo = (ROOT / "requirements-geo.txt").read_text(encoding="utf-8")
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    runtime_lines = [
        line.strip()
        for line in runtime.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert runtime_lines
    assert all("==" in line for line in runtime_lines)
    assert "pytest==" in dev
    assert "geopandas==" in geo
    assert "requirements-dev.txt" in ci


def test_duckdb_and_postgres_daily_snapshot_grouping_match():
    duck_daily = (ROOT / "sql" / "04_create_daily_summary.sql").read_text(encoding="utf-8")
    pg_daily = (ROOT / "sql" / "postgres" / "refresh_daily_summary.sql").read_text(encoding="utf-8")

    assert "DATE_TRUNC('minute', captured_at)" in duck_daily
    assert "DATE_TRUNC('minute', r.captured_at)" in pg_daily
    assert "America/Toronto" in duck_daily
    assert "America/Toronto" in pg_daily
