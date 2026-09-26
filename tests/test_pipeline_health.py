from pathlib import Path

import pandas as pd

from dashboard.pipeline_health import assess_pipeline_health, outage_change_pct


ROOT = Path(__file__).resolve().parents[1]
NOW = pd.Timestamp("2026-09-26T01:00:00Z")


def base_health_row():
    return {
        "latest_success_captured_at": pd.Timestamp("2026-09-26T00:30:00Z"),
        "incremental_refreshed_at": pd.Timestamp("2026-09-26T00:31:00Z"),
        "heavy_refreshed_at": pd.Timestamp("2026-09-25T05:00:00Z"),
        "last_run_status": "success",
        "consecutive_errors": 0,
        "latest_success_outage_count": 25,
        "previous_success_outage_count": 30,
    }


def test_pipeline_health_is_good_when_fresh():
    result = assess_pipeline_health(base_health_row(), now=NOW)

    assert result["overall_status"] == "good"
    assert result["collection_status"] == "good"
    assert result["analytics_status"] == "good"
    assert result["heavy_status"] == "good"
    assert result["alerts"] == []


def test_pipeline_health_warns_on_large_snapshot_drop_without_calling_zero_invalid():
    row = base_health_row()
    row["latest_success_outage_count"] = 2
    row["previous_success_outage_count"] = 40

    result = assess_pipeline_health(row, now=NOW)

    assert result["overall_status"] == "warning"
    assert any(alert.code == "outage_count_drop" for alert in result["alerts"])


def test_zero_outage_snapshot_is_only_an_anomaly_warning():
    row = base_health_row()
    row["latest_success_outage_count"] = 0
    row["previous_success_outage_count"] = 40

    result = assess_pipeline_health(row, now=NOW)

    assert result["overall_status"] == "warning"
    assert not any(alert.level == "critical" for alert in result["alerts"])
    assert any(alert.code == "outage_count_drop" for alert in result["alerts"])


def test_pipeline_health_is_critical_when_collection_or_incremental_refresh_is_stale():
    row = base_health_row()
    row["latest_success_captured_at"] = pd.Timestamp("2026-09-25T22:00:00Z")
    row["incremental_refreshed_at"] = pd.Timestamp("2026-09-25T22:00:00Z")

    result = assess_pipeline_health(row, now=NOW)

    assert result["overall_status"] == "critical"
    assert result["collection_status"] == "critical"
    assert result["analytics_status"] == "critical"


def test_outage_change_pct_handles_zero_previous_snapshot():
    assert outage_change_pct(10, 0) is None
    assert outage_change_pct(20, 10) == 100.0


def test_operational_workflows_are_serialized_and_check_health():
    hourly = (ROOT / ".github" / "workflows" / "hydro.yml").read_text(encoding="utf-8")
    maintenance = (ROOT / ".github" / "workflows" / "hydro_maintenance.yml").read_text(encoding="utf-8")

    for workflow in (hourly, maintenance):
        assert "permissions:\n  contents: read" in workflow
        assert "group: hydro-production" in workflow
        assert "cancel-in-progress: false" in workflow
        assert "Check pipeline health" in workflow
        assert "python scripts/check_pipeline_health.py" in workflow
        assert "timeout-minutes:" in workflow


def test_pipeline_health_sql_and_incremental_refresh_marker_are_versioned():
    sql_dir = ROOT / "sql" / "postgres"
    health_sql = (sql_dir / "pipeline_health_snapshot.sql").read_text(encoding="utf-8")
    marker_sql = (sql_dir / "mark_incremental_refresh_complete.sql").read_text(encoding="utf-8")
    refresh = (ROOT / "scripts" / "refresh_supabase_analytics.py").read_text(encoding="utf-8")

    assert "collection_runs" in health_sql
    assert "incremental_analytics" in health_sql
    assert "heavy_analytics" in health_sql
    assert "app_active_outages" in health_sql
    assert "'incremental_analytics'" in marker_sql
    assert "mark_incremental_refresh_complete(connection)" in refresh


def test_dashboard_has_pipeline_health_page_and_global_cache_clear():
    app = (ROOT / "dashboard" / "streamlit_app.py").read_text(encoding="utf-8")
    access = (ROOT / "dashboard" / "data_access.py").read_text(encoding="utf-8")

    assert '"Santé du pipeline"' in app
    assert "load_supabase_pipeline_health" in app
    assert "clear_dashboard_caches()" in app
    assert "def clear_dashboard_caches" in access
    assert "load_supabase_pipeline_health" in access
