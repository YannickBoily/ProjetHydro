"""Operational health check for the Hydro-Québec ingestion pipeline."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard.pipeline_health import assess_pipeline_health


SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "postgres" / "pipeline_health_snapshot.sql"


def connect():
    database_url = os.environ.get("SUPABASE_DB_URL")
    database_hostaddr = os.environ.get("SUPABASE_DB_HOSTADDR")
    if not database_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable.")

    kwargs = {
        "sslmode": "require",
        "connect_timeout": 15,
        "application_name": "projethydro_pipeline_health",
    }
    if database_hostaddr:
        kwargs["hostaddr"] = database_hostaddr
    return psycopg2.connect(database_url, **kwargs)


def fetch_health_snapshot(connection) -> dict:
    sql = SQL_PATH.read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [getattr(description, "name", description[0]) for description in cursor.description]
        row = cursor.fetchone()
    return dict(zip(columns, row)) if row else {}


def format_age(minutes):
    if minutes is None:
        return "n/a"
    if minutes < 120:
        return f"{minutes:.0f} min"
    return f"{minutes / 60:.1f} h"


def append_github_summary(snapshot: dict, assessment: dict) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    status_icon = {"good": "✅", "warning": "⚠️", "critical": "❌"}
    lines = [
        "## Pipeline health",
        "",
        f"**Overall:** {status_icon.get(assessment['overall_status'], 'ℹ️')} {assessment['overall_status']}",
        "",
        "| Signal | Value |",
        "|---|---:|",
        f"| Latest successful snapshot | `{snapshot.get('latest_success_snapshot_id') or 'n/a'}` |",
        f"| Snapshot age | {format_age(assessment['collection_age_minutes'])} |",
        f"| Outages in latest snapshot | {snapshot.get('latest_success_outage_count') or 0} |",
        f"| Incremental analytics age | {format_age(assessment['analytics_age_minutes'])} |",
        f"| Heavy analytics age | {format_age(assessment['heavy_age_minutes'])} |",
        f"| Successful runs (24h) | {snapshot.get('success_runs_24h') or 0} |",
        f"| Error runs (24h) | {snapshot.get('error_runs_24h') or 0} |",
        f"| Active outages | {snapshot.get('active_outages_count') or 0} |",
        "",
    ]

    alerts = assessment["alerts"]
    if alerts:
        lines += ["### Alerts", ""]
        for alert in alerts:
            lines.append(f"- {status_icon.get(alert.level, 'ℹ️')} **{alert.code}** — {alert.message}")
        lines.append("")

    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main() -> None:
    connection = connect()
    try:
        snapshot = fetch_health_snapshot(connection)
    finally:
        connection.close()

    assessment = assess_pipeline_health(snapshot)

    print("=== Pipeline health ===")
    print(f"Overall: {assessment['overall_status']}")
    print(f"Latest successful snapshot: {snapshot.get('latest_success_snapshot_id')}")
    print(f"Collection age: {format_age(assessment['collection_age_minutes'])}")
    print(f"Incremental analytics age: {format_age(assessment['analytics_age_minutes'])}")
    print(f"Heavy analytics age: {format_age(assessment['heavy_age_minutes'])}")
    print(f"Outages in latest snapshot: {snapshot.get('latest_success_outage_count') or 0}")
    print(f"Successful runs / 24h: {snapshot.get('success_runs_24h') or 0}")
    print(f"Error runs / 24h: {snapshot.get('error_runs_24h') or 0}")

    for alert in assessment["alerts"]:
        prefix = "::error::" if alert.level == "critical" else "::warning::"
        print(f"{prefix}{alert.code}: {alert.message}")

    append_github_summary(snapshot, assessment)

    if assessment["overall_status"] == "critical":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
