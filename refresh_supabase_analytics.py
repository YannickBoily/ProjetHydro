import os
from pathlib import Path

import psycopg2


DEFAULT_HEAVY_REFRESH_HOURS = 24
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "postgres"


def load_sql(filename: str) -> str:
    """Charger une requête PostgreSQL versionnée hors du code Python."""
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def get_heavy_refresh_hours() -> int:
    raw_value = os.environ.get(
        "SUPABASE_HEAVY_REFRESH_HOURS",
        str(DEFAULT_HEAVY_REFRESH_HOURS),
    )

    try:
        hours = int(raw_value)
    except (TypeError, ValueError):
        hours = DEFAULT_HEAVY_REFRESH_HOURS

    return max(hours, 1)


def force_heavy_refresh() -> bool:
    value = os.environ.get(
        "SUPABASE_FORCE_HEAVY_REFRESH",
        "",
    )
    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def connect():
    database_url = os.environ.get("SUPABASE_DB_URL")
    database_hostaddr = os.environ.get("SUPABASE_DB_HOSTADDR")

    if not database_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable.")

    connection_kwargs = {
        "sslmode": "require",
        "connect_timeout": 15,
        "application_name": "projethydro_analytics_refresh",
    }

    if database_hostaddr:
        connection_kwargs["hostaddr"] = database_hostaddr

    return psycopg2.connect(database_url, **connection_kwargs)


def execute_step(connection, name: str, sql: str) -> None:
    print(f"Running: {name}")

    with connection.cursor() as cursor:
        cursor.execute(sql)

    connection.commit()
    print(f"Done: {name}")


def ensure_refresh_state_table(connection) -> None:
    execute_step(
        connection,
        "ensure analytics refresh state",
        load_sql("ensure_refresh_state.sql"),
    )


def heavy_refresh_is_due(connection) -> bool:
    if force_heavy_refresh():
        print("Heavy analytics refresh forced by environment.")
        return True

    refresh_hours = get_heavy_refresh_hours()

    with connection.cursor() as cursor:
        cursor.execute(
            load_sql("heavy_refresh_is_due.sql"),
            (refresh_hours,),
        )
        due = bool(cursor.fetchone()[0])

    if due:
        print(
            "Heavy analytics refresh is due "
            f"(interval: {refresh_hours}h)."
        )
    else:
        print(
            "Skipping heavy analytics refresh "
            f"(interval: {refresh_hours}h)."
        )

    return due


def mark_heavy_refresh_complete(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            load_sql("mark_heavy_refresh_complete.sql")
        )

    connection.commit()
    print("Heavy analytics refresh timestamp updated.")



def force_latest_rebuild() -> bool:
    value = os.environ.get(
        "SUPABASE_FORCE_LATEST_REBUILD",
        "",
    )
    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def ensure_incremental_tables(connection) -> None:
    """Create persistent analytical tables and their indexes once."""
    execute_step(
        connection,
        "ensure incremental analytical tables",
        load_sql("ensure_incremental_tables.sql"),
    )


def migrate_incremental_timestamp_columns(connection) -> bool:
    """Convert legacy analytical timestamps to TIMESTAMPTZ once.

    Returns True when a migration was required so the caller can rebuild all
    latest-outage metrics whose durations depend on the corrected timezone.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            load_sql("check_incremental_timestamp_migration.sql")
        )
        needs_migration = bool(cursor.fetchone()[0])

    if not needs_migration:
        return False

    execute_step(
        connection,
        "migrate analytical timestamps to timestamptz",
        load_sql("migrate_incremental_timestamps.sql"),
    )
    return True


def latest_table_needs_bootstrap(connection) -> bool:
    if force_latest_rebuild():
        print("Full app_latest_outages rebuild forced by environment.")
        return True

    with connection.cursor() as cursor:
        cursor.execute(
            load_sql("latest_table_needs_bootstrap.sql")
        )
        return bool(cursor.fetchone()[0])


def prepare_affected_outage_ids(connection, bootstrap: bool) -> int:
    """Build a tiny temp table containing only outage IDs to recompute."""
    with connection.cursor() as cursor:
        cursor.execute(
            load_sql("prepare_affected_outage_ids.sql")
        )

        if bootstrap:
            cursor.execute(
                load_sql("populate_affected_outage_ids_all.sql")
            )
        else:
            cursor.execute(
                load_sql("populate_affected_outage_ids_latest.sql")
            )

        cursor.execute(load_sql("count_affected_outage_ids.sql"))
        affected_count = int(cursor.fetchone()[0])

    connection.commit()
    print(f"Outage IDs selected for latest-table refresh: {affected_count:,}")
    return affected_count


def refresh_latest_incrementally(connection, bootstrap: bool) -> int:
    affected_count = prepare_affected_outage_ids(connection, bootstrap)

    if affected_count == 0:
        print("No outage IDs require an app_latest_outages update.")
        return 0

    execute_step(
        connection,
        "incremental refresh app_latest_outages",
        load_sql("refresh_latest_incremental.sql"),
    )

    return affected_count


def refresh_active_outages(connection) -> None:
    """Refresh only the small active table while preserving its indexes."""
    execute_step(
        connection,
        "refresh app_active_outages",
        load_sql("refresh_active_outages.sql"),
    )


def print_lightweight_summary(connection, affected_count: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            load_sql("summary_latest_capture.sql")
        )
        row = cursor.fetchone()
        latest_capture = row[0] if row else None

        cursor.execute(
            load_sql("summary_active_outages.sql")
        )
        active_count, active_capture = cursor.fetchone()

    print(f"app_latest_outages rows recomputed: {affected_count:,}")
    print(f"app_latest_outages latest capture: {latest_capture}")
    print(f"app_active_outages rows: {active_count:,}")
    print(f"app_active_outages latest capture: {active_capture}")


def main() -> None:
    connection = connect()

    try:
        execute_step(
            connection,
            "ensure performance indexes",
            load_sql("ensure_performance_indexes.sql"),
        )

        ensure_refresh_state_table(connection)
        ensure_incremental_tables(connection)
        timezone_migrated = migrate_incremental_timestamp_columns(connection)

        bootstrap = latest_table_needs_bootstrap(connection) or timezone_migrated
        if timezone_migrated:
            print("Timezone migration detected: rebuilding all latest outage metrics.")
        affected_count = refresh_latest_incrementally(
            connection,
            bootstrap=bootstrap,
        )
        refresh_active_outages(connection)

        if heavy_refresh_is_due(connection):
            execute_step(
                connection,
                "refresh app_daily_summary",
                load_sql("refresh_daily_summary.sql"),
            )

            execute_step(
                connection,
                "refresh app_data_quality_report",
                load_sql("refresh_data_quality_report.sql"),
            )

            mark_heavy_refresh_complete(connection)
        else:
            print(
                "app_daily_summary and app_data_quality_report "
                "were not rebuilt on this run."
            )

        execute_step(
            connection,
            "secure app tables",
            load_sql("secure_app_tables.sql"),
        )

        print_lightweight_summary(
            connection,
            affected_count=affected_count,
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()
