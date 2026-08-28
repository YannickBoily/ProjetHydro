import os

import psycopg2


DEFAULT_HEAVY_REFRESH_HOURS = 24


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
        """
        CREATE TABLE IF NOT EXISTS app_refresh_state (
            refresh_group TEXT PRIMARY KEY,
            last_refreshed_at TIMESTAMPTZ NOT NULL
        );

        ALTER TABLE app_refresh_state ENABLE ROW LEVEL SECURITY;
        REVOKE ALL ON TABLE app_refresh_state FROM anon, authenticated;
        """,
    )


def heavy_refresh_is_due(connection) -> bool:
    if force_heavy_refresh():
        print("Heavy analytics refresh forced by environment.")
        return True

    refresh_hours = get_heavy_refresh_hours()

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                to_regclass('public.app_daily_summary') IS NULL
                OR to_regclass('public.app_data_quality_report') IS NULL
                OR last_refreshed_at IS NULL
                OR last_refreshed_at
                    <= NOW() - (%s * INTERVAL '1 hour')
            FROM (
                SELECT (
                    SELECT last_refreshed_at
                    FROM app_refresh_state
                    WHERE refresh_group = 'heavy_analytics'
                ) AS last_refreshed_at
            ) state;
            """,
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
            """
            INSERT INTO app_refresh_state (
                refresh_group,
                last_refreshed_at
            )
            VALUES (
                'heavy_analytics',
                NOW()
            )
            ON CONFLICT (refresh_group)
            DO UPDATE SET
                last_refreshed_at = EXCLUDED.last_refreshed_at;
            """
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
        """
        SET statement_timeout = '120s';

        CREATE TABLE IF NOT EXISTS app_latest_outages (
            outage_id TEXT NOT NULL,
            customers_affected INTEGER,
            start_time TIMESTAMP,
            estimated_restore TIMESTAMP,
            status_code TEXT,
            status TEXT,
            latest_raw_cause_code DOUBLE PRECISION,
            latest_raw_cause_label TEXT,
            analysis_cause_code DOUBLE PRECISION,
            analysis_cause_label TEXT,
            has_known_cause BOOLEAN,
            known_cause_last_seen_at TIMESTAMP,
            municipality_id INTEGER,
            municipality_label TEXT,
            municipality_name TEXT,
            municipality_full_name TEXT,
            mrc_name TEXT,
            region_name TEXT,
            is_geocoded BOOLEAN,
            latest_row_captured_at TIMESTAMP,
            first_capture_at TIMESTAMP,
            last_capture_at TIMESTAMP,
            capture_count BIGINT,
            observed_duration_hours DOUBLE PRECISION,
            outage_age_hours_at_latest_capture DOUBLE PRECISION,
            restore_eta_hours_at_latest_capture DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            lat DOUBLE PRECISION,
            is_major_outage BOOLEAN
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_app_latest_outages_outage_id
        ON app_latest_outages (outage_id);

        CREATE INDEX IF NOT EXISTS idx_app_latest_outages_sort
        ON app_latest_outages (
            last_capture_at DESC,
            customers_affected DESC
        );

        CREATE INDEX IF NOT EXISTS idx_app_latest_outages_first_capture
        ON app_latest_outages (first_capture_at DESC);

        CREATE TABLE IF NOT EXISTS app_active_outages (
            LIKE app_latest_outages INCLUDING DEFAULTS
        );

        ALTER TABLE app_active_outages
        ADD COLUMN IF NOT EXISTS active_capture_at TIMESTAMP;

        ALTER TABLE app_active_outages
        ADD COLUMN IF NOT EXISTS outage_age_hours_at_capture DOUBLE PRECISION;

        ALTER TABLE app_active_outages
        ADD COLUMN IF NOT EXISTS restore_eta_hours_at_capture DOUBLE PRECISION;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_app_active_outages_outage_id
        ON app_active_outages (outage_id);

        CREATE INDEX IF NOT EXISTS idx_app_active_outages_customers
        ON app_active_outages (customers_affected DESC);

        """,
    )


def latest_table_needs_bootstrap(connection) -> bool:
    if force_latest_rebuild():
        print("Full app_latest_outages rebuild forced by environment.")
        return True

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT NOT EXISTS (
                SELECT 1
                FROM app_latest_outages
                LIMIT 1
            );
            """
        )
        return bool(cursor.fetchone()[0])


def prepare_affected_outage_ids(connection, bootstrap: bool) -> int:
    """Build a tiny temp table containing only outage IDs to recompute."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _affected_outage_ids (
                outage_id TEXT PRIMARY KEY
            ) ON COMMIT PRESERVE ROWS;

            TRUNCATE TABLE _affected_outage_ids;
            """
        )

        if bootstrap:
            cursor.execute(
                """
                INSERT INTO _affected_outage_ids (outage_id)
                SELECT DISTINCT outage_id
                FROM raw_outage_snapshots
                WHERE outage_id IS NOT NULL;
                """
            )
        else:
            cursor.execute(
                """
                WITH latest_capture AS (
                    SELECT captured_at AS max_captured_at
                    FROM raw_outage_snapshots
                    WHERE captured_at IS NOT NULL
                    ORDER BY captured_at DESC
                    LIMIT 1
                )
                INSERT INTO _affected_outage_ids (outage_id)
                SELECT DISTINCT r.outage_id
                FROM raw_outage_snapshots r
                CROSS JOIN latest_capture l
                WHERE r.outage_id IS NOT NULL
                  AND r.captured_at BETWEEN l.max_captured_at - INTERVAL '5 minutes'
                                        AND l.max_captured_at;
                """
            )

        cursor.execute("SELECT COUNT(*) FROM _affected_outage_ids;")
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
        """
        SET statement_timeout = '120s';

        WITH latest_per_outage AS (
            SELECT DISTINCT ON (r.outage_id)
                r.*
            FROM raw_outage_snapshots r
            INNER JOIN _affected_outage_ids a
                ON r.outage_id = a.outage_id
            WHERE r.captured_at IS NOT NULL
            ORDER BY r.outage_id, r.captured_at DESC
        ),

        capture_stats AS (
            SELECT
                r.outage_id,
                MIN(r.captured_at) AS first_capture_at,
                MAX(r.captured_at) AS last_capture_at,
                COUNT(*) AS capture_count
            FROM raw_outage_snapshots r
            INNER JOIN _affected_outage_ids a
                ON r.outage_id = a.outage_id
            WHERE r.captured_at IS NOT NULL
            GROUP BY r.outage_id
        ),

        known_cause AS (
            SELECT DISTINCT ON (r.outage_id)
                r.outage_id,
                r.cause_code AS known_cause_code,
                r.cause_label AS known_cause_label,
                r.captured_at AS known_cause_last_seen_at
            FROM raw_outage_snapshots r
            INNER JOIN _affected_outage_ids a
                ON r.outage_id = a.outage_id
            WHERE r.cause_label IS NOT NULL
              AND TRIM(r.cause_label) <> ''
              AND LOWER(TRIM(r.cause_label)) <> 'unknown'
            ORDER BY r.outage_id, r.captured_at DESC
        )

        INSERT INTO app_latest_outages (
            outage_id,
            customers_affected,
            start_time,
            estimated_restore,
            status_code,
            status,
            latest_raw_cause_code,
            latest_raw_cause_label,
            analysis_cause_code,
            analysis_cause_label,
            has_known_cause,
            known_cause_last_seen_at,
            municipality_id,
            municipality_label,
            municipality_name,
            municipality_full_name,
            mrc_name,
            region_name,
            is_geocoded,
            latest_row_captured_at,
            first_capture_at,
            last_capture_at,
            capture_count,
            observed_duration_hours,
            outage_age_hours_at_latest_capture,
            restore_eta_hours_at_latest_capture,
            lon,
            lat,
            is_major_outage
        )
        SELECT
            r.outage_id,
            r.customers_affected,
            r.start_time,
            r.estimated_restore,
            r.status_code,
            r.status,
            r.cause_code AS latest_raw_cause_code,
            r.cause_label AS latest_raw_cause_label,
            COALESCE(k.known_cause_code, r.cause_code) AS analysis_cause_code,
            COALESCE(k.known_cause_label, r.cause_label, 'unknown') AS analysis_cause_label,
            (k.known_cause_label IS NOT NULL) AS has_known_cause,
            k.known_cause_last_seen_at,
            r.municipality_id,
            COALESCE(
                m.municipality_label,
                'Municipalité ' || CAST(r.municipality_id AS TEXT)
            ) AS municipality_label,
            m.municipality_name,
            m.municipality_full_name,
            m.mrc_name,
            m.region_name,
            m.is_geocoded,
            r.captured_at AS latest_row_captured_at,
            s.first_capture_at,
            s.last_capture_at,
            s.capture_count,
            EXTRACT(EPOCH FROM (s.last_capture_at - s.first_capture_at)) / 3600.0,
            EXTRACT(EPOCH FROM (r.captured_at - r.start_time)) / 3600.0,
            EXTRACT(EPOCH FROM (r.estimated_restore - r.captured_at)) / 3600.0,
            r.lon,
            r.lat,
            (r.customers_affected >= 1000) AS is_major_outage
        FROM latest_per_outage r
        LEFT JOIN capture_stats s
            ON r.outage_id = s.outage_id
        LEFT JOIN known_cause k
            ON r.outage_id = k.outage_id
        LEFT JOIN dim_municipalities m
            ON r.municipality_id = m.municipality_id

        ON CONFLICT (outage_id)
        DO UPDATE SET
            customers_affected = EXCLUDED.customers_affected,
            start_time = EXCLUDED.start_time,
            estimated_restore = EXCLUDED.estimated_restore,
            status_code = EXCLUDED.status_code,
            status = EXCLUDED.status,
            latest_raw_cause_code = EXCLUDED.latest_raw_cause_code,
            latest_raw_cause_label = EXCLUDED.latest_raw_cause_label,
            analysis_cause_code = EXCLUDED.analysis_cause_code,
            analysis_cause_label = EXCLUDED.analysis_cause_label,
            has_known_cause = EXCLUDED.has_known_cause,
            known_cause_last_seen_at = EXCLUDED.known_cause_last_seen_at,
            municipality_id = EXCLUDED.municipality_id,
            municipality_label = EXCLUDED.municipality_label,
            municipality_name = EXCLUDED.municipality_name,
            municipality_full_name = EXCLUDED.municipality_full_name,
            mrc_name = EXCLUDED.mrc_name,
            region_name = EXCLUDED.region_name,
            is_geocoded = EXCLUDED.is_geocoded,
            latest_row_captured_at = EXCLUDED.latest_row_captured_at,
            first_capture_at = EXCLUDED.first_capture_at,
            last_capture_at = EXCLUDED.last_capture_at,
            capture_count = EXCLUDED.capture_count,
            observed_duration_hours = EXCLUDED.observed_duration_hours,
            outage_age_hours_at_latest_capture = EXCLUDED.outage_age_hours_at_latest_capture,
            restore_eta_hours_at_latest_capture = EXCLUDED.restore_eta_hours_at_latest_capture,
            lon = EXCLUDED.lon,
            lat = EXCLUDED.lat,
            is_major_outage = EXCLUDED.is_major_outage;
        """,
    )

    return affected_count


def refresh_active_outages(connection) -> None:
    """Refresh only the small active table while preserving its indexes."""
    execute_step(
        connection,
        "refresh app_active_outages",
        """
        SET statement_timeout = '120s';

        TRUNCATE TABLE app_active_outages;

        WITH latest_capture AS (
            SELECT captured_at AS max_captured_at
            FROM raw_outage_snapshots
            WHERE captured_at IS NOT NULL
            ORDER BY captured_at DESC
            LIMIT 1
        ),

        active_ids AS (
            SELECT DISTINCT ON (r.outage_id)
                r.outage_id,
                r.captured_at AS active_capture_at
            FROM raw_outage_snapshots r
            CROSS JOIN latest_capture l
            WHERE r.outage_id IS NOT NULL
              AND r.captured_at BETWEEN l.max_captured_at - INTERVAL '5 minutes'
                                    AND l.max_captured_at
            ORDER BY r.outage_id, r.captured_at DESC
        )

        INSERT INTO app_active_outages (
            outage_id,
            customers_affected,
            start_time,
            estimated_restore,
            status_code,
            status,
            latest_raw_cause_code,
            latest_raw_cause_label,
            analysis_cause_code,
            analysis_cause_label,
            has_known_cause,
            known_cause_last_seen_at,
            municipality_id,
            municipality_label,
            municipality_name,
            municipality_full_name,
            mrc_name,
            region_name,
            is_geocoded,
            latest_row_captured_at,
            first_capture_at,
            last_capture_at,
            capture_count,
            observed_duration_hours,
            outage_age_hours_at_latest_capture,
            restore_eta_hours_at_latest_capture,
            lon,
            lat,
            is_major_outage,
            active_capture_at,
            outage_age_hours_at_capture,
            restore_eta_hours_at_capture
        )
        SELECT
            l.outage_id,
            l.customers_affected,
            l.start_time,
            l.estimated_restore,
            l.status_code,
            l.status,
            l.latest_raw_cause_code,
            l.latest_raw_cause_label,
            l.analysis_cause_code,
            l.analysis_cause_label,
            l.has_known_cause,
            l.known_cause_last_seen_at,
            l.municipality_id,
            l.municipality_label,
            l.municipality_name,
            l.municipality_full_name,
            l.mrc_name,
            l.region_name,
            l.is_geocoded,
            l.latest_row_captured_at,
            l.first_capture_at,
            l.last_capture_at,
            l.capture_count,
            l.observed_duration_hours,
            l.outage_age_hours_at_latest_capture,
            l.restore_eta_hours_at_latest_capture,
            l.lon,
            l.lat,
            l.is_major_outage,
            a.active_capture_at,
            l.outage_age_hours_at_latest_capture,
            l.restore_eta_hours_at_latest_capture
        FROM app_latest_outages l
        INNER JOIN active_ids a
            ON l.outage_id = a.outage_id;
        """,
    )


def print_lightweight_summary(connection, affected_count: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT latest_row_captured_at
            FROM app_latest_outages
            WHERE latest_row_captured_at IS NOT NULL
            ORDER BY latest_row_captured_at DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()
        latest_capture = row[0] if row else None

        cursor.execute(
            """
            SELECT COUNT(*), MAX(active_capture_at)
            FROM app_active_outages;
            """
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
            """
            SET statement_timeout = '120s';

            CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_outage_capture_desc
            ON raw_outage_snapshots (outage_id, captured_at DESC);

            CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_capture_outage_desc
            ON raw_outage_snapshots (captured_at DESC, outage_id);

            CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_known_cause_desc
            ON raw_outage_snapshots (outage_id, captured_at DESC)
            WHERE cause_label IS NOT NULL
              AND TRIM(cause_label) <> ''
              AND LOWER(TRIM(cause_label)) <> 'unknown';
            """,
        )

        ensure_refresh_state_table(connection)
        ensure_incremental_tables(connection)

        bootstrap = latest_table_needs_bootstrap(connection)
        affected_count = refresh_latest_incrementally(
            connection,
            bootstrap=bootstrap,
        )
        refresh_active_outages(connection)

        if heavy_refresh_is_due(connection):
            execute_step(
                connection,
                "refresh app_daily_summary",
                """
                SET statement_timeout = '120s';

                DROP TABLE IF EXISTS app_daily_summary;

                CREATE TABLE app_daily_summary AS
                WITH snapshots AS (
                    SELECT
                        r.*,
                        DATE_TRUNC('minute', r.captured_at) AS capture_batch_minute,
                        CAST(r.captured_at AS DATE) AS capture_date
                    FROM raw_outage_snapshots r
                    WHERE r.captured_at IS NOT NULL
                ),

                capture_summary AS (
                    SELECT
                        capture_batch_minute,
                        capture_date,
                        COUNT(DISTINCT outage_id) AS active_outages_estimate,
                        SUM(customers_affected) AS customers_affected_snapshot,
                        COUNT(DISTINCT municipality_id) AS municipalities_affected_snapshot,
                        SUM(CASE WHEN customers_affected >= 1000 THEN 1 ELSE 0 END) AS major_outages_snapshot
                    FROM snapshots
                    GROUP BY capture_batch_minute, capture_date
                ),

                daily_from_snapshots AS (
                    SELECT
                        capture_date,
                        COUNT(*) AS snapshots_count,
                        MAX(active_outages_estimate) AS max_active_outages_estimate,
                        ROUND(AVG(active_outages_estimate), 2) AS avg_active_outages_estimate,
                        MAX(customers_affected_snapshot) AS max_customers_affected,
                        ROUND(AVG(customers_affected_snapshot), 2) AS avg_customers_affected,
                        MAX(municipalities_affected_snapshot) AS max_municipalities_affected,
                        MAX(major_outages_snapshot) AS max_major_outages
                    FROM capture_summary
                    GROUP BY capture_date
                ),

                first_seen AS (
                    SELECT
                        outage_id,
                        MIN(captured_at) AS first_seen_at
                    FROM raw_outage_snapshots
                    WHERE outage_id IS NOT NULL
                      AND captured_at IS NOT NULL
                    GROUP BY outage_id
                ),

                new_outages AS (
                    SELECT
                        CAST(first_seen_at AS DATE) AS capture_date,
                        COUNT(*) AS new_outages_detected
                    FROM first_seen
                    GROUP BY CAST(first_seen_at AS DATE)
                ),

                observed AS (
                    SELECT
                        capture_date,
                        COUNT(*) AS raw_rows_count,
                        COUNT(DISTINCT outage_id) AS unique_outages_observed,
                        SUM(CASE WHEN LOWER(COALESCE(cause_label, 'unknown')) = 'unknown' THEN 1 ELSE 0 END)
                            AS unknown_cause_rows,
                        COUNT(DISTINCT municipality_id) AS municipalities_observed
                    FROM snapshots
                    GROUP BY capture_date
                )

                SELECT
                    d.capture_date AS date,
                    d.snapshots_count,
                    d.max_active_outages_estimate,
                    d.avg_active_outages_estimate,
                    d.max_customers_affected,
                    d.avg_customers_affected,
                    d.max_municipalities_affected,
                    d.max_major_outages,
                    COALESCE(n.new_outages_detected, 0) AS new_outages_detected,
                    o.raw_rows_count,
                    o.unique_outages_observed,
                    o.unknown_cause_rows,
                    o.municipalities_observed
                FROM daily_from_snapshots d
                LEFT JOIN new_outages n
                    ON d.capture_date = n.capture_date
                LEFT JOIN observed o
                    ON d.capture_date = o.capture_date;

                CREATE INDEX IF NOT EXISTS idx_app_daily_summary_date
                ON app_daily_summary (date);
                """,
            )

            execute_step(
                connection,
                "refresh app_data_quality_report",
                """
                SET statement_timeout = '120s';

                DROP TABLE IF EXISTS app_data_quality_report;

                CREATE TABLE app_data_quality_report AS
                WITH total AS (
                    SELECT COUNT(*) AS total_rows
                    FROM raw_outage_snapshots
                ),

                checks AS (
                    SELECT
                        'missing_outage_id' AS check_name,
                        'critical' AS severity,
                        COUNT(*) AS rows_affected,
                        'Rows where outage_id is missing.' AS description
                    FROM raw_outage_snapshots
                    WHERE outage_id IS NULL OR TRIM(outage_id) = ''

                    UNION ALL

                    SELECT
                        'missing_captured_at' AS check_name,
                        'critical' AS severity,
                        COUNT(*) AS rows_affected,
                        'Rows where captured_at is missing or invalid.' AS description
                    FROM raw_outage_snapshots
                    WHERE captured_at IS NULL

                    UNION ALL

                    SELECT
                        'negative_customers_affected' AS check_name,
                        'critical' AS severity,
                        COUNT(*) AS rows_affected,
                        'Rows where customers_affected is negative.' AS description
                    FROM raw_outage_snapshots
                    WHERE customers_affected < 0

                    UNION ALL

                    SELECT
                        'invalid_coordinates' AS check_name,
                        'warning' AS severity,
                        COUNT(*) AS rows_affected,
                        'Rows with coordinates outside approximate Quebec bounds.' AS description
                    FROM raw_outage_snapshots
                    WHERE lon IS NULL
                       OR lat IS NULL
                       OR lon < -80
                       OR lon > -57
                       OR lat < 44
                       OR lat > 63

                    UNION ALL

                    SELECT
                        'estimated_restore_before_start_time' AS check_name,
                        'warning' AS severity,
                        COUNT(*) AS rows_affected,
                        'Rows where estimated_restore is before start_time.' AS description
                    FROM raw_outage_snapshots
                    WHERE estimated_restore IS NOT NULL
                      AND start_time IS NOT NULL
                      AND estimated_restore < start_time

                    UNION ALL

                    SELECT
                        'captured_at_before_start_time' AS check_name,
                        'warning' AS severity,
                        COUNT(*) AS rows_affected,
                        'Rows where captured_at is before start_time.' AS description
                    FROM raw_outage_snapshots
                    WHERE captured_at IS NOT NULL
                      AND start_time IS NOT NULL
                      AND captured_at < start_time

                    UNION ALL

                    SELECT
                        'duplicate_outage_id_captured_at' AS check_name,
                        'critical' AS severity,
                        COUNT(*) AS rows_affected,
                        'Duplicate records for the same outage_id and captured_at.' AS description
                    FROM (
                        SELECT
                            outage_id,
                            captured_at,
                            COUNT(*) AS duplicate_count
                        FROM raw_outage_snapshots
                        WHERE outage_id IS NOT NULL
                          AND captured_at IS NOT NULL
                        GROUP BY outage_id, captured_at
                        HAVING COUNT(*) > 1
                    ) duplicates

                    UNION ALL

                    SELECT
                        'unknown_cause_rows' AS check_name,
                        'info' AS severity,
                        COUNT(*) AS rows_affected,
                        'Rows where cause_label is unknown.' AS description
                    FROM raw_outage_snapshots
                    WHERE LOWER(COALESCE(cause_label, 'unknown')) = 'unknown'
                )

                SELECT
                    c.check_name,
                    c.severity,
                    CASE
                        WHEN c.rows_affected = 0 THEN 'pass'
                        WHEN c.severity = 'info' THEN 'info'
                        ELSE 'fail'
                    END AS status,
                    c.rows_affected,
                    t.total_rows,
                    ROUND(c.rows_affected * 100.0 / NULLIF(t.total_rows, 0), 2) AS failed_rate_pct,
                    c.description,
                    NOW() AS created_at
                FROM checks c
                CROSS JOIN total t;

                CREATE INDEX IF NOT EXISTS idx_app_data_quality_report_check
                ON app_data_quality_report (check_name);
                """,
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
            """
            ALTER TABLE app_latest_outages ENABLE ROW LEVEL SECURITY;
            ALTER TABLE app_active_outages ENABLE ROW LEVEL SECURITY;
            ALTER TABLE app_daily_summary ENABLE ROW LEVEL SECURITY;
            ALTER TABLE app_data_quality_report ENABLE ROW LEVEL SECURITY;

            REVOKE ALL ON TABLE app_latest_outages FROM anon, authenticated;
            REVOKE ALL ON TABLE app_active_outages FROM anon, authenticated;
            REVOKE ALL ON TABLE app_daily_summary FROM anon, authenticated;
            REVOKE ALL ON TABLE app_data_quality_report FROM anon, authenticated;
            """,
        )

        print_lightweight_summary(
            connection,
            affected_count=affected_count,
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()
