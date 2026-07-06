import os

import psycopg2


def connect():
    database_url = os.environ.get("SUPABASE_DB_URL")
    database_hostaddr = os.environ.get("SUPABASE_DB_HOSTADDR")

    if not database_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable.")

    connection_kwargs = {}

    if database_hostaddr:
        connection_kwargs["hostaddr"] = database_hostaddr

    return psycopg2.connect(database_url, **connection_kwargs)


def execute_step(connection, name: str, sql: str) -> None:
    print(f"Running: {name}")

    with connection.cursor() as cursor:
        cursor.execute(sql)

    connection.commit()
    print(f"Done: {name}")


def main() -> None:
    connection = connect()

    try:
        execute_step(
            connection,
            "create performance indexes",
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

            CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_municipality_capture
            ON raw_outage_snapshots (municipality_id, captured_at DESC);
            """,
        )

        execute_step(
            connection,
            "refresh app_latest_outages",
            """
            SET statement_timeout = '120s';

            DROP TABLE IF EXISTS app_latest_outages;

            CREATE TABLE app_latest_outages AS
            WITH latest_per_outage AS (
                SELECT DISTINCT ON (r.outage_id)
                    r.*
                FROM raw_outage_snapshots r
                WHERE r.outage_id IS NOT NULL
                  AND r.captured_at IS NOT NULL
                ORDER BY r.outage_id, r.captured_at DESC
            ),

            capture_stats AS (
                SELECT
                    outage_id,
                    MIN(captured_at) AS first_capture_at,
                    MAX(captured_at) AS last_capture_at,
                    COUNT(*) AS capture_count
                FROM raw_outage_snapshots
                WHERE outage_id IS NOT NULL
                  AND captured_at IS NOT NULL
                GROUP BY outage_id
            ),

            known_cause AS (
                SELECT DISTINCT ON (outage_id)
                    outage_id,
                    cause_code AS known_cause_code,
                    cause_label AS known_cause_label,
                    captured_at AS known_cause_last_seen_at
                FROM raw_outage_snapshots
                WHERE outage_id IS NOT NULL
                  AND cause_label IS NOT NULL
                  AND TRIM(cause_label) <> ''
                  AND LOWER(TRIM(cause_label)) <> 'unknown'
                ORDER BY outage_id, captured_at DESC
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

                CASE
                    WHEN k.known_cause_label IS NOT NULL THEN TRUE
                    ELSE FALSE
                END AS has_known_cause,

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

                EXTRACT(EPOCH FROM (s.last_capture_at - s.first_capture_at)) / 3600.0
                    AS observed_duration_hours,
                EXTRACT(EPOCH FROM (r.captured_at - r.start_time)) / 3600.0
                    AS outage_age_hours_at_latest_capture,
                EXTRACT(EPOCH FROM (r.estimated_restore - r.captured_at)) / 3600.0
                    AS restore_eta_hours_at_latest_capture,

                r.lon,
                r.lat,

                CASE
                    WHEN r.customers_affected >= 1000 THEN TRUE
                    ELSE FALSE
                END AS is_major_outage
            FROM latest_per_outage r
            LEFT JOIN capture_stats s
                ON r.outage_id = s.outage_id
            LEFT JOIN known_cause k
                ON r.outage_id = k.outage_id
            LEFT JOIN dim_municipalities m
                ON r.municipality_id = m.municipality_id;

            CREATE INDEX IF NOT EXISTS idx_app_latest_outages_last_capture
            ON app_latest_outages (last_capture_at DESC);

            CREATE INDEX IF NOT EXISTS idx_app_latest_outages_customers
            ON app_latest_outages (customers_affected DESC);

            CREATE INDEX IF NOT EXISTS idx_app_latest_outages_region
            ON app_latest_outages (region_name);
            """,
        )

        execute_step(
            connection,
            "refresh app_active_outages",
            """
            SET statement_timeout = '120s';

            DROP TABLE IF EXISTS app_active_outages;

            CREATE TABLE app_active_outages AS
            WITH latest_capture AS (
                SELECT MAX(captured_at) AS max_captured_at
                FROM raw_outage_snapshots
                WHERE captured_at IS NOT NULL
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

            SELECT
                l.*,
                a.active_capture_at,
                l.outage_age_hours_at_latest_capture AS outage_age_hours_at_capture,
                l.restore_eta_hours_at_latest_capture AS restore_eta_hours_at_capture
            FROM app_latest_outages l
            INNER JOIN active_ids a
                ON l.outage_id = a.outage_id;

            CREATE INDEX IF NOT EXISTS idx_app_active_outages_customers
            ON app_active_outages (customers_affected DESC);

            CREATE INDEX IF NOT EXISTS idx_app_active_outages_region
            ON app_active_outages (region_name);
            """,
        )

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

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*), MAX(latest_row_captured_at) FROM app_latest_outages;")
            latest_count, latest_capture = cursor.fetchone()

            cursor.execute("SELECT COUNT(*), MAX(active_capture_at) FROM app_active_outages;")
            active_count, active_capture = cursor.fetchone()

        print(f"app_latest_outages rows: {latest_count:,}")
        print(f"app_latest_outages latest capture: {latest_capture}")
        print(f"app_active_outages rows: {active_count:,}")
        print(f"app_active_outages latest capture: {active_capture}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
