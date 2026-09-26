WITH successful_runs AS (
    SELECT
        snapshot_id,
        captured_at,
        source_version,
        outage_count,
        ROW_NUMBER() OVER (ORDER BY captured_at DESC, snapshot_id DESC) AS rn
    FROM collection_runs
    WHERE status = 'success'
),
last_run AS (
    SELECT
        snapshot_id,
        captured_at,
        status,
        outage_count,
        finished_at,
        error_message
    FROM collection_runs
    ORDER BY captured_at DESC, snapshot_id DESC
    LIMIT 1
),
runs_24h AS (
    SELECT
        COUNT(*) FILTER (WHERE status = 'success') AS success_runs_24h,
        COUNT(*) FILTER (WHERE status = 'error') AS error_runs_24h,
        COUNT(*) FILTER (WHERE status = 'pending') AS pending_runs_24h
    FROM collection_runs
    WHERE captured_at >= NOW() - INTERVAL '24 hours'
),
last_success AS (
    SELECT MAX(captured_at) AS captured_at
    FROM collection_runs
    WHERE status = 'success'
),
errors_since_success AS (
    SELECT COUNT(*) AS consecutive_errors
    FROM collection_runs c
    CROSS JOIN last_success s
    WHERE c.status = 'error'
      AND (s.captured_at IS NULL OR c.captured_at > s.captured_at)
),
refresh_state AS (
    SELECT
        MAX(last_refreshed_at) FILTER (
            WHERE refresh_group = 'incremental_analytics'
        ) AS incremental_refreshed_at,
        MAX(last_refreshed_at) FILTER (
            WHERE refresh_group = 'heavy_analytics'
        ) AS heavy_refreshed_at
    FROM app_refresh_state
),
active_state AS (
    SELECT
        COUNT(*) AS active_outages_count,
        MAX(latest_row_captured_at) AS active_capture_at,
        COALESCE(SUM(customers_affected), 0) AS active_customers_affected
    FROM app_active_outages
),
latest_state AS (
    SELECT
        COUNT(*) AS latest_outages_count,
        MAX(last_capture_at) AS latest_table_capture_at
    FROM app_latest_outages
)
SELECT
    NOW() AS observed_at,
    s1.snapshot_id AS latest_success_snapshot_id,
    s1.captured_at AS latest_success_captured_at,
    s1.source_version AS latest_success_source_version,
    s1.outage_count AS latest_success_outage_count,
    s2.outage_count AS previous_success_outage_count,
    lr.snapshot_id AS last_run_snapshot_id,
    lr.captured_at AS last_run_captured_at,
    lr.status AS last_run_status,
    lr.finished_at AS last_run_finished_at,
    lr.error_message AS last_run_error_message,
    r24.success_runs_24h,
    r24.error_runs_24h,
    r24.pending_runs_24h,
    ese.consecutive_errors,
    rs.incremental_refreshed_at,
    rs.heavy_refreshed_at,
    ast.active_outages_count,
    ast.active_capture_at,
    ast.active_customers_affected,
    lst.latest_outages_count,
    lst.latest_table_capture_at
FROM (SELECT 1) seed
LEFT JOIN successful_runs s1 ON s1.rn = 1
LEFT JOIN successful_runs s2 ON s2.rn = 2
LEFT JOIN last_run lr ON TRUE
CROSS JOIN runs_24h r24
CROSS JOIN errors_since_success ese
CROSS JOIN refresh_state rs
CROSS JOIN active_state ast
CROSS JOIN latest_state lst;
