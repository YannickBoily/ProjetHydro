CREATE OR REPLACE TABLE latest_outages AS
WITH outage_capture_stats AS (
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

ranked AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.outage_id
            ORDER BY r.captured_at DESC
        ) AS row_num
    FROM raw_outage_snapshots r
    WHERE r.outage_id IS NOT NULL
      AND r.captured_at IS NOT NULL
)

SELECT
    r.outage_id,
    r.customers_affected,
    r.start_time,
    r.estimated_restore,
    r.status_code,
    r.status,
    r.cause_code,
    r.cause_label,
    r.municipality_id,
    r.captured_at AS latest_row_captured_at,
    s.first_capture_at,
    s.last_capture_at,
    s.capture_count,
    DATE_DIFF('hour', s.first_capture_at, s.last_capture_at) AS observed_duration_hours,
    DATE_DIFF('hour', r.start_time, r.captured_at) AS outage_age_hours_at_latest_capture,
    DATE_DIFF('hour', r.captured_at, r.estimated_restore) AS restore_eta_hours_at_latest_capture,
    r.lon,
    r.lat,
    CASE
        WHEN r.customers_affected >= 1000 THEN TRUE
        ELSE FALSE
    END AS is_major_outage
FROM ranked r
LEFT JOIN outage_capture_stats s
    ON r.outage_id = s.outage_id
WHERE r.row_num = 1
ORDER BY s.last_capture_at DESC, r.customers_affected DESC;