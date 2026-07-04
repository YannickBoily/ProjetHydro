CREATE OR REPLACE TABLE data_quality_report AS
WITH checks AS (
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
    )

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
    check_name,
    severity,
    CASE
        WHEN rows_affected = 0 THEN 'pass'
        WHEN severity = 'info' THEN 'info'
        ELSE 'fail'
    END AS status,
    rows_affected,
    (
        SELECT COUNT(*)
        FROM raw_outage_snapshots
    ) AS total_rows,
    ROUND(
        rows_affected * 100.0 / NULLIF((SELECT COUNT(*) FROM raw_outage_snapshots), 0),
        2
    ) AS failed_rate_pct,
    description,
    CURRENT_TIMESTAMP AS created_at
FROM checks
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        WHEN 'info' THEN 3
        ELSE 4
    END,
    check_name;