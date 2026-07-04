CREATE OR REPLACE TABLE daily_summary AS
WITH capture_summary AS (
    SELECT
        captured_at,
        CAST(captured_at AS DATE) AS capture_date,
        COUNT(DISTINCT outage_id) AS active_outages_estimate,
        SUM(customers_affected) AS customers_affected_snapshot,
        COUNT(DISTINCT municipality_id) AS municipalities_affected_snapshot,
        SUM(
            CASE
                WHEN customers_affected >= 1000 THEN 1
                ELSE 0
            END
        ) AS major_outages_snapshot
    FROM raw_outage_snapshots
    WHERE captured_at IS NOT NULL
    GROUP BY captured_at
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
        CAST(captured_at AS DATE) AS capture_date,
        COUNT(*) AS raw_rows_count,
        COUNT(DISTINCT outage_id) AS unique_outages_observed,
        SUM(
            CASE
                WHEN LOWER(COALESCE(cause_label, 'unknown')) = 'unknown' THEN 1
                ELSE 0
            END
        ) AS unknown_cause_rows,
        COUNT(DISTINCT municipality_id) AS municipalities_observed
    FROM raw_outage_snapshots
    WHERE captured_at IS NOT NULL
    GROUP BY CAST(captured_at AS DATE)
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
    ON d.capture_date = o.capture_date
ORDER BY d.capture_date;