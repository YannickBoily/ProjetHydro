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
