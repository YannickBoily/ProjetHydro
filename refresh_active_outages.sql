SET statement_timeout = '120s';

TRUNCATE TABLE app_active_outages;

WITH latest_successful_snapshot AS (
    SELECT snapshot_id, captured_at
    FROM collection_runs
    WHERE status = 'success'
    ORDER BY captured_at DESC
    LIMIT 1
),

active_ids AS (
    SELECT DISTINCT ON (r.outage_id)
        r.outage_id,
        s.captured_at AS active_capture_at
    FROM raw_outage_snapshots r
    INNER JOIN latest_successful_snapshot s
        ON r.snapshot_id = s.snapshot_id
    WHERE r.outage_id IS NOT NULL
    ORDER BY r.outage_id
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
