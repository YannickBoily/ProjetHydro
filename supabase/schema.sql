-- ============================================================
-- Supabase / PostgreSQL schema for Hydro-Québec outage project
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_outage_snapshots (
    outage_id TEXT NOT NULL,
    customers_affected INTEGER,
    start_time TIMESTAMP,
    estimated_restore TIMESTAMP,
    status_code TEXT,
    status TEXT,
    cause_code DOUBLE PRECISION,
    cause_label TEXT,
    municipality_id INTEGER,
    captured_at TIMESTAMP NOT NULL,
    lon DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT raw_outage_snapshots_unique UNIQUE (outage_id, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_captured_at
ON raw_outage_snapshots (captured_at);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_outage_id
ON raw_outage_snapshots (outage_id);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_municipality_id
ON raw_outage_snapshots (municipality_id);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_lon_lat
ON raw_outage_snapshots (lon, lat);


CREATE TABLE IF NOT EXISTS dim_municipalities (
    municipality_id INTEGER PRIMARY KEY,
    municipality_label TEXT,
    municipality_name TEXT,
    municipality_full_name TEXT,
    geo_municipality_code INTEGER,
    municipality_type_code TEXT,
    mrc_code INTEGER,
    mrc_name TEXT,
    region_code INTEGER,
    region_name TEXT,
    is_geocoded BOOLEAN,
    match_rate_pct DOUBLE PRECISION,
    matched_records_count INTEGER,
    outage_records_count INTEGER,
    avg_lon DOUBLE PRECISION,
    avg_lat DOUBLE PRECISION,
    first_seen_at TIMESTAMP,
    last_seen_at TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- Small helper view to quickly validate the load.
CREATE OR REPLACE VIEW vw_supabase_load_summary AS
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT outage_id) AS unique_outages,
    MIN(captured_at) AS first_capture_at,
    MAX(captured_at) AS latest_capture_at
FROM raw_outage_snapshots;


-- Latest observation per outage, enriched with municipality names.
CREATE OR REPLACE VIEW vw_latest_outages AS
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
),

known_cause_ranked AS (
    SELECT
        outage_id,
        cause_code AS known_cause_code,
        cause_label AS known_cause_label,
        captured_at AS known_cause_last_seen_at,
        ROW_NUMBER() OVER (
            PARTITION BY outage_id
            ORDER BY captured_at DESC
        ) AS cause_row_num
    FROM raw_outage_snapshots
    WHERE outage_id IS NOT NULL
      AND cause_label IS NOT NULL
      AND TRIM(cause_label) <> ''
      AND LOWER(TRIM(cause_label)) <> 'unknown'
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
    EXTRACT(EPOCH FROM (s.last_capture_at - s.first_capture_at)) / 3600 AS observed_duration_hours,
    EXTRACT(EPOCH FROM (r.captured_at - r.start_time)) / 3600 AS outage_age_hours_at_latest_capture,
    EXTRACT(EPOCH FROM (r.estimated_restore - r.captured_at)) / 3600 AS restore_eta_hours_at_latest_capture,
    r.lon,
    r.lat,
    CASE
        WHEN r.customers_affected >= 1000 THEN TRUE
        ELSE FALSE
    END AS is_major_outage
FROM ranked r
LEFT JOIN outage_capture_stats s
    ON r.outage_id = s.outage_id
LEFT JOIN known_cause_ranked k
    ON r.outage_id = k.outage_id
   AND k.cause_row_num = 1
LEFT JOIN dim_municipalities m
    ON r.municipality_id = m.municipality_id
WHERE r.row_num = 1;


-- Active outages based on the latest capture window.
CREATE OR REPLACE VIEW vw_active_outages AS
WITH latest_capture AS (
    SELECT MAX(captured_at) AS max_captured_at
    FROM raw_outage_snapshots
    WHERE captured_at IS NOT NULL
),

latest_capture_window AS (
    SELECT
        max_captured_at - INTERVAL '5 minutes' AS window_start,
        max_captured_at AS window_end
    FROM latest_capture
),

ranked_active AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.outage_id
            ORDER BY r.captured_at DESC
        ) AS row_num
    FROM raw_outage_snapshots r
    CROSS JOIN latest_capture_window w
    WHERE r.captured_at BETWEEN w.window_start AND w.window_end
      AND r.outage_id IS NOT NULL
)

SELECT
    l.*
FROM vw_latest_outages l
INNER JOIN ranked_active a
    ON l.outage_id = a.outage_id
WHERE a.row_num = 1;
