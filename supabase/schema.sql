SET TIME ZONE 'UTC';

-- ============================================================
-- Supabase / PostgreSQL schema for Hydro-Québec outage project
-- ============================================================

CREATE TABLE IF NOT EXISTS collection_runs (
    snapshot_id TEXT PRIMARY KEY,
    captured_at TIMESTAMPTZ NOT NULL,
    source_version TEXT,
    status TEXT NOT NULL,
    outage_count INTEGER NOT NULL CHECK (outage_count >= 0),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_outage_snapshots (
    snapshot_id TEXT NOT NULL,
    outage_id TEXT NOT NULL,
    customers_affected INTEGER,
    start_time TIMESTAMPTZ,
    estimated_restore TIMESTAMPTZ,
    status_code TEXT,
    status TEXT,
    cause_code DOUBLE PRECISION,
    cause_label TEXT,
    municipality_id INTEGER,
    captured_at TIMESTAMPTZ NOT NULL,
    lon DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT raw_outage_snapshots_unique UNIQUE (outage_id, captured_at)
);

-- Backward-compatible migration for databases created before snapshot IDs.
ALTER TABLE raw_outage_snapshots
ADD COLUMN IF NOT EXISTS snapshot_id TEXT;

UPDATE raw_outage_snapshots
SET snapshot_id = 'legacy:' || to_char(captured_at, 'YYYYMMDD"T"HH24MISS.US')
WHERE snapshot_id IS NULL
  AND captured_at IS NOT NULL;

INSERT INTO collection_runs (
    snapshot_id,
    captured_at,
    source_version,
    status,
    outage_count
)
SELECT
    snapshot_id,
    MIN(captured_at),
    'legacy',
    'success',
    COUNT(*)
FROM raw_outage_snapshots
WHERE snapshot_id IS NOT NULL
GROUP BY snapshot_id
ON CONFLICT (snapshot_id) DO NOTHING;

ALTER TABLE raw_outage_snapshots
ALTER COLUMN snapshot_id SET NOT NULL;

-- Historical timezone migration. Capture timestamps were stored as UTC wall
-- clock values, while Hydro start/restore timestamps were Quebec local wall
-- clock values. PostgreSQL blocks ALTER COLUMN TYPE while dependent views exist,
-- so remove only the known core views when a legacy timestamp column is present.
-- They are recreated later in this file.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND data_type = 'timestamp without time zone'
          AND (table_name, column_name) IN (
              ('collection_runs', 'captured_at'),
              ('raw_outage_snapshots', 'captured_at'),
              ('raw_outage_snapshots', 'start_time'),
              ('raw_outage_snapshots', 'estimated_restore')
          )
    ) THEN
        DROP VIEW IF EXISTS vw_active_outages;
        DROP VIEW IF EXISTS vw_latest_outages;
        DROP VIEW IF EXISTS vw_supabase_load_summary;
    END IF;
END $$;

-- Convert only legacy timestamp-without-zone columns; this block is idempotent.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'collection_runs'
          AND column_name = 'captured_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE collection_runs
        ALTER COLUMN captured_at TYPE TIMESTAMPTZ
        USING captured_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'raw_outage_snapshots'
          AND column_name = 'captured_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE raw_outage_snapshots
        ALTER COLUMN captured_at TYPE TIMESTAMPTZ
        USING captured_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'raw_outage_snapshots'
          AND column_name = 'start_time'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE raw_outage_snapshots
        ALTER COLUMN start_time TYPE TIMESTAMPTZ
        USING start_time AT TIME ZONE 'America/Toronto';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'raw_outage_snapshots'
          AND column_name = 'estimated_restore'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE raw_outage_snapshots
        ALTER COLUMN estimated_restore TYPE TIMESTAMPTZ
        USING estimated_restore AT TIME ZONE 'America/Toronto';
    END IF;
END $$;

-- Index set intentionally kept small to reduce write amplification.
-- The UNIQUE constraint above already provides an index starting with outage_id.
CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_outage_capture_desc
ON raw_outage_snapshots (outage_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_capture_outage_desc
ON raw_outage_snapshots (captured_at DESC, outage_id);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_known_cause_desc
ON raw_outage_snapshots (outage_id, captured_at DESC)
WHERE cause_label IS NOT NULL
  AND TRIM(cause_label) <> ''
  AND LOWER(TRIM(cause_label)) <> 'unknown';

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_snapshot_id
ON raw_outage_snapshots (snapshot_id);

CREATE INDEX IF NOT EXISTS idx_collection_runs_success_capture
ON collection_runs (captured_at DESC)
WHERE status = 'success';


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
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dim_municipalities'
          AND column_name = 'first_seen_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE dim_municipalities
        ALTER COLUMN first_seen_at TYPE TIMESTAMPTZ
        USING first_seen_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dim_municipalities'
          AND column_name = 'last_seen_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE dim_municipalities
        ALTER COLUMN last_seen_at TYPE TIMESTAMPTZ
        USING last_seen_at AT TIME ZONE 'UTC';
    END IF;
END $$;


-- Small helper view to quickly validate the load.
CREATE OR REPLACE VIEW vw_supabase_load_summary AS
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT outage_id) AS unique_outages,
    MIN(captured_at) AS first_capture_at,
    MAX(captured_at) AS latest_raw_capture_at,
    (
        SELECT captured_at
        FROM collection_runs
        WHERE status = 'success'
        ORDER BY captured_at DESC
        LIMIT 1
    ) AS latest_successful_capture_at,
    (
        SELECT outage_count
        FROM collection_runs
        WHERE status = 'success'
        ORDER BY captured_at DESC
        LIMIT 1
    ) AS latest_snapshot_outage_count
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


-- Active outages are exactly the rows from the latest successful snapshot.
-- A successful snapshot may contain zero rows; in that case this view is empty.
CREATE OR REPLACE VIEW vw_active_outages AS
WITH latest_successful_snapshot AS (
    SELECT snapshot_id, captured_at
    FROM collection_runs
    WHERE status = 'success'
    ORDER BY captured_at DESC
    LIMIT 1
),

active_ids AS (
    SELECT DISTINCT r.outage_id
    FROM raw_outage_snapshots r
    INNER JOIN latest_successful_snapshot s
        ON r.snapshot_id = s.snapshot_id
    WHERE r.outage_id IS NOT NULL
)

SELECT
    l.*
FROM vw_latest_outages l
INNER JOIN active_ids a
    ON l.outage_id = a.outage_id;
