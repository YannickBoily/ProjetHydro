SET statement_timeout = '120s';

CREATE TABLE IF NOT EXISTS app_latest_outages (
    outage_id TEXT NOT NULL,
    customers_affected INTEGER,
    start_time TIMESTAMPTZ,
    estimated_restore TIMESTAMPTZ,
    status_code TEXT,
    status TEXT,
    latest_raw_cause_code DOUBLE PRECISION,
    latest_raw_cause_label TEXT,
    analysis_cause_code DOUBLE PRECISION,
    analysis_cause_label TEXT,
    has_known_cause BOOLEAN,
    known_cause_last_seen_at TIMESTAMPTZ,
    municipality_id INTEGER,
    municipality_label TEXT,
    municipality_name TEXT,
    municipality_full_name TEXT,
    mrc_name TEXT,
    region_name TEXT,
    is_geocoded BOOLEAN,
    latest_row_captured_at TIMESTAMPTZ,
    first_capture_at TIMESTAMPTZ,
    last_capture_at TIMESTAMPTZ,
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
ADD COLUMN IF NOT EXISTS active_capture_at TIMESTAMPTZ;

ALTER TABLE app_active_outages
ADD COLUMN IF NOT EXISTS outage_age_hours_at_capture DOUBLE PRECISION;

ALTER TABLE app_active_outages
ADD COLUMN IF NOT EXISTS restore_eta_hours_at_capture DOUBLE PRECISION;

CREATE UNIQUE INDEX IF NOT EXISTS idx_app_active_outages_outage_id
ON app_active_outages (outage_id);

CREATE INDEX IF NOT EXISTS idx_app_active_outages_customers
ON app_active_outages (customers_affected DESC);
