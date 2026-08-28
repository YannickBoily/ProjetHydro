-- One-time migration for existing Supabase projects.
-- Run this after one successful execution of the incremental refresh code.
--
-- Goal: keep the indexes used by the current workload while removing indexes
-- that duplicate a left-prefix of another index or are not used by the
-- dashboard queries. Fewer indexes means less WAL and fewer page writes for
-- every new outage snapshot.

-- Create the indexes required by the incremental analytics path first.
CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_outage_capture_desc
ON raw_outage_snapshots (outage_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_capture_outage_desc
ON raw_outage_snapshots (captured_at DESC, outage_id);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_known_cause_desc
ON raw_outage_snapshots (outage_id, captured_at DESC)
WHERE cause_label IS NOT NULL
  AND TRIM(cause_label) <> ''
  AND LOWER(TRIM(cause_label)) <> 'unknown';

-- Redundant for the current application workload:
-- - outage_id is already the first column of the UNIQUE constraint and the
--   outage/capture index.
-- - captured_at is the first column of capture/outage.
-- - municipality_id and lon/lat are not filtered directly by the dashboard.
DROP INDEX IF EXISTS idx_raw_outage_snapshots_outage_id;
DROP INDEX IF EXISTS idx_raw_outage_snapshots_captured_at;
DROP INDEX IF EXISTS idx_raw_outage_snapshots_municipality_id;
DROP INDEX IF EXISTS idx_raw_outage_snapshots_municipality_capture;
DROP INDEX IF EXISTS idx_raw_outage_snapshots_lon_lat;

ANALYZE raw_outage_snapshots;

-- Analytical tables: keep only indexes matching actual dashboard ORDER BYs.
CREATE UNIQUE INDEX IF NOT EXISTS idx_app_latest_outages_outage_id
ON app_latest_outages (outage_id);

CREATE INDEX IF NOT EXISTS idx_app_latest_outages_sort
ON app_latest_outages (last_capture_at DESC, customers_affected DESC);

CREATE INDEX IF NOT EXISTS idx_app_latest_outages_first_capture
ON app_latest_outages (first_capture_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_app_active_outages_outage_id
ON app_active_outages (outage_id);

CREATE INDEX IF NOT EXISTS idx_app_active_outages_customers
ON app_active_outages (customers_affected DESC);

DROP INDEX IF EXISTS idx_app_latest_outages_last_capture;
DROP INDEX IF EXISTS idx_app_latest_outages_customers;
DROP INDEX IF EXISTS idx_app_latest_outages_region;
DROP INDEX IF EXISTS idx_app_active_outages_region;
