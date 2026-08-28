-- Optional manual performance indexes.
-- You can run this in the Supabase SQL Editor if the dashboard or refresh script is slow.

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_outage_capture_desc
ON raw_outage_snapshots (outage_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_capture_outage_desc
ON raw_outage_snapshots (captured_at DESC, outage_id);

CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_known_cause_desc
ON raw_outage_snapshots (outage_id, captured_at DESC)
WHERE cause_label IS NOT NULL
  AND TRIM(cause_label) <> ''
  AND LOWER(TRIM(cause_label)) <> 'unknown';

