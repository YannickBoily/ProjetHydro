SET statement_timeout = '120s';

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
