WITH latest_successful_snapshot AS (
    SELECT snapshot_id
    FROM collection_runs
    WHERE status = 'success'
    ORDER BY captured_at DESC
    LIMIT 1
)
INSERT INTO _affected_outage_ids (outage_id)
SELECT DISTINCT r.outage_id
FROM raw_outage_snapshots r
INNER JOIN latest_successful_snapshot s
    ON r.snapshot_id = s.snapshot_id
WHERE r.outage_id IS NOT NULL;
