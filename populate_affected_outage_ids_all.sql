INSERT INTO _affected_outage_ids (outage_id)
SELECT DISTINCT outage_id
FROM raw_outage_snapshots
WHERE outage_id IS NOT NULL;
