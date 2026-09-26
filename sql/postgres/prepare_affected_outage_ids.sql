CREATE TEMP TABLE IF NOT EXISTS _affected_outage_ids (
    outage_id TEXT PRIMARY KEY
) ON COMMIT PRESERVE ROWS;

TRUNCATE TABLE _affected_outage_ids;
