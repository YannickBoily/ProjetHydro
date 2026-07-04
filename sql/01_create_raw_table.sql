CREATE OR REPLACE TABLE raw_outage_snapshots AS
SELECT
    CAST(outage_id AS VARCHAR) AS outage_id,
    CAST(customers_affected AS INTEGER) AS customers_affected,
    TRY_CAST(start_time AS TIMESTAMP) AS start_time,
    TRY_CAST(estimated_restore AS TIMESTAMP) AS estimated_restore,
    CAST(status_code AS VARCHAR) AS status_code,
    CAST(status AS VARCHAR) AS status,
    TRY_CAST(cause_code AS DOUBLE) AS cause_code,
    CAST(cause_label AS VARCHAR) AS cause_label,
    CAST(municipality_id AS INTEGER) AS municipality_id,
    TRY_CAST(captured_at AS TIMESTAMP) AS captured_at,
    CAST(lon AS DOUBLE) AS lon,
    CAST(lat AS DOUBLE) AS lat
FROM read_csv_auto('data/raw/hydroquebec_history.csv', header = true);