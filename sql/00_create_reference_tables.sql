CREATE OR REPLACE TABLE dim_municipalities AS
SELECT
    CAST(municipality_id AS INTEGER) AS municipality_id,
    CAST(municipality_label AS VARCHAR) AS municipality_label,
    NULLIF(TRIM(CAST(municipality_name AS VARCHAR)), '') AS municipality_name,
    NULLIF(TRIM(CAST(municipality_full_name AS VARCHAR)), '') AS municipality_full_name,
    TRY_CAST(geo_municipality_code AS INTEGER) AS geo_municipality_code,
    NULLIF(TRIM(CAST(municipality_type_code AS VARCHAR)), '') AS municipality_type_code,
    TRY_CAST(mrc_code AS INTEGER) AS mrc_code,
    NULLIF(TRIM(CAST(mrc_name AS VARCHAR)), '') AS mrc_name,
    TRY_CAST(region_code AS INTEGER) AS region_code,
    NULLIF(TRIM(CAST(region_name AS VARCHAR)), '') AS region_name,
    CAST(is_geocoded AS BOOLEAN) AS is_geocoded,
    TRY_CAST(match_rate_pct AS DOUBLE) AS match_rate_pct,
    TRY_CAST(matched_records_count AS INTEGER) AS matched_records_count,
    TRY_CAST(outage_records_count AS INTEGER) AS outage_records_count,
    TRY_CAST(avg_lon AS DOUBLE) AS avg_lon,
    TRY_CAST(avg_lat AS DOUBLE) AS avg_lat,
    TRY_CAST(first_seen_at AS TIMESTAMP) AS first_seen_at,
    TRY_CAST(last_seen_at AS TIMESTAMP) AS last_seen_at
FROM read_csv_auto('data/reference/municipalities.csv', header = true);