SELECT NOT EXISTS (
    SELECT 1
    FROM app_latest_outages
    LIMIT 1
);
