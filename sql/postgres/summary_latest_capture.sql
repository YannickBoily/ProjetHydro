SELECT latest_row_captured_at
FROM app_latest_outages
WHERE latest_row_captured_at IS NOT NULL
ORDER BY latest_row_captured_at DESC
LIMIT 1;
