SELECT
    to_regclass('public.app_daily_summary') IS NULL
    OR to_regclass('public.app_data_quality_report') IS NULL
    OR last_refreshed_at IS NULL
    OR last_refreshed_at <= NOW() - (%s * INTERVAL '1 hour')
FROM (
    SELECT (
        SELECT last_refreshed_at
        FROM app_refresh_state
        WHERE refresh_group = 'heavy_analytics'
    ) AS last_refreshed_at
) state;
