INSERT INTO app_refresh_state (
    refresh_group,
    last_refreshed_at
)
VALUES (
    'incremental_analytics',
    NOW()
)
ON CONFLICT (refresh_group)
DO UPDATE SET
    last_refreshed_at = EXCLUDED.last_refreshed_at;
