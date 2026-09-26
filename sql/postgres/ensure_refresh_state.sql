CREATE TABLE IF NOT EXISTS app_refresh_state (
    refresh_group TEXT PRIMARY KEY,
    last_refreshed_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE app_refresh_state ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE app_refresh_state FROM anon, authenticated;
