ALTER TABLE app_latest_outages ENABLE ROW LEVEL SECURITY;
ALTER TABLE app_active_outages ENABLE ROW LEVEL SECURITY;
ALTER TABLE app_daily_summary ENABLE ROW LEVEL SECURITY;
ALTER TABLE app_data_quality_report ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE app_latest_outages FROM anon, authenticated;
REVOKE ALL ON TABLE app_active_outages FROM anon, authenticated;
REVOKE ALL ON TABLE app_daily_summary FROM anon, authenticated;
REVOKE ALL ON TABLE app_data_quality_report FROM anon, authenticated;
