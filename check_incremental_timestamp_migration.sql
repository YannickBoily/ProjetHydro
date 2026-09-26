SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name IN ('app_latest_outages', 'app_active_outages')
      AND data_type = 'timestamp without time zone'
      AND column_name IN (
          'start_time',
          'estimated_restore',
          'known_cause_last_seen_at',
          'latest_row_captured_at',
          'first_capture_at',
          'last_capture_at',
          'active_capture_at'
      )
);
