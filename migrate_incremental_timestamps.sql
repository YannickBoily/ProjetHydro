DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_latest_outages'
          AND column_name = 'start_time'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_latest_outages
        ALTER COLUMN start_time TYPE TIMESTAMPTZ
        USING start_time AT TIME ZONE 'America/Toronto';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_latest_outages'
          AND column_name = 'estimated_restore'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_latest_outages
        ALTER COLUMN estimated_restore TYPE TIMESTAMPTZ
        USING estimated_restore AT TIME ZONE 'America/Toronto';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_latest_outages'
          AND column_name = 'known_cause_last_seen_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_latest_outages
        ALTER COLUMN known_cause_last_seen_at TYPE TIMESTAMPTZ
        USING known_cause_last_seen_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_latest_outages'
          AND column_name = 'latest_row_captured_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_latest_outages
        ALTER COLUMN latest_row_captured_at TYPE TIMESTAMPTZ
        USING latest_row_captured_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_latest_outages'
          AND column_name = 'first_capture_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_latest_outages
        ALTER COLUMN first_capture_at TYPE TIMESTAMPTZ
        USING first_capture_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_latest_outages'
          AND column_name = 'last_capture_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_latest_outages
        ALTER COLUMN last_capture_at TYPE TIMESTAMPTZ
        USING last_capture_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_active_outages'
          AND column_name = 'start_time'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_active_outages
        ALTER COLUMN start_time TYPE TIMESTAMPTZ
        USING start_time AT TIME ZONE 'America/Toronto';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_active_outages'
          AND column_name = 'estimated_restore'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_active_outages
        ALTER COLUMN estimated_restore TYPE TIMESTAMPTZ
        USING estimated_restore AT TIME ZONE 'America/Toronto';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_active_outages'
          AND column_name = 'known_cause_last_seen_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_active_outages
        ALTER COLUMN known_cause_last_seen_at TYPE TIMESTAMPTZ
        USING known_cause_last_seen_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_active_outages'
          AND column_name = 'latest_row_captured_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_active_outages
        ALTER COLUMN latest_row_captured_at TYPE TIMESTAMPTZ
        USING latest_row_captured_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_active_outages'
          AND column_name = 'first_capture_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_active_outages
        ALTER COLUMN first_capture_at TYPE TIMESTAMPTZ
        USING first_capture_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_active_outages'
          AND column_name = 'last_capture_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_active_outages
        ALTER COLUMN last_capture_at TYPE TIMESTAMPTZ
        USING last_capture_at AT TIME ZONE 'UTC';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'app_active_outages'
          AND column_name = 'active_capture_at'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE app_active_outages
        ALTER COLUMN active_capture_at TYPE TIMESTAMPTZ
        USING active_capture_at AT TIME ZONE 'UTC';
    END IF;
END $$;
