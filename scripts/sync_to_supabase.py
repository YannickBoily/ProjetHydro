from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

try:
    from scripts.time_utils import normalize_capture_series, normalize_hydro_local_series
except ModuleNotFoundError:  # direct execution: python scripts/sync_to_supabase.py
    from time_utils import normalize_capture_series, normalize_hydro_local_series


# =============================================================================
# Configuration
# =============================================================================

RAW_FILE = Path("data/raw/hydroquebec_history.csv")
CURRENT_SNAPSHOT_FILE = Path("data/raw/current_snapshot.csv")
CURRENT_SNAPSHOT_META_FILE = Path("data/raw/current_snapshot_meta.json")
MUNICIPALITIES_FILE = Path("data/reference/municipalities.csv")

RAW_BATCH_SIZE = 500
MUNICIPALITY_BATCH_SIZE = 250
DEFAULT_LOOKBACK_HOURS = 2

FILE_RAW_COLUMNS = [
    "outage_id",
    "customers_affected",
    "start_time",
    "estimated_restore",
    "status_code",
    "status",
    "cause_code",
    "cause_label",
    "municipality_id",
    "captured_at",
    "lon",
    "lat",
]

# snapshot_id is persisted in PostgreSQL but intentionally not added to the
# long-lived local history CSV. Legacy history receives a deterministic ID
# derived from captured_at during synchronization.
RAW_COLUMNS = ["snapshot_id", *FILE_RAW_COLUMNS]

MUNICIPALITY_COLUMNS = [
    "municipality_id",
    "municipality_label",
    "municipality_name",
    "municipality_full_name",
    "geo_municipality_code",
    "municipality_type_code",
    "mrc_code",
    "mrc_name",
    "region_code",
    "region_name",
    "is_geocoded",
    "match_rate_pct",
    "matched_records_count",
    "outage_records_count",
    "avg_lon",
    "avg_lat",
    "first_seen_at",
    "last_seen_at",
]


# =============================================================================
# Helpers
# =============================================================================

def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def clean_value(value: Any) -> Any:
    """Convert pandas values to database-friendly Python values."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def prepare_dataframe(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Ensure all expected columns exist and are ordered."""
    df = df.copy()
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def dataframe_to_records(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """Convert a DataFrame to psycopg2-compatible tuples."""
    records: list[tuple] = []
    for row in df[columns].itertuples(index=False, name=None):
        records.append(tuple(clean_value(value) for value in row))
    return records


def get_sync_lookback_hours() -> int:
    raw_value = os.environ.get(
        "SUPABASE_SYNC_LOOKBACK_HOURS",
        str(DEFAULT_LOOKBACK_HOURS),
    )
    try:
        hours = int(raw_value)
    except (TypeError, ValueError):
        hours = DEFAULT_LOOKBACK_HOURS
    return max(hours, 0)


def legacy_snapshot_id(value: Any) -> str:
    """Return a deterministic ID compatible with the SQL migration."""
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        raise ValueError("Cannot build snapshot_id from an invalid captured_at value.")
    return "legacy:" + pd.Timestamp(timestamp).strftime("%Y%m%dT%H%M%S.%f")


def normalize_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the common outage fields read from a CSV."""
    df = df.copy()

    # Legacy CSV semantics are mixed by design: captures were written as UTC
    # wall-clock values, while Hydro start/restore values were Quebec local
    # wall-clock values. New snapshots include explicit offsets. Normalize both
    # forms to aware UTC before inserting into TIMESTAMPTZ columns.
    if "captured_at" in df.columns:
        df["captured_at"] = normalize_capture_series(df["captured_at"])
    for column in ["start_time", "estimated_restore"]:
        if column in df.columns:
            df[column] = normalize_hydro_local_series(df[column])

    for column in [
        "customers_affected",
        "cause_code",
        "municipality_id",
        "lon",
        "lat",
    ]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df = prepare_dataframe(df, FILE_RAW_COLUMNS)
    df = df.dropna(subset=["outage_id", "captured_at"])
    df = df.drop_duplicates(subset=["outage_id", "captured_at"], keep="last")
    return df


def metadata_to_run(metadata: dict[str, Any]) -> dict[str, Any]:
    required = {"snapshot_id", "captured_at", "outage_count"}
    missing = required.difference(metadata)
    if missing:
        raise ValueError(
            "Snapshot metadata is missing required fields: " + ", ".join(sorted(missing))
        )

    captured_at = pd.to_datetime(metadata["captured_at"], errors="coerce", utc=True)
    if pd.isna(captured_at):
        raise ValueError("Snapshot metadata contains an invalid captured_at value.")

    def optional_timestamp(name: str):
        value = metadata.get(name)
        if value in (None, ""):
            return None
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(parsed):
            return None
        return pd.Timestamp(parsed)

    return {
        "snapshot_id": str(metadata["snapshot_id"]),
        "captured_at": pd.Timestamp(captured_at),
        "source_version": (
            None if metadata.get("source_version") in (None, "")
            else str(metadata.get("source_version"))
        ),
        "outage_count": int(metadata["outage_count"]),
        "started_at": optional_timestamp("started_at"),
        "finished_at": optional_timestamp("finished_at"),
        "error_message": metadata.get("error_message"),
    }


def runs_from_rows(df: pd.DataFrame, source_version: str = "legacy") -> list[dict[str, Any]]:
    """Build one successful collection-run descriptor per snapshot in a row set."""
    if df.empty:
        return []

    grouped = (
        df.groupby(["snapshot_id", "captured_at"], dropna=False)
        .size()
        .reset_index(name="outage_count")
    )

    runs: list[dict[str, Any]] = []
    for row in grouped.itertuples(index=False):
        runs.append(
            {
                "snapshot_id": str(row.snapshot_id),
                "captured_at": pd.Timestamp(row.captured_at),
                "source_version": source_version,
                "outage_count": int(row.outage_count),
                "started_at": None,
                "finished_at": None,
                "error_message": None,
            }
        )
    return runs


def deduplicate_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep one descriptor per snapshot_id, preferring the last descriptor."""
    by_id: dict[str, dict[str, Any]] = {}
    for run in runs:
        by_id[str(run["snapshot_id"])] = run
    return list(by_id.values())


# =============================================================================
# Local data loading
# =============================================================================

def load_raw_history() -> pd.DataFrame:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw file not found: {RAW_FILE}")

    df = normalize_raw_dataframe(pd.read_csv(RAW_FILE, low_memory=False))
    if not df.empty:
        df["snapshot_id"] = df["captured_at"].map(legacy_snapshot_id)
    else:
        df["snapshot_id"] = pd.Series(dtype="object")
    return prepare_dataframe(df, RAW_COLUMNS)


def load_current_snapshot_metadata() -> dict[str, Any] | None:
    if not CURRENT_SNAPSHOT_META_FILE.exists():
        return None
    try:
        payload = json.loads(CURRENT_SNAPSHOT_META_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Unable to read snapshot metadata: {CURRENT_SNAPSHOT_META_FILE}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Current snapshot metadata must be a JSON object.")
    return payload


def load_current_snapshot() -> tuple[pd.DataFrame, dict[str, Any] | None]:
    """Load current snapshot, including a valid zero-row snapshot via its manifest."""
    metadata = load_current_snapshot_metadata()

    if not CURRENT_SNAPSHOT_FILE.exists():
        if metadata is not None:
            raise FileNotFoundError(
                f"Snapshot metadata exists but CSV is missing: {CURRENT_SNAPSHOT_FILE}"
            )
        return pd.DataFrame(columns=RAW_COLUMNS), None

    df = normalize_raw_dataframe(pd.read_csv(CURRENT_SNAPSHOT_FILE, low_memory=False))

    if metadata is not None:
        run = metadata_to_run(metadata)
        if run["outage_count"] != len(df):
            raise RuntimeError(
                "Current snapshot row count does not match its manifest: "
                f"CSV={len(df):,}, manifest={run['outage_count']:,}."
            )

        if not df.empty:
            manifest_capture = pd.Timestamp(run["captured_at"])
            if not bool((df["captured_at"] == manifest_capture).all()):
                raise RuntimeError(
                    "Current snapshot captured_at values do not match its manifest."
                )

        df["snapshot_id"] = str(run["snapshot_id"])
        return prepare_dataframe(df, RAW_COLUMNS), run

    # Backward compatibility with snapshots created before the sidecar manifest.
    if df.empty:
        return pd.DataFrame(columns=RAW_COLUMNS), None

    unique_captures = df["captured_at"].dropna().unique()
    if len(unique_captures) != 1:
        raise RuntimeError(
            "Legacy current_snapshot.csv must contain exactly one captured_at value."
        )

    df["snapshot_id"] = df["captured_at"].map(legacy_snapshot_id)
    run = runs_from_rows(df, source_version="legacy-current")[0]
    return prepare_dataframe(df, RAW_COLUMNS), run


def load_municipalities() -> pd.DataFrame:
    if not MUNICIPALITIES_FILE.exists():
        print(f"Municipality file not found: {MUNICIPALITIES_FILE}")
        return pd.DataFrame(columns=MUNICIPALITY_COLUMNS)

    df = pd.read_csv(MUNICIPALITIES_FILE, low_memory=False)

    for column in ["first_seen_at", "last_seen_at"]:
        if column in df.columns:
            df[column] = normalize_capture_series(df[column])

    if "is_geocoded" in df.columns:
        df["is_geocoded"] = (
            df["is_geocoded"].astype(str).str.lower().isin(["true", "1", "yes"])
        )

    for column in [
        "municipality_id",
        "geo_municipality_code",
        "mrc_code",
        "region_code",
        "match_rate_pct",
        "matched_records_count",
        "outage_records_count",
        "avg_lon",
        "avg_lat",
    ]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df = prepare_dataframe(df, MUNICIPALITY_COLUMNS)
    df = df.dropna(subset=["municipality_id"])
    return df.drop_duplicates(subset=["municipality_id"], keep="last")


# =============================================================================
# Schema migration and collection-run tracking
# =============================================================================

def ensure_collection_schema(connection) -> None:
    """Apply the P0 snapshot migration without discarding existing history."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SET TIME ZONE 'UTC';

            CREATE TABLE IF NOT EXISTS collection_runs (
                snapshot_id TEXT PRIMARY KEY,
                captured_at TIMESTAMPTZ NOT NULL,
                source_version TEXT,
                status TEXT NOT NULL,
                outage_count INTEGER NOT NULL CHECK (outage_count >= 0),
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                error_message TEXT,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            ALTER TABLE raw_outage_snapshots
            ADD COLUMN IF NOT EXISTS snapshot_id TEXT;

            UPDATE raw_outage_snapshots
            SET snapshot_id = 'legacy:' || to_char(
                captured_at,
                'YYYYMMDD"T"HH24MISS.US'
            )
            WHERE snapshot_id IS NULL
              AND captured_at IS NOT NULL;

            INSERT INTO collection_runs (
                snapshot_id,
                captured_at,
                source_version,
                status,
                outage_count,
                started_at,
                finished_at,
                error_message
            )
            SELECT
                snapshot_id,
                MIN(captured_at),
                'legacy',
                'success',
                COUNT(*),
                NULL,
                NULL,
                NULL
            FROM raw_outage_snapshots
            WHERE snapshot_id IS NOT NULL
            GROUP BY snapshot_id
            ON CONFLICT (snapshot_id) DO NOTHING;

            ALTER TABLE raw_outage_snapshots
            ALTER COLUMN snapshot_id SET NOT NULL;

            -- P1 timezone migration. Historical capture timestamps are UTC
            -- wall-clock values; Hydro start/restore timestamps are Quebec
            -- wall-clock values. Convert only legacy timestamp-without-zone
            -- columns so the migration remains idempotent.
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'collection_runs'
                      AND column_name = 'captured_at'
                      AND data_type = 'timestamp without time zone'
                ) THEN
                    ALTER TABLE collection_runs
                    ALTER COLUMN captured_at TYPE TIMESTAMPTZ
                    USING captured_at AT TIME ZONE 'UTC';
                END IF;

                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'raw_outage_snapshots'
                      AND column_name = 'captured_at'
                      AND data_type = 'timestamp without time zone'
                ) THEN
                    ALTER TABLE raw_outage_snapshots
                    ALTER COLUMN captured_at TYPE TIMESTAMPTZ
                    USING captured_at AT TIME ZONE 'UTC';
                END IF;

                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'raw_outage_snapshots'
                      AND column_name = 'start_time'
                      AND data_type = 'timestamp without time zone'
                ) THEN
                    ALTER TABLE raw_outage_snapshots
                    ALTER COLUMN start_time TYPE TIMESTAMPTZ
                    USING start_time AT TIME ZONE 'America/Toronto';
                END IF;

                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'raw_outage_snapshots'
                      AND column_name = 'estimated_restore'
                      AND data_type = 'timestamp without time zone'
                ) THEN
                    ALTER TABLE raw_outage_snapshots
                    ALTER COLUMN estimated_restore TYPE TIMESTAMPTZ
                    USING estimated_restore AT TIME ZONE 'America/Toronto';
                END IF;

                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'dim_municipalities'
                      AND column_name = 'first_seen_at'
                      AND data_type = 'timestamp without time zone'
                ) THEN
                    ALTER TABLE dim_municipalities
                    ALTER COLUMN first_seen_at TYPE TIMESTAMPTZ
                    USING first_seen_at AT TIME ZONE 'UTC';
                END IF;

                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'dim_municipalities'
                      AND column_name = 'last_seen_at'
                      AND data_type = 'timestamp without time zone'
                ) THEN
                    ALTER TABLE dim_municipalities
                    ALTER COLUMN last_seen_at TYPE TIMESTAMPTZ
                    USING last_seen_at AT TIME ZONE 'UTC';
                END IF;
            END $$;

            CREATE INDEX IF NOT EXISTS idx_raw_outage_snapshots_snapshot_id
            ON raw_outage_snapshots (snapshot_id);

            CREATE INDEX IF NOT EXISTS idx_collection_runs_success_capture
            ON collection_runs (captured_at DESC)
            WHERE status = 'success';
            """
        )
    connection.commit()


def begin_collection_runs(connection, runs: list[dict[str, Any]]) -> None:
    """Persist pending run records before their raw rows are inserted."""
    if not runs:
        return

    values = [
        (
            run["snapshot_id"],
            clean_value(run["captured_at"]),
            run.get("source_version"),
            "pending",
            int(run["outage_count"]),
            clean_value(run.get("started_at")),
            None,
            None,
        )
        for run in runs
    ]

    sql = """
        INSERT INTO collection_runs (
            snapshot_id,
            captured_at,
            source_version,
            status,
            outage_count,
            started_at,
            finished_at,
            error_message
        )
        VALUES %s
        ON CONFLICT (snapshot_id) DO UPDATE SET
            captured_at = EXCLUDED.captured_at,
            source_version = COALESCE(EXCLUDED.source_version, collection_runs.source_version),
            outage_count = EXCLUDED.outage_count,
            started_at = COALESCE(EXCLUDED.started_at, collection_runs.started_at),
            status = CASE
                WHEN collection_runs.status = 'success' THEN 'success'
                ELSE 'pending'
            END,
            error_message = CASE
                WHEN collection_runs.status = 'success' THEN collection_runs.error_message
                ELSE NULL
            END;
    """

    with connection.cursor() as cursor:
        execute_values(cursor, sql, values, page_size=500)
    connection.commit()


def finish_collection_runs(
    connection,
    runs: list[dict[str, Any]],
    *,
    status: str,
    error_message: str | None = None,
) -> None:
    if not runs:
        return

    snapshot_ids = [str(run["snapshot_id"]) for run in runs]
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE collection_runs
            SET status = CASE
                    WHEN collection_runs.status = 'success' AND %s = 'error'
                        THEN collection_runs.status
                    ELSE %s
                END,
                finished_at = CASE
                    WHEN collection_runs.status = 'success' AND %s = 'error'
                        THEN collection_runs.finished_at
                    ELSE NOW()
                END,
                error_message = CASE
                    WHEN collection_runs.status = 'success' AND %s = 'error'
                        THEN collection_runs.error_message
                    ELSE %s
                END
            WHERE snapshot_id = ANY(%s);
            """,
            (status, status, status, status, error_message, snapshot_ids),
        )
    connection.commit()


# =============================================================================
# Incremental synchronization
# =============================================================================

def get_latest_database_capture(connection) -> pd.Timestamp | None:
    """Return the latest successful collection capture, including zero snapshots."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT captured_at
            FROM collection_runs
            WHERE status = 'success'
            ORDER BY captured_at DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()

    value = row[0] if row else None
    if value is None:
        return None

    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp)


def filter_incremental_raw_history(connection, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    latest_database_capture = get_latest_database_capture(connection)
    if latest_database_capture is None:
        print("Supabase has no successful snapshot. A full synchronization will be performed.")
        return df

    cutoff = latest_database_capture - pd.Timedelta(hours=get_sync_lookback_hours())
    incremental_df = df[df["captured_at"] >= cutoff].copy()

    print(f"Supabase latest successful capture: {latest_database_capture}")
    print(f"Incremental sync cutoff: {cutoff}")
    print(
        "Raw rows selected for incremental sync: "
        f"{len(incremental_df):,} / {len(df):,}"
    )
    return incremental_df


def raw_table_is_empty(connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT NOT EXISTS (SELECT 1 FROM raw_outage_snapshots LIMIT 1);")
        return bool(cursor.fetchone()[0])


def choose_raw_sync_batch(
    connection,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Choose raw rows plus collection runs; zero-row current snapshots are valid."""
    current_df, current_run = load_current_snapshot()
    current_available = current_run is not None

    if raw_table_is_empty(connection) and RAW_FILE.exists():
        print("Supabase raw table is empty: bootstrapping from local history CSV.")
        history = load_raw_history()
        runs = runs_from_rows(history)
        rows = history

        # Include the freshly fetched snapshot in the same bootstrap run. This
        # avoids waiting an extra hour when the bundled history is stale.
        if current_available:
            if not current_df.empty:
                rows = pd.concat([rows, current_df], ignore_index=True)
                rows = rows.drop_duplicates(
                    subset=["outage_id", "captured_at"], keep="last"
                )
            runs.append(current_run)

        return prepare_dataframe(rows, RAW_COLUMNS), deduplicate_runs(runs)

    if current_available:
        print(f"Using current snapshot for incremental sync: {len(current_df):,} rows.")
        selected = filter_incremental_raw_history(connection, current_df)
        return prepare_dataframe(selected, RAW_COLUMNS), [current_run]

    if RAW_FILE.exists():
        print("Current snapshot unavailable; falling back to local history CSV.")
        history = filter_incremental_raw_history(connection, load_raw_history())
        return history, runs_from_rows(history)

    raise FileNotFoundError(
        "Neither a valid data/raw/current_snapshot.csv manifest nor "
        "data/raw/hydroquebec_history.csv is available."
    )


# =============================================================================
# Raw history synchronization
# =============================================================================

def sync_raw_history(connection, df: pd.DataFrame) -> None:
    """Insert immutable raw observations in small transaction batches."""
    if df.empty:
        print("No raw outage records to sync (valid for a zero-outage snapshot).")
        return

    records = dataframe_to_records(df, RAW_COLUMNS)
    insert_sql = f"""
        INSERT INTO raw_outage_snapshots ({", ".join(RAW_COLUMNS)})
        VALUES %s
        ON CONFLICT (outage_id, captured_at) DO NOTHING;
    """

    total_records = len(records)
    print(f"Starting raw synchronization: {total_records:,} rows")

    for start in range(0, total_records, RAW_BATCH_SIZE):
        end = min(start + RAW_BATCH_SIZE, total_records)
        batch = records[start:end]
        try:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    insert_sql,
                    batch,
                    page_size=RAW_BATCH_SIZE,
                )
        except Exception:
            # Roll back the whole raw snapshot transaction. collection_runs is
            # still pending in its separately committed control record.
            connection.rollback()
            print(f"Raw synchronization failed for rows {start + 1:,} to {end:,}.")
            raise
        print(f"Raw sync progress: {end:,} / {total_records:,}")

    # Do not commit here: the caller marks collection_runs as success in the
    # same transaction. Raw rows and the successful run therefore become
    # visible atomically.
    print(f"Prepared raw outage records for commit: {total_records:,}")


# =============================================================================
# Municipality synchronization
# =============================================================================

def sync_municipalities(
    connection,
    df: pd.DataFrame,
) -> None:
    """
    Upsert municipality reference data in small transaction batches.
    """
    if df.empty:
        print(
            "No municipality records to sync."
        )
        return

    records = dataframe_to_records(
        df,
        MUNICIPALITY_COLUMNS,
    )

    insert_sql = f"""
        INSERT INTO dim_municipalities (
            {", ".join(MUNICIPALITY_COLUMNS)}
        )
        VALUES %s

        ON CONFLICT (
            municipality_id
        )

        DO UPDATE SET
            municipality_label =
                EXCLUDED.municipality_label,

            municipality_name =
                EXCLUDED.municipality_name,

            municipality_full_name =
                EXCLUDED.municipality_full_name,

            geo_municipality_code =
                EXCLUDED.geo_municipality_code,

            municipality_type_code =
                EXCLUDED.municipality_type_code,

            mrc_code =
                EXCLUDED.mrc_code,

            mrc_name =
                EXCLUDED.mrc_name,

            region_code =
                EXCLUDED.region_code,

            region_name =
                EXCLUDED.region_name,

            is_geocoded =
                EXCLUDED.is_geocoded,

            match_rate_pct =
                EXCLUDED.match_rate_pct,

            matched_records_count =
                EXCLUDED.matched_records_count,

            outage_records_count =
                EXCLUDED.outage_records_count,

            avg_lon =
                EXCLUDED.avg_lon,

            avg_lat =
                EXCLUDED.avg_lat,

            first_seen_at =
                EXCLUDED.first_seen_at,

            last_seen_at =
                EXCLUDED.last_seen_at,

            updated_at =
                NOW()

        WHERE (
            dim_municipalities.municipality_label,
            dim_municipalities.municipality_name,
            dim_municipalities.municipality_full_name,
            dim_municipalities.geo_municipality_code,
            dim_municipalities.municipality_type_code,
            dim_municipalities.mrc_code,
            dim_municipalities.mrc_name,
            dim_municipalities.region_code,
            dim_municipalities.region_name,
            dim_municipalities.is_geocoded,
            dim_municipalities.match_rate_pct,
            dim_municipalities.matched_records_count,
            dim_municipalities.outage_records_count,
            dim_municipalities.avg_lon,
            dim_municipalities.avg_lat,
            dim_municipalities.first_seen_at,
            dim_municipalities.last_seen_at
        ) IS DISTINCT FROM (
            EXCLUDED.municipality_label,
            EXCLUDED.municipality_name,
            EXCLUDED.municipality_full_name,
            EXCLUDED.geo_municipality_code,
            EXCLUDED.municipality_type_code,
            EXCLUDED.mrc_code,
            EXCLUDED.mrc_name,
            EXCLUDED.region_code,
            EXCLUDED.region_name,
            EXCLUDED.is_geocoded,
            EXCLUDED.match_rate_pct,
            EXCLUDED.matched_records_count,
            EXCLUDED.outage_records_count,
            EXCLUDED.avg_lon,
            EXCLUDED.avg_lat,
            EXCLUDED.first_seen_at,
            EXCLUDED.last_seen_at
        );
    """

    total_records = len(
        records
    )

    print(
        "Starting municipality synchronization: "
        f"{total_records:,} rows"
    )

    for start in range(
        0,
        total_records,
        MUNICIPALITY_BATCH_SIZE,
    ):
        end = min(
            start + MUNICIPALITY_BATCH_SIZE,
            total_records,
        )

        batch = records[
            start:end
        ]

        try:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    insert_sql,
                    batch,
                    page_size=(
                        MUNICIPALITY_BATCH_SIZE
                    ),
                )

            connection.commit()

        except Exception:
            connection.rollback()

            print(
                "Municipality synchronization "
                f"failed for rows {start + 1:,}"
                f" to {end:,}."
            )

            raise

        print(
            "Municipality sync progress: "
            f"{end:,}"
            f" / {total_records:,}"
        )

    print(
        "Synced municipality records: "
        f"{total_records:,}"
    )



# =============================================================================
# Lightweight validation / orchestration
# =============================================================================

def print_database_summary(connection, synced_rows: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT snapshot_id, captured_at, outage_count
            FROM collection_runs
            WHERE status = 'success'
            ORDER BY captured_at DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()

    snapshot_id, latest_capture, outage_count = row if row else (None, None, None)
    print(f"Raw rows submitted this run: {synced_rows:,}")
    print(f"Latest successful snapshot: {snapshot_id}")
    print(f"Latest successful capture: {latest_capture}")
    print(f"Latest snapshot outage count: {outage_count}")


def municipality_table_is_empty(connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT NOT EXISTS (SELECT 1 FROM dim_municipalities LIMIT 1);")
        return bool(cursor.fetchone()[0])


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    database_url = os.environ.get("SUPABASE_DB_URL")
    if not database_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable.")

    connection_kwargs = {
        "sslmode": "require",
        "connect_timeout": 15,
        "application_name": "projethydro_sync",
    }
    database_hostaddr = os.environ.get("SUPABASE_DB_HOSTADDR")
    if database_hostaddr:
        connection_kwargs["hostaddr"] = database_hostaddr

    connection = psycopg2.connect(database_url, **connection_kwargs)

    try:
        # Automatic, idempotent migration: existing history is assigned legacy
        # snapshot IDs and represented in collection_runs without data loss.
        ensure_collection_schema(connection)

        sync_raw = env_flag("SUPABASE_SYNC_RAW", default=True)
        synced_rows = 0

        if sync_raw:
            raw_rows, collection_runs = choose_raw_sync_batch(connection)
            print(f"Raw rows selected for this run: {len(raw_rows):,}")
            print(f"Collection runs represented: {len(collection_runs):,}")

            begin_collection_runs(connection, collection_runs)
            try:
                sync_raw_history(connection, raw_rows)
            except Exception as exc:
                try:
                    finish_collection_runs(
                        connection,
                        collection_runs,
                        status="error",
                        error_message=str(exc)[:2000],
                    )
                except Exception:
                    connection.rollback()
                raise
            else:
                finish_collection_runs(
                    connection,
                    collection_runs,
                    status="success",
                )
                synced_rows = len(raw_rows)
        else:
            print("Raw synchronization disabled for this run.")

        sync_municipality_reference = env_flag(
            "SUPABASE_SYNC_MUNICIPALITIES",
            default=False,
        ) or municipality_table_is_empty(connection)

        if sync_municipality_reference:
            municipalities = load_municipalities()
            print(f"Municipality rows selected: {len(municipalities):,}")
            sync_municipalities(connection, municipalities)
        else:
            print(
                "Municipality synchronization skipped. "
                "Use SUPABASE_SYNC_MUNICIPALITIES=1 for a maintenance run."
            )

        if sync_raw:
            print_database_summary(connection, synced_rows=synced_rows)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
