from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# =============================================================================
# Configuration
# =============================================================================

RAW_FILE = Path(
    "data/raw/hydroquebec_history.csv"
)

CURRENT_SNAPSHOT_FILE = Path(
    "data/raw/current_snapshot.csv"
)

MUNICIPALITIES_FILE = Path(
    "data/reference/municipalities.csv"
)

RAW_BATCH_SIZE = 500
MUNICIPALITY_BATCH_SIZE = 250

# On resynchronise une petite fenêtre avant la dernière capture déjà présente.
# Cela permet de récupérer d'éventuelles corrections ou arrivées tardives
# sans renvoyer l'historique complet.
DEFAULT_LOOKBACK_HOURS = 2


RAW_COLUMNS = [
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

def env_flag(
    name: str,
    default: bool = False,
) -> bool:
    value = os.environ.get(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def clean_value(
    value: Any,
) -> Any:
    """
    Convert pandas values to database-friendly Python values.
    """
    if pd.isna(value):
        return None

    if isinstance(
        value,
        pd.Timestamp,
    ):
        return value.to_pydatetime()

    return value


def prepare_dataframe(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Ensure all expected columns exist and are ordered.
    """
    df = df.copy()

    for column in columns:
        if column not in df.columns:
            df[column] = None

    return df[
        columns
    ]


def dataframe_to_records(
    df: pd.DataFrame,
    columns: list[str],
) -> list[tuple]:
    """
    Convert a DataFrame to psycopg2-compatible tuples.
    """
    records: list[tuple] = []

    for row in df[
        columns
    ].itertuples(
        index=False,
        name=None,
    ):
        records.append(
            tuple(
                clean_value(
                    value
                )
                for value in row
            )
        )

    return records


def get_sync_lookback_hours() -> int:
    """
    Read the incremental synchronization lookback window.
    """
    raw_value = os.environ.get(
        "SUPABASE_SYNC_LOOKBACK_HOURS",
        str(
            DEFAULT_LOOKBACK_HOURS
        ),
    )

    try:
        hours = int(
            raw_value
        )
    except (
        TypeError,
        ValueError,
    ):
        hours = DEFAULT_LOOKBACK_HOURS

    return max(
        hours,
        0,
    )


# =============================================================================
# Local data loading
# =============================================================================

def load_raw_history() -> pd.DataFrame:
    """
    Load and normalize the local Hydro-Québec outage history.
    """
    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Raw file not found: {RAW_FILE}"
        )

    df = pd.read_csv(
        RAW_FILE,
        low_memory=False,
    )

    datetime_columns = [
        "start_time",
        "estimated_restore",
        "captured_at",
    ]

    for column in datetime_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
                utc=True,
            )

    numeric_columns = [
        "customers_affected",
        "cause_code",
        "municipality_id",
        "lon",
        "lat",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    df = prepare_dataframe(
        df,
        RAW_COLUMNS,
    )

    df = df.dropna(
        subset=[
            "outage_id",
            "captured_at",
        ]
    )

    # Keep only one local observation for each database key.
    df = df.drop_duplicates(
        subset=[
            "outage_id",
            "captured_at",
        ],
        keep="last",
    )

    return df


def load_current_snapshot() -> pd.DataFrame:
    """Load the small snapshot produced by fetch_outages.py."""
    if not CURRENT_SNAPSHOT_FILE.exists():
        return pd.DataFrame(columns=RAW_COLUMNS)

    df = pd.read_csv(
        CURRENT_SNAPSHOT_FILE,
        low_memory=False,
    )

    datetime_columns = [
        "start_time",
        "estimated_restore",
        "captured_at",
    ]

    for column in datetime_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
                utc=True,
            )

    numeric_columns = [
        "customers_affected",
        "cause_code",
        "municipality_id",
        "lon",
        "lat",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    df = prepare_dataframe(df, RAW_COLUMNS)
    df = df.dropna(subset=["outage_id", "captured_at"])
    df = df.drop_duplicates(
        subset=["outage_id", "captured_at"],
        keep="last",
    )

    return df


def load_municipalities() -> pd.DataFrame:
    """
    Load and normalize the municipality reference table.
    """
    if not MUNICIPALITIES_FILE.exists():
        print(
            "Municipality file not found: "
            f"{MUNICIPALITIES_FILE}"
        )

        return pd.DataFrame(
            columns=MUNICIPALITY_COLUMNS
        )

    df = pd.read_csv(
        MUNICIPALITIES_FILE,
        low_memory=False,
    )

    datetime_columns = [
        "first_seen_at",
        "last_seen_at",
    ]

    for column in datetime_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
                utc=True,
            )

    if "is_geocoded" in df.columns:
        df["is_geocoded"] = (
            df["is_geocoded"]
            .astype(
                str
            )
            .str.lower()
            .isin(
                [
                    "true",
                    "1",
                    "yes",
                ]
            )
        )

    numeric_columns = [
        "municipality_id",
        "geo_municipality_code",
        "mrc_code",
        "region_code",
        "match_rate_pct",
        "matched_records_count",
        "outage_records_count",
        "avg_lon",
        "avg_lat",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    df = prepare_dataframe(
        df,
        MUNICIPALITY_COLUMNS,
    )

    df = df.dropna(
        subset=[
            "municipality_id",
        ]
    )

    df = df.drop_duplicates(
        subset=[
            "municipality_id",
        ],
        keep="last",
    )

    return df


# =============================================================================
# Incremental synchronization
# =============================================================================
def get_latest_database_capture(
    connection,
) -> pd.Timestamp | None:
    """
    Return the latest raw capture already stored in Supabase.

    Database timestamps are interpreted as UTC whether PostgreSQL returns
    them as timezone-aware or timezone-naive values.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT MAX(captured_at)
            FROM raw_outage_snapshots;
            """
        )

        value = cursor.fetchone()[0]

    if value is None:
        return None

    timestamp = pd.to_datetime(
        value,
        errors="coerce",
        utc=True,
    )

    if pd.isna(timestamp):
        return None

    return pd.Timestamp(timestamp)

def filter_incremental_raw_history(
    connection,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Keep only recent rows that may need to be inserted or updated.

    A small lookback window is retained so late-arriving corrections can
    still be synchronized safely through the UPSERT.
    """
    if df.empty:
        return df

    latest_database_capture = (
        get_latest_database_capture(
            connection
        )
    )

    if latest_database_capture is None:
        print(
            "Supabase raw table is empty. "
            "A full initial synchronization will be performed."
        )

        return df

    lookback_hours = (
        get_sync_lookback_hours()
    )

    cutoff = (
        latest_database_capture
        - pd.Timedelta(
            hours=lookback_hours
        )
    )

    incremental_df = (
        df[
            df["captured_at"]
            >= cutoff
        ]
        .copy()
    )

    print(
        "Supabase latest capture: "
        f"{latest_database_capture}"
    )

    print(
        "Incremental sync cutoff: "
        f"{cutoff}"
    )

    print(
        "Raw rows selected for incremental sync: "
        f"{len(incremental_df):,}"
        f" / {len(df):,}"
    )

    return incremental_df


# =============================================================================
# Raw history synchronization
# =============================================================================

def sync_raw_history(
    connection,
    df: pd.DataFrame,
) -> None:
    """
    Insert immutable raw observations in small transaction batches.

    A captured snapshot is treated as an event: if the same
    (outage_id, captured_at) key is submitted again, it is ignored.
    """
    if df.empty:
        print(
            "No raw outage records to sync."
        )
        return

    records = dataframe_to_records(
        df,
        RAW_COLUMNS,
    )

    insert_sql = f"""
        INSERT INTO raw_outage_snapshots (
            {", ".join(RAW_COLUMNS)}
        )
        VALUES %s

        ON CONFLICT (
            outage_id,
            captured_at
        )
        DO NOTHING;
    """

    total_records = len(
        records
    )

    print(
        "Starting raw synchronization: "
        f"{total_records:,} rows"
    )

    for start in range(
        0,
        total_records,
        RAW_BATCH_SIZE,
    ):
        end = min(
            start + RAW_BATCH_SIZE,
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
                    page_size=RAW_BATCH_SIZE,
                )

            connection.commit()

        except Exception:
            connection.rollback()

            print(
                "Raw synchronization failed "
                f"for rows {start + 1:,}"
                f" to {end:,}."
            )

            raise

        print(
            "Raw sync progress: "
            f"{end:,}"
            f" / {total_records:,}"
        )

    print(
        "Synced raw outage records: "
        f"{total_records:,}"
    )


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

def print_database_summary(
    connection,
    synced_rows: int,
) -> None:
    """Print validation values without scanning the entire raw table."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT captured_at
            FROM raw_outage_snapshots
            WHERE captured_at IS NOT NULL
            ORDER BY captured_at DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()

    latest_capture = row[0] if row else None

    print(
        "Raw rows submitted this run: "
        f"{synced_rows:,}"
    )
    print(
        "Supabase latest capture: "
        f"{latest_capture}"
    )


def raw_table_is_empty(connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT NOT EXISTS (
                SELECT 1
                FROM raw_outage_snapshots
                LIMIT 1
            );
            """
        )
        return bool(cursor.fetchone()[0])


def municipality_table_is_empty(connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT NOT EXISTS (
                SELECT 1
                FROM dim_municipalities
                LIMIT 1
            );
            """
        )
        return bool(cursor.fetchone()[0])


def choose_raw_sync_dataframe(connection) -> pd.DataFrame:
    """Prefer the current snapshot; use the full CSV only for bootstrap/fallback."""
    if raw_table_is_empty(connection) and RAW_FILE.exists():
        print(
            "Supabase raw table is empty: bootstrapping from the local history CSV."
        )
        return load_raw_history()

    snapshot = load_current_snapshot()

    if not snapshot.empty:
        print(
            "Using current snapshot for incremental sync: "
            f"{len(snapshot):,} rows."
        )
        return filter_incremental_raw_history(
            connection,
            snapshot,
        )

    if RAW_FILE.exists():
        print(
            "Current snapshot not found; falling back to the local history CSV."
        )
        return filter_incremental_raw_history(
            connection,
            load_raw_history(),
        )

    raise FileNotFoundError(
        "Neither data/raw/current_snapshot.csv nor "
        "data/raw/hydroquebec_history.csv is available."
    )


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    database_url = os.environ.get(
        "SUPABASE_DB_URL"
    )

    if not database_url:
        raise RuntimeError(
            "Missing SUPABASE_DB_URL "
            "environment variable."
        )

    database_hostaddr = os.environ.get(
        "SUPABASE_DB_HOSTADDR"
    )

    connection_kwargs = {
        "sslmode": "require",
        "connect_timeout": 15,
        "application_name": "projethydro_sync",
    }

    if database_hostaddr:
        connection_kwargs[
            "hostaddr"
        ] = database_hostaddr

    connection = psycopg2.connect(
        database_url,
        **connection_kwargs,
    )

    try:
        sync_raw = env_flag(
            "SUPABASE_SYNC_RAW",
            default=True,
        )

        synced_rows = 0

        if sync_raw:
            incremental_raw_history = choose_raw_sync_dataframe(
                connection
            )

            print(
                "Raw rows selected for this run: "
                f"{len(incremental_raw_history):,}"
            )

            sync_raw_history(
                connection,
                incremental_raw_history,
            )
            synced_rows = len(incremental_raw_history)
        else:
            print("Raw synchronization disabled for this run.")

        sync_municipality_reference = env_flag(
            "SUPABASE_SYNC_MUNICIPALITIES",
            default=False,
        ) or municipality_table_is_empty(connection)

        if sync_municipality_reference:
            municipalities = load_municipalities()
            print(
                "Municipality rows selected: "
                f"{len(municipalities):,}"
            )
            sync_municipalities(
                connection,
                municipalities,
            )
        else:
            print(
                "Municipality synchronization skipped. "
                "Use SUPABASE_SYNC_MUNICIPALITIES=1 for a maintenance run."
            )

        if sync_raw:
            print_database_summary(
                connection,
                synced_rows=synced_rows,
            )

    finally:
        connection.close()


if __name__ == "__main__":
    main()
