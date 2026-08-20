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

MUNICIPALITIES_FILE = Path(
    "data/reference/municipalities.csv"
)

RAW_BATCH_SIZE = 500
MUNICIPALITY_BATCH_SIZE = 250

# On resynchronise une petite fenêtre avant la dernière capture déjà présente.
# Cela permet de récupérer d'éventuelles corrections ou arrivées tardives
# sans renvoyer l'historique complet.
DEFAULT_LOOKBACK_HOURS = 24


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
    Upsert raw outage history in small transaction batches.
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

        DO UPDATE SET
            customers_affected =
                EXCLUDED.customers_affected,

            start_time =
                EXCLUDED.start_time,

            estimated_restore =
                EXCLUDED.estimated_restore,

            status_code =
                EXCLUDED.status_code,

            status =
                EXCLUDED.status,

            cause_code =
                EXCLUDED.cause_code,

            cause_label =
                EXCLUDED.cause_label,

            municipality_id =
                EXCLUDED.municipality_id,

            lon =
                EXCLUDED.lon,

            lat =
                EXCLUDED.lat,

            ingested_at =
                NOW()

        WHERE (
            raw_outage_snapshots.customers_affected,
            raw_outage_snapshots.start_time,
            raw_outage_snapshots.estimated_restore,
            raw_outage_snapshots.status_code,
            raw_outage_snapshots.status,
            raw_outage_snapshots.cause_code,
            raw_outage_snapshots.cause_label,
            raw_outage_snapshots.municipality_id,
            raw_outage_snapshots.lon,
            raw_outage_snapshots.lat
        ) IS DISTINCT FROM (
            EXCLUDED.customers_affected,
            EXCLUDED.start_time,
            EXCLUDED.estimated_restore,
            EXCLUDED.status_code,
            EXCLUDED.status,
            EXCLUDED.cause_code,
            EXCLUDED.cause_label,
            EXCLUDED.municipality_id,
            EXCLUDED.lon,
            EXCLUDED.lat
        );
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
# Database summary
# =============================================================================

def print_database_summary(
    connection,
) -> None:
    """
    Print a small validation summary after synchronization.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(
                    DISTINCT outage_id
                ) AS unique_outages,
                MIN(
                    captured_at
                ) AS first_capture,
                MAX(
                    captured_at
                ) AS latest_capture

            FROM raw_outage_snapshots;
            """
        )

        (
            total_rows,
            unique_outages,
            first_capture,
            latest_capture,
        ) = cursor.fetchone()

    print(
        "Supabase total raw rows: "
        f"{total_rows:,}"
    )

    print(
        "Supabase unique outages: "
        f"{unique_outages:,}"
    )

    print(
        "Supabase first capture: "
        f"{first_capture}"
    )

    print(
        "Supabase latest capture: "
        f"{latest_capture}"
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

    raw_history = (
        load_raw_history()
    )

    municipalities = (
        load_municipalities()
    )

    print(
        "Local raw rows available: "
        f"{len(raw_history):,}"
    )

    print(
        "Local municipality rows: "
        f"{len(municipalities):,}"
    )

    database_hostaddr = os.environ.get(
        "SUPABASE_DB_HOSTADDR"
    )

    connection_kwargs = {
        "sslmode":
            "require",
        "connect_timeout":
            15,
        "application_name":
            "projethydro_sync",
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
        incremental_raw_history = (
            filter_incremental_raw_history(
                connection,
                raw_history,
            )
        )

        sync_raw_history(
            connection,
            incremental_raw_history,
        )

        sync_municipalities(
            connection,
            municipalities,
        )

        print_database_summary(
            connection
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()

