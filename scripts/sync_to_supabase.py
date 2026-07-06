import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


RAW_FILE = Path("data/raw/hydroquebec_history.csv")
MUNICIPALITIES_FILE = Path("data/reference/municipalities.csv")

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


def clean_value(value):
    """Convert pandas values to database-friendly Python values."""
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


def prepare_dataframe(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Ensure all expected columns exist and are ordered."""
    df = df.copy()

    for col in columns:
        if col not in df.columns:
            df[col] = None

    return df[columns]


def load_raw_history() -> pd.DataFrame:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw file not found: {RAW_FILE}")

    df = pd.read_csv(RAW_FILE, low_memory=False)

    datetime_columns = ["start_time", "estimated_restore", "captured_at"]

    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_columns = [
        "customers_affected",
        "cause_code",
        "municipality_id",
        "lon",
        "lat",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = prepare_dataframe(df, RAW_COLUMNS)

    df = df.dropna(subset=["outage_id", "captured_at"])

    return df


def load_municipalities() -> pd.DataFrame:
    if not MUNICIPALITIES_FILE.exists():
        print(f"Municipality file not found: {MUNICIPALITIES_FILE}")
        return pd.DataFrame(columns=MUNICIPALITY_COLUMNS)

    df = pd.read_csv(MUNICIPALITIES_FILE, low_memory=False)

    datetime_columns = ["first_seen_at", "last_seen_at"]

    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "is_geocoded" in df.columns:
        df["is_geocoded"] = (
            df["is_geocoded"]
            .astype(str)
            .str.lower()
            .isin(["true", "1", "yes"])
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

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = prepare_dataframe(df, MUNICIPALITY_COLUMNS)

    df = df.dropna(subset=["municipality_id"])

    return df


def dataframe_to_records(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    records = []

    for row in df[columns].itertuples(index=False, name=None):
        records.append(tuple(clean_value(value) for value in row))

    return records


def sync_raw_history(connection, df: pd.DataFrame) -> None:
    if df.empty:
        print("No raw outage records to sync.")
        return

    records = dataframe_to_records(df, RAW_COLUMNS)

    insert_sql = f"""
        INSERT INTO raw_outage_snapshots (
            {", ".join(RAW_COLUMNS)}
        )
        VALUES %s
        ON CONFLICT (outage_id, captured_at)
        DO UPDATE SET
            customers_affected = EXCLUDED.customers_affected,
            start_time = EXCLUDED.start_time,
            estimated_restore = EXCLUDED.estimated_restore,
            status_code = EXCLUDED.status_code,
            status = EXCLUDED.status,
            cause_code = EXCLUDED.cause_code,
            cause_label = EXCLUDED.cause_label,
            municipality_id = EXCLUDED.municipality_id,
            lon = EXCLUDED.lon,
            lat = EXCLUDED.lat,
            ingested_at = NOW();
    """

    with connection.cursor() as cursor:
        execute_values(
            cursor,
            insert_sql,
            records,
            page_size=5000,
        )

    connection.commit()

    print(f"Synced raw outage records: {len(records):,}")


def sync_municipalities(connection, df: pd.DataFrame) -> None:
    if df.empty:
        print("No municipality records to sync.")
        return

    records = dataframe_to_records(df, MUNICIPALITY_COLUMNS)

    insert_sql = f"""
        INSERT INTO dim_municipalities (
            {", ".join(MUNICIPALITY_COLUMNS)}
        )
        VALUES %s
        ON CONFLICT (municipality_id)
        DO UPDATE SET
            municipality_label = EXCLUDED.municipality_label,
            municipality_name = EXCLUDED.municipality_name,
            municipality_full_name = EXCLUDED.municipality_full_name,
            geo_municipality_code = EXCLUDED.geo_municipality_code,
            municipality_type_code = EXCLUDED.municipality_type_code,
            mrc_code = EXCLUDED.mrc_code,
            mrc_name = EXCLUDED.mrc_name,
            region_code = EXCLUDED.region_code,
            region_name = EXCLUDED.region_name,
            is_geocoded = EXCLUDED.is_geocoded,
            match_rate_pct = EXCLUDED.match_rate_pct,
            matched_records_count = EXCLUDED.matched_records_count,
            outage_records_count = EXCLUDED.outage_records_count,
            avg_lon = EXCLUDED.avg_lon,
            avg_lat = EXCLUDED.avg_lat,
            first_seen_at = EXCLUDED.first_seen_at,
            last_seen_at = EXCLUDED.last_seen_at,
            updated_at = NOW();
    """

    with connection.cursor() as cursor:
        execute_values(
            cursor,
            insert_sql,
            records,
            page_size=2000,
        )

    connection.commit()

    print(f"Synced municipality records: {len(records):,}")


def print_database_summary(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT outage_id) AS unique_outages,
                MIN(captured_at) AS first_capture,
                MAX(captured_at) AS latest_capture
            FROM raw_outage_snapshots;
            """
        )
        total_rows, unique_outages, first_capture, latest_capture = cursor.fetchone()

    print(f"Supabase total raw rows: {total_rows:,}")
    print(f"Supabase unique outages: {unique_outages:,}")
    print(f"Supabase first capture: {first_capture}")
    print(f"Supabase latest capture: {latest_capture}")


def main() -> None:
    database_url = os.environ.get("SUPABASE_DB_URL")

    if not database_url:
        raise RuntimeError(
            "Missing SUPABASE_DB_URL environment variable."
        )

    raw_history = load_raw_history()
    municipalities = load_municipalities()

    print(f"Local raw rows to sync: {len(raw_history):,}")
    print(f"Local municipality rows to sync: {len(municipalities):,}")

    connection = psycopg2.connect(database_url)

    try:
        sync_raw_history(connection, raw_history)
        sync_municipalities(connection, municipalities)
        print_database_summary(connection)

    finally:
        connection.close()


if __name__ == "__main__":
    main()
