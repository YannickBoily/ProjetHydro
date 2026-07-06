import os

import pandas as pd
import psycopg2


def main() -> None:
    database_url = os.environ.get("SUPABASE_DB_URL")

    if not database_url:
        raise RuntimeError("Missing SUPABASE_DB_URL environment variable.")

    database_hostaddr = os.environ.get("SUPABASE_DB_HOSTADDR")

    connection_kwargs = {}

    if database_hostaddr:
        connection_kwargs["hostaddr"] = database_hostaddr

    connection = psycopg2.connect(database_url, **connection_kwargs)
    try:
        summary = pd.read_sql_query(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT outage_id) AS unique_outages,
                MIN(captured_at) AS first_capture_at,
                MAX(captured_at) AS latest_capture_at
            FROM raw_outage_snapshots;
            """,
            connection,
        )

        causes = pd.read_sql_query(
            """
            SELECT
                COALESCE(cause_label, 'unknown') AS cause_label,
                COUNT(*) AS rows_count
            FROM raw_outage_snapshots
            GROUP BY COALESCE(cause_label, 'unknown')
            ORDER BY rows_count DESC;
            """,
            connection,
        )

        regions = pd.read_sql_query(
            """
            SELECT
                COALESCE(region_name, 'Non géocodée') AS region_name,
                COUNT(*) AS municipalities_count
            FROM dim_municipalities
            GROUP BY COALESCE(region_name, 'Non géocodée')
            ORDER BY municipalities_count DESC;
            """,
            connection,
        )

        print("\n=== Load summary ===")
        print(summary.to_string(index=False))

        print("\n=== Causes ===")
        print(causes.to_string(index=False))

        print("\n=== Municipalities by region ===")
        print(regions.to_string(index=False))

    finally:
        connection.close()


if __name__ == "__main__":
    main()
