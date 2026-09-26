from pathlib import Path

import duckdb


WAREHOUSE_PATH = Path("data/warehouse/outages.duckdb")
SQL_DIR = Path("sql")
RAW_CSV_PATH = Path("data/raw/hydroquebec_history.csv")


SQL_FILES = [
    "00_create_reference_tables.sql",
    "01_create_raw_table.sql",
    "02_create_latest_outages.sql",
    "03_create_active_outages.sql",
    "04_create_daily_summary.sql",
    "05_create_data_quality_report.sql",
]

def run_sql_file(connection: duckdb.DuckDBPyConnection, sql_file: Path) -> None:
    """Execute a SQL file in DuckDB."""
    print(f"Running {sql_file}...")
    query = sql_file.read_text(encoding="utf-8")
    connection.execute(query)


def main() -> None:
    if not RAW_CSV_PATH.exists():
        raise FileNotFoundError(f"Raw CSV not found: {RAW_CSV_PATH}")

    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(WAREHOUSE_PATH))

    for sql_file_name in SQL_FILES:
        run_sql_file(connection, SQL_DIR / sql_file_name)

    connection.close()

    print(f"Warehouse built successfully: {WAREHOUSE_PATH}")


if __name__ == "__main__":
    main()