from pathlib import Path

import duckdb


WAREHOUSE_PATH = Path("data/warehouse/outages.duckdb")
PROCESSED_DIR = Path("data/processed")


TABLES_TO_EXPORT = {
    "latest_outages": "latest_outages.csv",
    "active_outages": "active_outages.csv",
    "daily_summary": "daily_summary.csv",
    "data_quality_report": "data_quality_report.csv",
}


def export_table(connection: duckdb.DuckDBPyConnection, table_name: str, output_file: Path) -> None:
    """Export a DuckDB table to a CSV file."""
    print(f"Exporting {table_name} -> {output_file}")

    query = f"""
    COPY (
        SELECT *
        FROM {table_name}
    )
    TO '{output_file.as_posix()}'
    WITH (HEADER, DELIMITER ',');
    """

    connection.execute(query)


def main() -> None:
    if not WAREHOUSE_PATH.exists():
        raise FileNotFoundError(
            f"Warehouse not found: {WAREHOUSE_PATH}. Run scripts/build_warehouse.py first."
        )

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(WAREHOUSE_PATH))

    for table_name, file_name in TABLES_TO_EXPORT.items():
        export_table(connection, table_name, PROCESSED_DIR / file_name)

    connection.close()

    print("Processed CSV files exported successfully.")


if __name__ == "__main__":
    main()