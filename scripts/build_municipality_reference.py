from pathlib import Path

import pandas as pd


RAW_FILE = Path("data/raw/hydroquebec_history.csv")
REFERENCE_DIR = Path("data/reference")
REFERENCE_FILE = REFERENCE_DIR / "municipalities.csv"


def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw file not found: {RAW_FILE}")

    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

    history = pd.read_csv(RAW_FILE, low_memory=False)

    required_columns = ["municipality_id", "lon", "lat", "captured_at"]

    missing_columns = [col for col in required_columns if col not in history.columns]

    if missing_columns:
        raise ValueError(
            "Missing required columns: " + ", ".join(missing_columns)
        )

    history["municipality_id"] = pd.to_numeric(
        history["municipality_id"],
        errors="coerce",
    ).astype("Int64")

    history["lon"] = pd.to_numeric(history["lon"], errors="coerce")
    history["lat"] = pd.to_numeric(history["lat"], errors="coerce")
    history["captured_at"] = pd.to_datetime(history["captured_at"], errors="coerce")

    municipality_stats = (
        history.dropna(subset=["municipality_id"])
        .groupby("municipality_id", as_index=False)
        .agg(
            avg_lon=("lon", "mean"),
            avg_lat=("lat", "mean"),
            outage_records_count=("municipality_id", "count"),
            first_seen_at=("captured_at", "min"),
            last_seen_at=("captured_at", "max"),
        )
    )

    municipality_stats["municipality_id"] = municipality_stats["municipality_id"].astype(int)

    municipality_stats["avg_lon"] = municipality_stats["avg_lon"].round(6)
    municipality_stats["avg_lat"] = municipality_stats["avg_lat"].round(6)

    municipality_stats = municipality_stats.sort_values("municipality_id")

    if REFERENCE_FILE.exists():
        existing = pd.read_csv(REFERENCE_FILE, low_memory=False)

        if "municipality_id" not in existing.columns:
            raise ValueError("Existing municipalities.csv must contain municipality_id")

        existing["municipality_id"] = pd.to_numeric(
            existing["municipality_id"],
            errors="coerce",
        ).astype("Int64")

        existing = existing.dropna(subset=["municipality_id"]).copy()
        existing["municipality_id"] = existing["municipality_id"].astype(int)

        keep_columns = [
            col
            for col in [
                "municipality_id",
                "municipality_name",
                "region_name",
            ]
            if col in existing.columns
        ]

        existing = existing[keep_columns].drop_duplicates("municipality_id")

        output = municipality_stats.merge(
            existing,
            on="municipality_id",
            how="left",
        )
    else:
        output = municipality_stats.copy()
        output["municipality_name"] = ""
        output["region_name"] = ""

    output["municipality_name"] = output["municipality_name"].fillna("").astype(str).str.strip()
    output["region_name"] = output["region_name"].fillna("").astype(str).str.strip()

    output["municipality_label"] = output.apply(
        lambda row: row["municipality_name"]
        if row["municipality_name"]
        else f"Municipalité {row['municipality_id']}",
        axis=1,
    )

    ordered_columns = [
        "municipality_id",
        "municipality_name",
        "municipality_label",
        "region_name",
        "avg_lon",
        "avg_lat",
        "outage_records_count",
        "first_seen_at",
        "last_seen_at",
    ]

    output = output[ordered_columns]

    output.to_csv(REFERENCE_FILE, index=False)

    print(f"Municipality reference file created: {REFERENCE_FILE}")
    print(f"Municipalities found: {len(output):,}")
    print("Fill municipality_name manually or from an official reference source.")


if __name__ == "__main__":
    main()