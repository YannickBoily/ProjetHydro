from pathlib import Path

import geopandas as gpd
import pandas as pd


RAW_HISTORY_FILE = Path("data/raw/hydroquebec_history.csv")
MUNICIPALITIES_SHP = Path("data/geo/munic_s.shp")

REFERENCE_DIR = Path("data/reference")
OUTPUT_FILE = REFERENCE_DIR / "municipalities.csv"


def load_outage_points() -> gpd.GeoDataFrame:
    """Load Hydro-Québec outage points from the raw history CSV."""
    if not RAW_HISTORY_FILE.exists():
        raise FileNotFoundError(f"Raw history file not found: {RAW_HISTORY_FILE}")

    history = pd.read_csv(RAW_HISTORY_FILE, low_memory=False)

    required_columns = ["municipality_id", "lon", "lat", "captured_at"]

    missing_columns = [
        col for col in required_columns
        if col not in history.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns in raw history: "
            + ", ".join(missing_columns)
        )

    history["municipality_id"] = pd.to_numeric(
        history["municipality_id"],
        errors="coerce",
    ).astype("Int64")

    history["lon"] = pd.to_numeric(history["lon"], errors="coerce")
    history["lat"] = pd.to_numeric(history["lat"], errors="coerce")
    history["captured_at"] = pd.to_datetime(history["captured_at"], errors="coerce")

    history = history.dropna(subset=["municipality_id", "lon", "lat"]).copy()

    history["municipality_id"] = history["municipality_id"].astype(int)

    points = gpd.GeoDataFrame(
        history,
        geometry=gpd.points_from_xy(history["lon"], history["lat"]),
        crs="EPSG:4326",
    )

    return points


def load_municipality_polygons() -> gpd.GeoDataFrame:
    """Load official Quebec municipality polygons."""
    if not MUNICIPALITIES_SHP.exists():
        raise FileNotFoundError(f"Municipality shapefile not found: {MUNICIPALITIES_SHP}")

    municipalities = gpd.read_file(MUNICIPALITIES_SHP)

    expected_columns = [
        "MUS_CO_GEO",
        "MUS_NM_MUN",
        "MUS_NM_NMC",
        "MUS_CO_MRC",
        "MUS_NM_MRC",
        "MUS_CO_REG",
        "MUS_NM_REG",
        "MUS_CO_DES",
    ]

    missing_columns = [
        col for col in expected_columns
        if col not in municipalities.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing expected columns in shapefile: "
            + ", ".join(missing_columns)
        )

    municipalities = municipalities[
        expected_columns + ["geometry"]
    ].copy()

    municipalities = municipalities.rename(
        columns={
            "MUS_CO_GEO": "geo_municipality_code",
            "MUS_NM_MUN": "municipality_name",
            "MUS_NM_NMC": "municipality_full_name",
            "MUS_CO_MRC": "mrc_code",
            "MUS_NM_MRC": "mrc_name",
            "MUS_CO_REG": "region_code",
            "MUS_NM_REG": "region_name",
            "MUS_CO_DES": "municipality_type_code",
        }
    )

    municipalities["geo_municipality_code"] = pd.to_numeric(
        municipalities["geo_municipality_code"],
        errors="coerce",
    ).astype("Int64")

    municipalities = municipalities.dropna(subset=["geo_municipality_code"]).copy()
    municipalities["geo_municipality_code"] = municipalities["geo_municipality_code"].astype(int)

    return municipalities


def build_reference(points: gpd.GeoDataFrame, polygons: gpd.GeoDataFrame) -> pd.DataFrame:
    """Spatially match Hydro outage points to official municipality polygons."""
    polygons = polygons.to_crs(points.crs)

    joined = gpd.sjoin(
        points,
        polygons,
        how="left",
        predicate="within",
    )

    # One output row per Hydro municipality_id.
    # We choose the most frequent spatial match for each Hydro municipality_id.
    matched_counts = (
        joined
        .dropna(subset=["municipality_id"])
        .groupby(
            [
                "municipality_id",
                "geo_municipality_code",
                "municipality_name",
                "municipality_full_name",
                "mrc_code",
                "mrc_name",
                "region_code",
                "region_name",
                "municipality_type_code",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(
            matched_records_count=("municipality_id", "count"),
        )
    )

    matched_counts = matched_counts.sort_values(
        ["municipality_id", "matched_records_count"],
        ascending=[True, False],
    )

    best_match = matched_counts.drop_duplicates(
        subset=["municipality_id"],
        keep="first",
    )

    hydro_stats = (
        points
        .groupby("municipality_id", as_index=False)
        .agg(
            avg_lon=("lon", "mean"),
            avg_lat=("lat", "mean"),
            outage_records_count=("municipality_id", "count"),
            first_seen_at=("captured_at", "min"),
            last_seen_at=("captured_at", "max"),
        )
    )

    output = hydro_stats.merge(
        best_match,
        on="municipality_id",
        how="left",
    )

    output["avg_lon"] = output["avg_lon"].round(6)
    output["avg_lat"] = output["avg_lat"].round(6)

    text_columns = [
        "municipality_name",
        "municipality_full_name",
        "mrc_name",
        "region_name",
        "municipality_type_code",
    ]

    for col in text_columns:
        output[col] = output[col].fillna("").astype(str).str.strip()

    output["municipality_label"] = output.apply(
        lambda row: row["municipality_name"]
        if row["municipality_name"]
        else f"Municipalité {int(row['municipality_id'])}",
        axis=1,
    )

    output["is_geocoded"] = output["municipality_name"] != ""

    output["match_rate_pct"] = (
        output["matched_records_count"].fillna(0)
        * 100
        / output["outage_records_count"]
    ).round(2)

    output = output[
        [
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
    ].sort_values("outage_records_count", ascending=False)

    return output


def main() -> None:
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading outage points...")
    points = load_outage_points()

    print("Loading municipality polygons...")
    polygons = load_municipality_polygons()

    print("Building municipality reference with spatial join...")
    reference = build_reference(points, polygons)

    reference.to_csv(OUTPUT_FILE, index=False)

    total = len(reference)
    geocoded = int(reference["is_geocoded"].sum())
    geocoded_rate = round(geocoded * 100 / total, 2) if total else 0

    print(f"Created: {OUTPUT_FILE}")
    print(f"Hydro municipality IDs found: {total:,}")
    print(f"Geocoded IDs: {geocoded:,}")
    print(f"Geocoded rate: {geocoded_rate}%")

    unmatched = reference[~reference["is_geocoded"]]

    if not unmatched.empty:
        print("\nUnmatched municipality IDs:")
        print(
            unmatched[
                [
                    "municipality_id",
                    "municipality_label",
                    "outage_records_count",
                    "avg_lon",
                    "avg_lat",
                ]
            ].head(20)
        )


if __name__ == "__main__":
    main()