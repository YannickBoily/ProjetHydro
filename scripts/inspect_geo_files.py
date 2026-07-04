from pathlib import Path

import geopandas as gpd


GEO_DIR = Path("data/geo")

SHAPEFILES = [
    "munic_s.shp",
    "mrc_s.shp",
    "regio_s.shp",
    "arron_s.shp",
]


def inspect_shapefile(path: Path) -> None:
    print("=" * 80)
    print(f"File: {path}")
    print("=" * 80)

    if not path.exists():
        print("File not found.")
        return

    gdf = gpd.read_file(path)

    print(f"Rows: {len(gdf):,}")
    print(f"CRS: {gdf.crs}")
    print("\nColumns:")
    for col in gdf.columns:
        print(f" - {col}")

    print("\nFirst rows:")
    print(gdf.drop(columns="geometry", errors="ignore").head())

    print("\nGeometry types:")
    print(gdf.geometry.geom_type.value_counts())


def main() -> None:
    for file_name in SHAPEFILES:
        inspect_shapefile(GEO_DIR / file_name)


if __name__ == "__main__":
    main()