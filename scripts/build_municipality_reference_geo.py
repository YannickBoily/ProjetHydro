"""
Construit un référentiel des municipalités québécoises à partir des
coordonnées présentes dans l'historique des pannes d'Hydro-Québec.

Le script :

1. charge les observations de pannes depuis un fichier CSV ;
2. transforme les coordonnées en points géographiques ;
3. charge les polygones officiels des municipalités ;
4. associe chaque point au polygone qui le contient ;
5. sélectionne la municipalité la plus souvent associée à chaque
   identifiant municipal Hydro-Québec ;
6. exporte le résultat dans data/reference/municipalities.csv.
"""

from pathlib import Path
from typing import Iterable

import geopandas as gpd
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RAW_HISTORY_FILE = Path("data/raw/hydroquebec_history.csv")
MUNICIPALITIES_SHP = Path("data/geo/munic_s.shp")

REFERENCE_DIR = Path("data/reference")
OUTPUT_FILE = REFERENCE_DIR / "municipalities.csv"

HISTORY_REQUIRED_COLUMNS = [
    "municipality_id",
    "lon",
    "lat",
    "captured_at",
]

SHAPEFILE_REQUIRED_COLUMNS = [
    "MUS_CO_GEO",
    "MUS_NM_MUN",
    "MUS_NM_NMC",
    "MUS_CO_MRC",
    "MUS_NM_MRC",
    "MUS_CO_REG",
    "MUS_NM_REG",
    "MUS_CO_DES",
]


def validate_columns(
    dataframe: pd.DataFrame,
    required_columns: Iterable[str],
    source_name: str,
) -> None:
    """
    Vérifie que toutes les colonnes requises sont présentes.

    Args:
        dataframe: Table à valider.
        required_columns: Colonnes attendues.
        source_name: Nom de la source utilisé dans le message d'erreur.

    Raises:
        ValueError: Si une ou plusieurs colonnes sont absentes.
    """
    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Colonnes manquantes dans {source_name} : "
            + ", ".join(missing_columns)
        )


def load_outage_points() -> gpd.GeoDataFrame:
    """
    Charge l'historique des pannes et crée un point par observation.

    Les coordonnées sont interprétées en longitude et latitude selon
    le système WGS 84, soit EPSG:4326.

    Returns:
        Un GeoDataFrame contenant les observations valides.

    Raises:
        FileNotFoundError: Si le fichier historique n'existe pas.
        ValueError: Si les colonnes requises sont absentes ou si aucune
            observation géographique valide n'est disponible.
    """
    if not RAW_HISTORY_FILE.exists():
        raise FileNotFoundError(
            f"Fichier historique introuvable : {RAW_HISTORY_FILE}"
        )

    history = pd.read_csv(
        RAW_HISTORY_FILE,
        low_memory=False,
    )

    validate_columns(
        history,
        HISTORY_REQUIRED_COLUMNS,
        "l'historique brut",
    )

    # Les valeurs invalides sont converties en valeurs manquantes.
    history["municipality_id"] = pd.to_numeric(
        history["municipality_id"],
        errors="coerce",
    ).astype("Int64")

    history["lon"] = pd.to_numeric(
        history["lon"],
        errors="coerce",
    )

    history["lat"] = pd.to_numeric(
        history["lat"],
        errors="coerce",
    )

    history["captured_at"] = pd.to_datetime(
        history["captured_at"],
        errors="coerce",
        utc=True,
    )

    # Une observation sans identifiant ou coordonnées ne peut pas être
    # utilisée pour une jointure spatiale.
    history = history.dropna(
        subset=["municipality_id", "lon", "lat"]
    ).copy()

    # Élimination des coordonnées impossibles.
    valid_coordinates = (
        history["lon"].between(-180, 180)
        & history["lat"].between(-90, 90)
    )

    history = history.loc[valid_coordinates].copy()

    if history.empty:
        raise ValueError(
            "Aucune observation ne contient un identifiant municipal "
            "et des coordonnées valides."
        )

    history["municipality_id"] = (
        history["municipality_id"].astype(int)
    )

    # Cet identifiant technique permet de compter chaque observation
    # une seule fois après la jointure spatiale.
    history = history.reset_index(drop=True)
    history["_outage_record_id"] = history.index

    return gpd.GeoDataFrame(
        history,
        geometry=gpd.points_from_xy(
            history["lon"],
            history["lat"],
        ),
        crs="EPSG:4326",
    )


def load_municipality_polygons() -> gpd.GeoDataFrame:
    """
    Charge et normalise les polygones officiels des municipalités.

    Returns:
        Un GeoDataFrame contenant les attributs municipaux nécessaires.

    Raises:
        FileNotFoundError: Si le Shapefile n'existe pas.
        ValueError: Si des colonnes sont absentes, si aucun CRS n'est défini
            ou si aucun polygone valide n'est disponible.
    """
    if not MUNICIPALITIES_SHP.exists():
        raise FileNotFoundError(
            f"Fichier géographique introuvable : {MUNICIPALITIES_SHP}"
        )

    municipalities = gpd.read_file(MUNICIPALITIES_SHP)

    validate_columns(
        municipalities,
        SHAPEFILE_REQUIRED_COLUMNS,
        "le fichier des municipalités",
    )

    if municipalities.crs is None:
        raise ValueError(
            "Le fichier des municipalités ne définit aucun système "
            "de coordonnées géographiques."
        )

    municipalities = municipalities[
        SHAPEFILE_REQUIRED_COLUMNS + ["geometry"]
    ].copy()

    # Remplacement des noms propres au Shapefile par des noms plus explicites.
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

    # Suppression des lignes inutilisables pour la jointure spatiale.
    municipalities = municipalities.dropna(
        subset=["geo_municipality_code", "geometry"]
    ).copy()

    municipalities = municipalities.loc[
        ~municipalities.geometry.is_empty
    ].copy()

    if municipalities.empty:
        raise ValueError(
            "Aucun polygone municipal valide n'a été trouvé."
        )

    municipalities["geo_municipality_code"] = (
        municipalities["geo_municipality_code"].astype(int)
    )

    return municipalities


def select_best_match(
    joined: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Sélectionne la municipalité officielle la plus souvent associée
    à chaque identifiant municipal Hydro-Québec.

    Les observations sans correspondance sont exclues du classement.
    Cela évite qu'un groupe de points non appariés devienne la meilleure
    correspondance simplement parce qu'il est plus fréquent.

    Args:
        joined: Résultat de la jointure spatiale.

    Returns:
        Une table contenant au maximum une correspondance par identifiant.
    """
    match_columns = [
        "municipality_id",
        "geo_municipality_code",
        "municipality_name",
        "municipality_full_name",
        "mrc_code",
        "mrc_name",
        "region_code",
        "region_name",
        "municipality_type_code",
    ]

    matched = joined.dropna(
        subset=["geo_municipality_code"]
    ).copy()

    if matched.empty:
        return pd.DataFrame(
            columns=match_columns + ["matched_records_count"]
        )

    # Protection contre d'éventuels polygones dupliqués dans la source.
    matched = matched.drop_duplicates(
        subset=[
            "_outage_record_id",
            "geo_municipality_code",
        ]
    )

    matched_counts = (
        matched
        .groupby(
            match_columns,
            dropna=False,
            as_index=False,
        )
        .agg(
            matched_records_count=("_outage_record_id", "size"),
        )
    )

    # Le code municipal sert de critère déterministe en cas d'égalité.
    matched_counts = matched_counts.sort_values(
        [
            "municipality_id",
            "matched_records_count",
            "geo_municipality_code",
        ],
        ascending=[True, False, True],
    )

    return matched_counts.drop_duplicates(
        subset=["municipality_id"],
        keep="first",
    )


def calculate_hydro_stats(
    points: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Calcule les statistiques historiques par identifiant municipal Hydro.

    Args:
        points: Observations géographiques des pannes.

    Returns:
        Une table contenant une ligne par municipality_id.
    """
    statistics = (
        points
        .groupby("municipality_id", as_index=False)
        .agg(
            avg_lon=("lon", "mean"),
            avg_lat=("lat", "mean"),
            outage_records_count=("_outage_record_id", "size"),
            first_seen_at=("captured_at", "min"),
            last_seen_at=("captured_at", "max"),
        )
    )

    statistics["avg_lon"] = statistics["avg_lon"].round(6)
    statistics["avg_lat"] = statistics["avg_lat"].round(6)

    return statistics


def build_reference(
    points: gpd.GeoDataFrame,
    polygons: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Construit le référentiel municipal par jointure spatiale.

    Le prédicat ``within`` signifie qu'un point doit se trouver strictement
    à l'intérieur d'un polygone. Un point situé exactement sur une frontière
    municipale peut donc ne pas être associé.

    Args:
        points: Points représentant les observations de pannes.
        polygons: Polygones officiels des municipalités.

    Returns:
        Le référentiel final prêt à être exporté.
    """
    if points.crs is None:
        raise ValueError(
            "Le GeoDataFrame des points ne définit aucun CRS."
        )

    if polygons.crs is None:
        raise ValueError(
            "Le GeoDataFrame des polygones ne définit aucun CRS."
        )

    # La jointure spatiale exige que les deux couches utilisent le même CRS.
    polygons = polygons.to_crs(points.crs)

    joined = gpd.sjoin(
        points,
        polygons,
        how="left",
        predicate="within",
    )

    best_match = select_best_match(joined)
    hydro_stats = calculate_hydro_stats(points)

    output = hydro_stats.merge(
        best_match,
        on="municipality_id",
        how="left",
    )

    output["matched_records_count"] = (
        pd.to_numeric(
            output["matched_records_count"],
            errors="coerce",
        )
        .fillna(0)
        .astype(int)
    )

    # Nettoyage des colonnes textuelles provenant du Shapefile.
    text_columns = [
        "municipality_name",
        "municipality_full_name",
        "mrc_name",
        "region_name",
        "municipality_type_code",
    ]

    for column in text_columns:
        if column not in output.columns:
            output[column] = ""

        output[column] = (
            output[column]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    # Une ligne est considérée comme géocodée lorsqu'un code municipal
    # officiel lui a été attribué.
    output["is_geocoded"] = (
        output["geo_municipality_code"].notna()
    )

    fallback_labels = (
        "Municipalité "
        + output["municipality_id"].astype(str)
    )

    output["municipality_label"] = (
        output["municipality_name"].where(
            output["municipality_name"] != "",
            fallback_labels,
        )
    )

    # Pourcentage des observations Hydro associées à la municipalité retenue.
    output["match_rate_pct"] = (
        output["matched_records_count"]
        .div(output["outage_records_count"])
        .mul(100)
        .round(2)
    )

    ordered_columns = [
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

    return (
        output[ordered_columns]
        .sort_values(
            ["outage_records_count", "municipality_id"],
            ascending=[False, True],
        )
        .reset_index(drop=True)
    )


def main() -> None:
    """Exécute la construction et l'export du référentiel municipal."""
    REFERENCE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Chargement des points de panne...")
    points = load_outage_points()

    print("Chargement des polygones municipaux...")
    polygons = load_municipality_polygons()

    print("Construction du référentiel par jointure spatiale...")
    reference = build_reference(
        points,
        polygons,
    )

    reference.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8",
    )

    total = len(reference)
    geocoded = int(reference["is_geocoded"].sum())

    geocoded_rate = (
        geocoded * 100 / total
        if total
        else 0.0
    )

    print(f"Fichier créé : {OUTPUT_FILE}")
    print(f"Identifiants municipaux Hydro trouvés : {total:,}")
    print(f"Identifiants géocodés : {geocoded:,}")
    print(f"Taux de géocodage : {geocoded_rate:.2f} %")

    unmatched = reference.loc[
        ~reference["is_geocoded"]
    ]

    if not unmatched.empty:
        print("\nIdentifiants municipaux sans correspondance :")

        print(
            unmatched[
                [
                    "municipality_id",
                    "municipality_label",
                    "outage_records_count",
                    "avg_lon",
                    "avg_lat",
                ]
            ]
            .head(20)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
