"""
Génère un fichier de référence des municipalités à partir de l'historique
des pannes d'Hydro-Québec.

Le script :
1. charge l'historique brut des pannes ;
2. valide les colonnes nécessaires ;
3. calcule des statistiques par municipalité ;
4. conserve les noms et régions déjà renseignés ;
5. exporte le résultat dans data/reference/municipalities.csv.

Les noms de municipalités absents devront être complétés manuellement
ou à partir d'une source officielle.
"""

from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Configuration des fichiers
# ---------------------------------------------------------------------------

RAW_FILE = Path("data/raw/hydroquebec_history.csv")
REFERENCE_DIR = Path("data/reference")
REFERENCE_FILE = REFERENCE_DIR / "municipalities.csv"

# Colonnes indispensables au calcul des statistiques municipales.
REQUIRED_COLUMNS = [
    "municipality_id",
    "lon",
    "lat",
    "captured_at",
]

# Colonnes descriptives à conserver lorsqu'un fichier de référence existe déjà.
OPTIONAL_REFERENCE_COLUMNS = [
    "municipality_name",
    "region_name",
]


def load_history(path: Path) -> pd.DataFrame:
    """
    Charge et valide le fichier historique des pannes.

    Args:
        path: Chemin du fichier CSV contenant l'historique.

    Returns:
        Le DataFrame nettoyé et prêt à être agrégé.

    Raises:
        FileNotFoundError: Si le fichier historique n'existe pas.
        ValueError: Si une colonne obligatoire est absente.
    """
    if not path.exists():
        raise FileNotFoundError(f"Fichier historique introuvable : {path}")

    history = pd.read_csv(path, low_memory=False)

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in history.columns
    ]

    if missing_columns:
        raise ValueError(
            "Colonnes obligatoires manquantes : "
            + ", ".join(missing_columns)
        )

    # Conversion des identifiants en entiers nullable.
    # Les valeurs invalides deviennent <NA> au lieu de provoquer une erreur.
    history["municipality_id"] = pd.to_numeric(
        history["municipality_id"],
        errors="coerce",
    ).astype("Int64")

    # Conversion des coordonnées en valeurs numériques.
    history["lon"] = pd.to_numeric(
        history["lon"],
        errors="coerce",
    )
    history["lat"] = pd.to_numeric(
        history["lat"],
        errors="coerce",
    )

    # Conversion des dates invalides en NaT.
    history["captured_at"] = pd.to_datetime(
        history["captured_at"],
        errors="coerce",
    )

    return history


def calculate_municipality_stats(history: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques historiques de chaque municipalité.

    Une ligne est produite par identifiant municipal avec :
    - les coordonnées moyennes ;
    - le nombre d'observations ;
    - la première date d'observation ;
    - la dernière date d'observation.

    Args:
        history: Historique nettoyé des pannes.

    Returns:
        Un DataFrame contenant une ligne par municipalité.
    """
    municipality_stats = (
        history
        .dropna(subset=["municipality_id"])
        .groupby("municipality_id", as_index=False)
        .agg(
            avg_lon=("lon", "mean"),
            avg_lat=("lat", "mean"),
            outage_records_count=("municipality_id", "size"),
            first_seen_at=("captured_at", "min"),
            last_seen_at=("captured_at", "max"),
        )
    )

    # Les identifiants ne contiennent plus de valeurs nulles après le dropna.
    municipality_stats["municipality_id"] = (
        municipality_stats["municipality_id"].astype(int)
    )

    # Six décimales offrent une précision géographique suffisante
    # pour l'affichage cartographique.
    municipality_stats["avg_lon"] = (
        municipality_stats["avg_lon"].round(6)
    )
    municipality_stats["avg_lat"] = (
        municipality_stats["avg_lat"].round(6)
    )

    return municipality_stats.sort_values(
        "municipality_id"
    ).reset_index(drop=True)


def load_existing_reference(path: Path) -> pd.DataFrame:
    """
    Charge les informations descriptives déjà renseignées.

    Le fichier peut contenir les colonnes municipality_name et region_name.
    Lorsqu'une colonne facultative est absente, elle est créée vide.

    Args:
        path: Chemin du fichier de référence existant.

    Returns:
        Un DataFrame contenant les informations descriptives disponibles.

    Raises:
        ValueError: Si municipality_id est absente du fichier existant.
    """
    existing = pd.read_csv(path, low_memory=False)

    if "municipality_id" not in existing.columns:
        raise ValueError(
            "Le fichier municipalities.csv existant doit contenir "
            "la colonne municipality_id."
        )

    existing["municipality_id"] = pd.to_numeric(
        existing["municipality_id"],
        errors="coerce",
    ).astype("Int64")

    # Retrait des lignes dont l'identifiant municipal est invalide.
    existing = existing.dropna(
        subset=["municipality_id"]
    ).copy()

    existing["municipality_id"] = (
        existing["municipality_id"].astype(int)
    )

    # Création des colonnes facultatives manquantes afin d'éviter
    # les erreurs lors du nettoyage et de la fusion.
    for column in OPTIONAL_REFERENCE_COLUMNS:
        if column not in existing.columns:
            existing[column] = ""

    columns_to_keep = [
        "municipality_id",
        *OPTIONAL_REFERENCE_COLUMNS,
    ]

    return (
        existing[columns_to_keep]
        .drop_duplicates("municipality_id", keep="first")
    )


def merge_reference_data(
    municipality_stats: pd.DataFrame,
    reference_path: Path,
) -> pd.DataFrame:
    """
    Fusionne les statistiques avec les informations descriptives existantes.

    Les noms et régions déjà renseignés sont conservés. Si aucun fichier de
    référence n'existe, les colonnes descriptives sont initialisées à vide.

    Args:
        municipality_stats: Statistiques calculées par municipalité.
        reference_path: Chemin du fichier de référence.

    Returns:
        Le DataFrame fusionné.
    """
    if reference_path.exists():
        existing_reference = load_existing_reference(reference_path)

        return municipality_stats.merge(
            existing_reference,
            on="municipality_id",
            how="left",
        )

    output = municipality_stats.copy()
    output["municipality_name"] = ""
    output["region_name"] = ""

    return output


def prepare_output(output: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les libellés et organise les colonnes finales.

    Lorsqu'un nom de municipalité est absent, un libellé générique basé sur
    l'identifiant est utilisé.

    Args:
        output: DataFrame contenant les statistiques et les descriptions.

    Returns:
        Le DataFrame final prêt à être exporté.
    """
    for column in OPTIONAL_REFERENCE_COLUMNS:
        if column not in output.columns:
            output[column] = ""

        output[column] = (
            output[column]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    # Utilisation du nom officiel lorsqu'il est disponible.
    # Sinon, création d'un libellé temporaire explicite.
    output["municipality_label"] = output.apply(
        lambda row: (
            row["municipality_name"]
            if row["municipality_name"]
            else f"Municipalité {row['municipality_id']}"
        ),
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

    return output[ordered_columns]


def main() -> None:
    """Exécute la génération complète du fichier de référence."""
    # Création du dossier de destination lorsqu'il n'existe pas.
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

    # Chargement et normalisation de l'historique.
    history = load_history(RAW_FILE)

    # Calcul des statistiques municipales.
    municipality_stats = calculate_municipality_stats(history)

    # Conservation des noms et régions renseignés lors d'une exécution passée.
    output = merge_reference_data(
        municipality_stats,
        REFERENCE_FILE,
    )

    # Nettoyage et organisation des colonnes finales.
    output = prepare_output(output)

    # Export sans colonne d'index Pandas.
    output.to_csv(
        REFERENCE_FILE,
        index=False,
        encoding="utf-8",
    )

    print(f"Fichier de référence créé : {REFERENCE_FILE}")
    print(f"Municipalités trouvées : {len(output):,}")
    print(
        "Complète municipality_name et region_name manuellement "
        "ou à partir d'une source officielle."
    )


if __name__ == "__main__":
    main()
