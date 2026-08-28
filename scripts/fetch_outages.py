from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


VERSION_URL = "https://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
DATA_URL_TEMPLATE = "https://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{version}.json"

CURRENT_SNAPSHOT_FILE = Path("data/raw/current_snapshot.csv")
LOCAL_HISTORY_FILE = Path("data/raw/hydroquebec_history.csv")


EXPECTED_COLUMNS = [
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


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def safe_get(arr, idx):
    """Retourne arr[idx] ou None si index absent."""
    try:
        return arr[idx]
    except (IndexError, TypeError):
        return None


def classify_cause(code):
    """Classification des causes Hydro-Québec."""
    try:
        if code is None or code == "":
            return "unknown"

        c = int(float(code))

        if 11 <= c <= 15 or c in [58, 70, 72, 73, 74, 79]:
            return "equipment"
        if 21 <= c <= 26:
            return "weather"
        if c == 51:
            return "vegetation"
        if c in [52, 53]:
            return "animal"
        if 31 <= c <= 34 or c in [41, 42, 43, 44, 54, 55, 56, 57]:
            return "accident"

        return "other"
    except (TypeError, ValueError):
        return "unknown"


def fetch_current_outages() -> pd.DataFrame:
    """Télécharge un snapshot Hydro-Québec et retourne un DataFrame normalisé.

    Un seul timestamp est créé pour tout le snapshot. Cela permet de traiter la
    collecte comme un batch atomique côté PostgreSQL et évite d'avoir une date
    légèrement différente pour chaque panne.
    """
    response = requests.get(VERSION_URL, timeout=10)
    response.raise_for_status()
    version = response.text.strip('"')

    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    captured_at_text = captured_at.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{captured_at.strftime('%H:%M:%S')} UTC] Version BIS : {version}")

    data_url = DATA_URL_TEMPLATE.format(version=version)
    response = requests.get(data_url, timeout=10)
    response.raise_for_status()
    data = response.json()

    rows = []
    for outage in data.get("pannes", []):
        if not isinstance(outage, list) or len(outage) < 9:
            print("⚠️ Ligne ignorée (malformée):", outage)
            continue

        coordinates = safe_get(outage, 4)

        rows.append(
            {
                "outage_id": f"{safe_get(outage, 8)}_{coordinates}_{safe_get(outage, 1)}",
                "customers_affected": safe_get(outage, 0),
                "start_time": safe_get(outage, 1),
                "estimated_restore": safe_get(outage, 2),
                "status_code": safe_get(outage, 5),
                "cause_code": safe_get(outage, 7),
                "municipality_id": safe_get(outage, 8),
                "coordinates": coordinates,
                "captured_at": captured_at_text,
            }
        )

    if not rows:
        raise RuntimeError("Aucune donnée de panne récupérée depuis Hydro-Québec.")

    df = pd.DataFrame(rows)

    coords = (
        df["coordinates"]
        .astype(str)
        .str.strip("[]")
        .str.split(",", n=1, expand=True)
    )
    df["lon"] = pd.to_numeric(coords[0], errors="coerce")
    df["lat"] = pd.to_numeric(coords[1], errors="coerce")
    df.drop(columns=["coordinates"], inplace=True)

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["estimated_restore"] = pd.to_datetime(df["estimated_restore"], errors="coerce")
    df["cause_label"] = df["cause_code"].apply(classify_cause)

    status_map = {
        "A": "assigned",
        "L": "working",
        "R": "en_route",
        "N": "new",
    }
    df["status"] = df["status_code"].map(status_map)

    df = df.reindex(columns=EXPECTED_COLUMNS)
    df = df.drop_duplicates(subset=["outage_id", "captured_at"], keep="last")

    return df


def write_current_snapshot(
    df: pd.DataFrame,
    output_file: Path = CURRENT_SNAPSHOT_FILE,
) -> None:
    """Écrit uniquement le petit snapshot courant (écrasé à chaque collecte)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"✅ Snapshot courant : {len(df):,} lignes -> {output_file}")


def append_local_history(
    df: pd.DataFrame,
    output_file: Path = LOCAL_HISTORY_FILE,
) -> None:
    """Ajoute le snapshot au CSV local sans relire/réécrire tout l'historique.

    Cette option est destinée au développement ou à une sauvegarde locale. En
    production, Supabase est le stockage historique et cette écriture est
    désactivée dans GitHub Actions pour limiter les I/O.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_file.exists() and output_file.stat().st_size > 0

    df.to_csv(
        output_file,
        mode="a" if file_exists else "w",
        header=not file_exists,
        index=False,
    )

    print(f"✅ Snapshot ajouté à l'historique local -> {output_file}")


def main() -> None:
    df = fetch_current_outages()
    write_current_snapshot(df)

    if env_flag("HYDRO_WRITE_LOCAL_HISTORY", default=False):
        append_local_history(df)
    else:
        print(
            "Historique CSV local désactivé. "
            "Supabase conserve l'historique en production."
        )


if __name__ == "__main__":
    main()
