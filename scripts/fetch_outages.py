from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from scripts.time_utils import normalize_hydro_local_series
except ModuleNotFoundError:  # direct execution: python scripts/fetch_outages.py
    from time_utils import normalize_hydro_local_series


VERSION_URL = "https://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
DATA_URL_TEMPLATE = "https://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{version}.json"

CURRENT_SNAPSHOT_FILE = Path("data/raw/current_snapshot.csv")
CURRENT_SNAPSHOT_META_FILE = Path("data/raw/current_snapshot_meta.json")
LOCAL_HISTORY_FILE = Path("data/raw/hydroquebec_history.csv")
SNAPSHOT_METADATA_ATTR = "snapshot_metadata"

HTTP_TIMEOUT = (5, 20)
HTTP_RETRY_TOTAL = 4
HTTP_RETRY_BACKOFF_SECONDS = 0.75
HTTP_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
HTTP_USER_AGENT = "ProjetHydro/1.0 (+https://github.com/)"


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


def build_http_session() -> requests.Session:
    """Create a resilient HTTP session for Hydro-Québec public endpoints."""
    retry = Retry(
        total=HTTP_RETRY_TOTAL,
        connect=HTTP_RETRY_TOTAL,
        read=HTTP_RETRY_TOTAL,
        status=HTTP_RETRY_TOTAL,
        backoff_factor=HTTP_RETRY_BACKOFF_SECONDS,
        status_forcelist=HTTP_RETRY_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": HTTP_USER_AGENT,
            "Accept": "application/json,text/plain;q=0.9,*/*;q=0.1",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def validate_json_content_type(response: requests.Response) -> None:
    """Reject an explicit non-JSON content type for the outage payload."""
    content_type = str(response.headers.get("Content-Type", "")).lower()
    if content_type and "json" not in content_type:
        raise RuntimeError(
            "Réponse Hydro-Québec invalide : Content-Type JSON attendu "
            f"(reçu: {content_type})."
        )


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


def _snapshot_metadata(
    *,
    snapshot_id: str,
    captured_at: datetime,
    source_version: str,
    outage_count: int,
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, object]:
    """Build the metadata persisted beside the current snapshot CSV."""
    return {
        "snapshot_id": snapshot_id,
        "captured_at": captured_at.isoformat(),
        "source_version": source_version,
        "status": "success",
        "outage_count": int(outage_count),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "error_message": None,
    }


def fetch_current_outages(session: requests.Session | None = None) -> pd.DataFrame:
    """Télécharge un snapshot Hydro-Québec et retourne un DataFrame normalisé.

    Un seul timestamp et un seul ``snapshot_id`` sont créés pour tout le batch.
    Un tableau ``pannes`` vide est un état valide : il signifie qu'aucune panne
    n'est active au moment de la collecte. En revanche, un payload absent ou
    entièrement malformé est traité comme une erreur de source.
    """
    started_at = datetime.now(timezone.utc)
    http = session or build_http_session()

    response = http.get(VERSION_URL, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    version = response.text.strip().strip('"').strip()
    if not version:
        raise RuntimeError("Réponse Hydro-Québec invalide : version BIS vide.")

    captured_at = datetime.now(timezone.utc)
    captured_at_text = captured_at.isoformat()
    snapshot_id = uuid.uuid4().hex

    print(f"[{captured_at.strftime('%H:%M:%S')} UTC] Version BIS : {version}")
    print(f"Snapshot ID : {snapshot_id}")

    data_url = DATA_URL_TEMPLATE.format(version=version)
    response = http.get(data_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    validate_json_content_type(response)
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError("Réponse Hydro-Québec invalide : JSON illisible.") from exc

    if not isinstance(data, dict):
        raise RuntimeError("Réponse Hydro-Québec invalide : objet JSON attendu.")

    outages = data.get("pannes")
    if not isinstance(outages, list):
        raise RuntimeError(
            "Réponse Hydro-Québec invalide : le champ 'pannes' est absent ou non valide."
        )

    rows = []
    malformed_count = 0

    for outage in outages:
        if not isinstance(outage, list) or len(outage) < 9:
            malformed_count += 1
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

    if outages and not rows:
        raise RuntimeError(
            "Le payload Hydro-Québec contient des pannes, mais aucune ligne n'a pu être parsée."
        )

    if rows:
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

        # Hydro publishes these values as Quebec wall-clock times when no
        # offset is present. Persist them as aware UTC timestamps so new
        # snapshots are unambiguous across DST boundaries.
        df["start_time"] = normalize_hydro_local_series(df["start_time"])
        df["estimated_restore"] = normalize_hydro_local_series(df["estimated_restore"])
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
    else:
        # A successful empty snapshot must still be persisted with headers so
        # downstream jobs can distinguish "0 panne" from "fetch failed".
        df = pd.DataFrame(columns=EXPECTED_COLUMNS)

    finished_at = datetime.now(timezone.utc)
    metadata = _snapshot_metadata(
        snapshot_id=snapshot_id,
        captured_at=captured_at,
        source_version=version,
        outage_count=len(df),
        started_at=started_at,
        finished_at=finished_at,
    )
    df.attrs[SNAPSHOT_METADATA_ATTR] = metadata

    if malformed_count:
        print(f"⚠️ Lignes malformées ignorées : {malformed_count:,}")

    if df.empty:
        print("✅ Snapshot valide : 0 panne active.")

    return df


def metadata_path_for_snapshot(output_file: Path) -> Path:
    """Return the metadata sidecar path associated with a snapshot CSV."""
    return output_file.with_name(f"{output_file.stem}_meta.json")


def write_snapshot_metadata(
    metadata: dict[str, object],
    output_file: Path = CURRENT_SNAPSHOT_META_FILE,
) -> None:
    """Persist snapshot metadata atomically."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = output_file.with_suffix(f"{output_file.suffix}.tmp")
    temp_file.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp_file.replace(output_file)


def write_current_snapshot(
    df: pd.DataFrame,
    output_file: Path = CURRENT_SNAPSHOT_FILE,
) -> None:
    """Écrit le snapshot courant et son manifeste de collecte.

    Le manifeste est indispensable pour représenter un snapshot valide de zéro
    ligne : le CSV seul ne peut pas porter ``snapshot_id`` ou ``captured_at``
    lorsqu'il est vide.
    """
    metadata = df.attrs.get(SNAPSHOT_METADATA_ATTR)
    if not isinstance(metadata, dict):
        raise ValueError("Snapshot metadata missing from DataFrame attributes.")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = output_file.with_suffix(f"{output_file.suffix}.tmp")
    df.to_csv(temp_file, index=False)
    temp_file.replace(output_file)

    metadata_file = metadata_path_for_snapshot(output_file)
    write_snapshot_metadata(metadata, metadata_file)

    print(f"✅ Snapshot courant : {len(df):,} lignes -> {output_file}")
    print(f"✅ Manifeste snapshot -> {metadata_file}")


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

    # Un snapshot vide est bien un événement de collecte, mais il n'ajoute
    # naturellement aucune observation au CSV historique ligne-par-ligne.
    if df.empty and file_exists:
        print(f"✅ Snapshot vide : aucune ligne ajoutée à l'historique -> {output_file}")
        return

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
