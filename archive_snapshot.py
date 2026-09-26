from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.time_utils import normalize_timestamp, UTC_TIMEZONE
except ModuleNotFoundError:  # direct execution: python scripts/archive_snapshot.py
    from time_utils import normalize_timestamp, UTC_TIMEZONE

CURRENT_SNAPSHOT_FILE = Path("data/raw/current_snapshot.csv")
CURRENT_SNAPSHOT_META_FILE = Path("data/raw/current_snapshot_meta.json")
DEFAULT_ARCHIVE_ROOT = Path("data/raw/archive")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_metadata(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to read snapshot metadata: {path}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Snapshot metadata must be a JSON object.")

    required = {"snapshot_id", "captured_at", "outage_count"}
    missing = required.difference(payload)
    if missing:
        raise RuntimeError(
            "Snapshot metadata is missing required fields: "
            + ", ".join(sorted(missing))
        )
    return payload


def archive_snapshot(
    snapshot_file: Path = CURRENT_SNAPSHOT_FILE,
    metadata_file: Path = CURRENT_SNAPSHOT_META_FILE,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
) -> tuple[Path, Path]:
    """Create an immutable gzip snapshot plus a checksum-bearing manifest."""
    if not snapshot_file.exists():
        raise FileNotFoundError(f"Snapshot CSV not found: {snapshot_file}")
    if not metadata_file.exists():
        raise FileNotFoundError(f"Snapshot metadata not found: {metadata_file}")

    metadata = load_metadata(metadata_file)
    captured_at = normalize_timestamp(
        metadata["captured_at"],
        naive_timezone=UTC_TIMEZONE,
    )
    if pd.isna(captured_at):
        raise RuntimeError("Snapshot metadata contains an invalid captured_at value.")

    snapshot_id = str(metadata["snapshot_id"]).strip()
    if not snapshot_id:
        raise RuntimeError("Snapshot metadata contains an empty snapshot_id.")

    # Verify the file before preserving it. Empty outage snapshots still contain
    # CSV headers and therefore remain valid archives.
    row_count = len(pd.read_csv(snapshot_file, low_memory=False))
    expected_count = int(metadata["outage_count"])
    if row_count != expected_count:
        raise RuntimeError(
            "Snapshot row count does not match metadata before archive: "
            f"CSV={row_count:,}, manifest={expected_count:,}."
        )

    archive_dir = archive_root / captured_at.strftime("%Y/%m/%d")
    archive_dir.mkdir(parents=True, exist_ok=True)

    stem = f"{captured_at.strftime('%Y%m%dT%H%M%S%fZ')}_{snapshot_id}"
    csv_gz_path = archive_dir / f"{stem}.csv.gz"
    manifest_path = archive_dir / f"{stem}.json"

    with snapshot_file.open("rb") as source, gzip.open(csv_gz_path, "wb") as target:
        shutil.copyfileobj(source, target)

    archived_metadata = dict(metadata)
    archived_metadata.update(
        {
            "archive_format": "csv.gz",
            "source_csv_sha256": sha256_file(snapshot_file),
            "archive_sha256": sha256_file(csv_gz_path),
            "archive_file": csv_gz_path.name,
        }
    )
    manifest_path.write_text(
        json.dumps(archived_metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"✅ Snapshot archive: {csv_gz_path}")
    print(f"✅ Archive manifest: {manifest_path}")
    return csv_gz_path, manifest_path


def main() -> None:
    archive_root = Path(os.environ.get("HYDRO_ARCHIVE_DIR", str(DEFAULT_ARCHIVE_ROOT)))
    archive_snapshot(archive_root=archive_root)


if __name__ == "__main__":
    main()
