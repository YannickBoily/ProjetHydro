from __future__ import annotations

import argparse
import gzip
import json
import shutil
from pathlib import Path

try:
    from scripts.archive_snapshot import sha256_file
except ModuleNotFoundError:  # direct execution: python scripts/restore_snapshot_archive.py
    from archive_snapshot import sha256_file

DEFAULT_SNAPSHOT_FILE = Path("data/raw/current_snapshot.csv")
DEFAULT_METADATA_FILE = Path("data/raw/current_snapshot_meta.json")


def restore_snapshot_archive(
    manifest_path: Path,
    snapshot_output: Path = DEFAULT_SNAPSHOT_FILE,
    metadata_output: Path = DEFAULT_METADATA_FILE,
) -> tuple[Path, Path]:
    """Restore and verify one archived snapshot for replay through the normal sync."""
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Archive manifest must be a JSON object.")

    archive_name = payload.get("archive_file")
    if not archive_name:
        raise RuntimeError("Archive manifest does not contain archive_file.")

    archive_path = manifest_path.parent / str(archive_name)
    if not archive_path.exists():
        raise FileNotFoundError(f"Archived snapshot not found: {archive_path}")

    expected_archive_hash = payload.get("archive_sha256")
    if expected_archive_hash and sha256_file(archive_path) != expected_archive_hash:
        raise RuntimeError("Archived snapshot checksum mismatch.")

    snapshot_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    temp_snapshot = snapshot_output.with_suffix(f"{snapshot_output.suffix}.tmp")

    with gzip.open(archive_path, "rb") as source, temp_snapshot.open("wb") as target:
        shutil.copyfileobj(source, target)

    expected_source_hash = payload.get("source_csv_sha256")
    if expected_source_hash and sha256_file(temp_snapshot) != expected_source_hash:
        temp_snapshot.unlink(missing_ok=True)
        raise RuntimeError("Restored snapshot checksum mismatch.")

    temp_snapshot.replace(snapshot_output)

    # Extra archive fields are harmless to the normal sync loader and useful
    # for traceability during a manual replay.
    metadata_output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"✅ Restored snapshot: {snapshot_output}")
    print(f"✅ Restored metadata: {metadata_output}")
    return snapshot_output, metadata_output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Restore a ProjetHydro GitHub artifact snapshot for replay."
    )
    parser.add_argument("manifest", type=Path, help="Path to the archived .json manifest")
    parser.add_argument(
        "--snapshot-output",
        type=Path,
        default=DEFAULT_SNAPSHOT_FILE,
    )
    parser.add_argument(
        "--metadata-output",
        type=Path,
        default=DEFAULT_METADATA_FILE,
    )
    args = parser.parse_args()

    restore_snapshot_archive(
        args.manifest,
        snapshot_output=args.snapshot_output,
        metadata_output=args.metadata_output,
    )


if __name__ == "__main__":
    main()
