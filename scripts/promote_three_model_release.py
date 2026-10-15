#!/usr/bin/env python
"""Promote one archived three-model AFT release for Streamlit inference.

The dashboard intentionally reads only ``ml/artifacts/three_model_production``.
Training a new candidate therefore never changes the public dashboard by itself.
Run this script after reviewing a release that you want to serve.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DIR = ROOT / "ml" / "artifacts" / "three_model_production"
MODEL_NAMES = ("initial_no_eta", "initial_first_eta", "live_latest_eta")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_release(source: Path) -> None:
    required = [source / "three_model_metadata.json"]
    for name in MODEL_NAMES:
        required.append(source / name / "candidate_model.json")
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("Release incomplète:\n- " + "\n- ".join(missing))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "release",
        type=Path,
        help="Dossier de release, ex. ml/releases/aft-3model-20261015-c783b769",
    )
    args = parser.parse_args()

    source = args.release.expanduser()
    if not source.is_absolute():
        source = (ROOT / source).resolve()
    validate_release(source)

    if PRODUCTION_DIR.exists():
        shutil.rmtree(PRODUCTION_DIR)
    PRODUCTION_DIR.mkdir(parents=True, exist_ok=True)

    shutil.copy2(source / "three_model_metadata.json", PRODUCTION_DIR / "three_model_metadata.json")
    for name in MODEL_NAMES:
        target_dir = PRODUCTION_DIR / name
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name / "candidate_model.json", target_dir / "candidate_model.json")

    metadata_path = PRODUCTION_DIR / "three_model_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    source_manifest_path = source / "model_run_manifest.json"
    source_manifest = (
        json.loads(source_manifest_path.read_text(encoding="utf-8"))
        if source_manifest_path.exists()
        else {}
    )
    release_id = source_manifest.get("model_release_id") or source.name

    manifest = {
        "production_release_id": release_id,
        "promoted_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_release_directory": str(source),
        "model_version": metadata.get("model_version"),
        "dataset_sha256": source_manifest.get("dataset_sha256", metadata.get("dataset", {}).get("sha256")),
        "dataset_cutoff_utc": source_manifest.get("dataset_cutoff_utc"),
        "metadata_sha256": sha256_file(metadata_path),
        "served_model_kind": "candidate",
    }
    (PRODUCTION_DIR / "production_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print("=== THREE-MODEL PRODUCTION PROMOTION COMPLETE ===")
    print(f"Release : {release_id}")
    print(f"Target  : {PRODUCTION_DIR}")
    print("Models  : initial_no_eta, initial_first_eta, live_latest_eta")
    print("\nCommit/push ml/artifacts/three_model_production so Streamlit Cloud can load it.")


if __name__ == "__main__":
    main()
