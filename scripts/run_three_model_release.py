#!/usr/bin/env python
"""One-command clean release for the three-model AFT workflow and Tableau."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CURRENT_DIR = ROOT / "ml" / "artifacts" / "three_model_candidate"
RELEASES_DIR = ROOT / "ml" / "releases"
TABLEAU_DIR = ROOT / "tableau_exports"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def dataset_profile(path: Path) -> dict[str, Any]:
    rows = 0
    max_capture = None
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        capture_col = next((c for c in ("captured_at", "capture_at", "snapshot_at") if c in fields), None)
        for row in reader:
            rows += 1
            if capture_col:
                value = (row.get(capture_col) or "").strip()
                if value and (max_capture is None or value > max_capture):
                    max_capture = value
    return {"rows": rows, "max_captured_at": max_capture}


def git_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return None


def clean_label(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-") or "aft-three-model-release"


def nested(d: dict, *keys, default=None):
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: "" if row.get(k) is None else row.get(k, "") for k in fields})


def prepend_release_columns(source: Path, target: Path, release: dict[str, str]) -> None:
    frame = pd.read_csv(source, low_memory=False)
    for key, value in reversed(list(release.items())):
        frame.insert(0, key, value)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False)


def comparison_rows(metadata: dict, release: dict[str, str]) -> list[dict]:
    rows = []
    common = {
        **release,
        "test_start": nested(metadata, "shared_split", "test_start"),
        "validation_start": nested(metadata, "shared_split", "validation_start"),
    }
    for model in ("initial_no_eta", "initial_first_eta"):
        m = nested(metadata, "models", model, "metrics", default={}) or {}
        rows.append({
            **common,
            "comparison_scope": "all_initial_test_completed",
            "model": model,
            "prediction_mode": "initial",
            "uses_hydro_eta": model == "initial_first_eta",
            "eta_timing": "none" if model == "initial_no_eta" else "first_snapshot",
            "target": "total_duration_hours",
            "rows": m.get("completed_rows"),
            "mae_hours": m.get("midpoint_mae_hours"),
            "median_absolute_error_hours": m.get("midpoint_median_absolute_error_hours"),
            "within_1h_rate": m.get("within_1h_rate"),
            "within_2h_rate": m.get("within_2h_rate"),
            "within_4h_rate": m.get("within_4h_rate"),
            "mean_distance_to_interval_hours": m.get("mean_distance_to_observed_interval_hours"),
        })
    base = nested(metadata, "benchmarks", "median_baseline", default={}) or {}
    rows.append({
        **common,
        "comparison_scope": "all_initial_test_completed",
        "model": "median_baseline",
        "prediction_mode": "initial",
        "uses_hydro_eta": False,
        "eta_timing": "none",
        "target": "total_duration_hours",
        "rows": base.get("completed_rows"),
        "mae_hours": base.get("midpoint_mae_hours"),
        "median_absolute_error_hours": base.get("midpoint_median_absolute_error_hours"),
        "within_1h_rate": base.get("within_1h_rate"),
        "within_2h_rate": base.get("within_2h_rate"),
        "within_4h_rate": base.get("within_4h_rate"),
    })

    initial_hq = nested(metadata, "benchmarks", "initial_hydro_first_eta", default={}) or {}
    hydro = initial_hq.get("hydro_quebec") or {}
    rows.append({
        **common,
        "comparison_scope": "initial_hydro_paired",
        "model": "hydro_first_eta",
        "prediction_mode": "initial",
        "uses_hydro_eta": True,
        "eta_timing": "first_snapshot_official",
        "target": "total_duration_hours",
        "rows": hydro.get("rows"),
        "mae_hours": hydro.get("midpoint_mae_hours"),
        "median_absolute_error_hours": hydro.get("midpoint_median_absolute_error_hours"),
        "within_1h_rate": hydro.get("within_1h_rate"),
        "within_2h_rate": hydro.get("within_2h_rate"),
        "within_4h_rate": hydro.get("within_4h_rate"),
        "mean_distance_to_interval_hours": hydro.get("mean_distance_to_observed_interval_hours"),
    })
    for model in ("initial_no_eta", "initial_first_eta"):
        m = nested(initial_hq, "models", model, default={}) or {}
        rows.append({
            **common,
            "comparison_scope": "initial_hydro_paired",
            "model": model,
            "prediction_mode": "initial",
            "uses_hydro_eta": model == "initial_first_eta",
            "eta_timing": "none" if model == "initial_no_eta" else "first_snapshot",
            "target": "total_duration_hours",
            "rows": m.get("rows"),
            "mae_hours": m.get("midpoint_mae_hours"),
            "median_absolute_error_hours": m.get("midpoint_median_absolute_error_hours"),
            "within_1h_rate": m.get("within_1h_rate"),
            "within_2h_rate": m.get("within_2h_rate"),
            "within_4h_rate": m.get("within_4h_rate"),
            "mean_distance_to_interval_hours": m.get("mean_distance_to_observed_interval_hours"),
            "mae_reduction_vs_hydro_percent": m.get("mae_reduction_vs_hydro_percent"),
            "model_closer_rate_vs_hydro": m.get("model_closer_rate"),
        })

    live_m = nested(metadata, "models", "live_latest_eta", "episode_weighted_metrics", default={}) or {}
    live_cmp = nested(metadata, "benchmarks", "live_hydro_latest_eta", "episode_weighted", default={}) or {}
    rows.append({
        **common,
        "comparison_scope": "live_hydro_paired_episode_weighted",
        "model": "live_latest_eta",
        "prediction_mode": "live",
        "uses_hydro_eta": True,
        "eta_timing": "latest_causally_available",
        "target": "remaining_duration_hours",
        "rows": live_m.get("completed_rows"),
        "mae_hours": live_cmp.get("model_mae_hours", live_m.get("midpoint_mae_hours")),
        "median_absolute_error_hours": live_m.get("midpoint_median_absolute_error_hours"),
        "within_1h_rate": live_m.get("within_1h_rate"),
        "within_2h_rate": live_m.get("within_2h_rate"),
        "within_4h_rate": live_m.get("within_4h_rate"),
        "mean_distance_to_interval_hours": live_cmp.get("model_mean_distance_to_interval_hours"),
        "mae_reduction_vs_hydro_percent": live_cmp.get("mae_reduction_percent"),
        "model_closer_rate_vs_hydro": live_cmp.get("model_closer_rate"),
    })
    rows.append({
        **common,
        "comparison_scope": "live_hydro_paired_episode_weighted",
        "model": "hydro_latest_eta",
        "prediction_mode": "live",
        "uses_hydro_eta": True,
        "eta_timing": "latest_causally_available_official",
        "target": "remaining_duration_hours",
        "rows": nested(metadata, "benchmarks", "live_hydro_latest_eta", "paired_completed_snapshot_rows"),
        "mae_hours": live_cmp.get("hydro_mae_hours"),
        "mean_distance_to_interval_hours": live_cmp.get("hydro_mean_distance_to_interval_hours"),
    })
    return rows


def calibration_rows(metadata: dict, release: dict[str, str]) -> list[dict]:
    rows = []
    for model in ("initial_no_eta", "initial_first_eta", "live_latest_eta"):
        probs = nested(metadata, "models", model, "threshold_probabilities", default={}) or {}
        for key, values in probs.items():
            if not isinstance(values, dict):
                continue
            threshold = int(re.search(r"(\d+)", key).group(1))
            rows.append({
                **release,
                "model": model,
                "prediction_mode": "live" if model == "live_latest_eta" else "initial",
                "threshold_hours": threshold,
                "known_rows": values.get("known_rows"),
                "predicted_probability": values.get(
                    "episode_weighted_predicted_probability",
                    values.get("mean_predicted_probability_all_rows"),
                ),
                "observed_rate": values.get(
                    "episode_weighted_observed_rate",
                    values.get("observed_rate_known_rows"),
                ),
                "brier_score": values.get(
                    "episode_weighted_brier",
                    values.get("brier_score_known_labels"),
                ),
            })
    return rows


def update_history(path: Path, current_rows: list[dict]) -> None:
    existing = pd.read_csv(path, low_memory=False) if path.exists() and path.stat().st_size else pd.DataFrame()
    current = pd.DataFrame(current_rows)
    # The previous one-model release runner wrote a different wide schema. Keep
    # it as a legacy backup instead of mixing incompatible rows in Tableau.
    if not existing.empty and not {"comparison_scope", "model"}.issubset(existing.columns):
        legacy = path.with_name("tableau_run_history_legacy.csv")
        if not legacy.exists():
            shutil.copy2(path, legacy)
        existing = pd.DataFrame()
    if not existing.empty and "model_release_id" in existing and "model_release_id" in current:
        existing = existing.loc[~existing["model_release_id"].astype(str).isin(set(current["model_release_id"].astype(str)))]
    combined = pd.concat([existing, current], ignore_index=True, sort=False)
    combined.to_csv(path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--label")
    parser.add_argument("--regime-start")
    parser.add_argument("--max-interval-minutes", type=float)
    parser.add_argument("--skip-train", action="store_true")
    args = parser.parse_args()

    dataset = args.dataset.expanduser().resolve()
    if not dataset.exists():
        raise FileNotFoundError(dataset)
    created_at = datetime.now(timezone.utc).isoformat()
    dataset_hash = sha256_file(dataset)
    profile = dataset_profile(dataset)

    if not args.skip_train:
        if CURRENT_DIR.exists():
            shutil.rmtree(CURRENT_DIR)
        cmd = [sys.executable, "-m", "ml.train_three_survival_models", str(dataset), "--artifact-dir", str(CURRENT_DIR)]
        if args.regime_start:
            cmd += ["--regime-start", args.regime_start]
        if args.max_interval_minutes is not None:
            cmd += ["--max-interval-minutes", str(args.max_interval_minutes)]
        print("\n=== THREE-MODEL TRAINING COMMAND ===")
        print(" ".join(cmd))
        print("====================================\n")
        subprocess.run(cmd, cwd=ROOT, check=True)
    metadata_path = CURRENT_DIR / "three_model_metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing {metadata_path}")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    cutoff = profile.get("max_captured_at")
    cutoff_day = re.sub(r"[^0-9]", "", str(cutoff)[:10]) if cutoff else "unknown"
    release_id = clean_label(args.label or f"aft-3model-{cutoff_day}-{dataset_hash[:8]}")
    release_dir = RELEASES_DIR / release_id
    if release_dir.exists():
        shutil.rmtree(release_dir)
    shutil.copytree(CURRENT_DIR, release_dir)

    release_cols = {
        "model_release_id": release_id,
        "release_created_at_utc": created_at,
        "dataset_sha256": dataset_hash,
        "dataset_cutoff_utc": cutoff or "",
    }
    manifest = {
        **release_cols,
        "dataset_file": dataset.name,
        "dataset_rows": profile["rows"],
        "git_commit": git_commit(),
        "model_version": metadata.get("model_version"),
        "test_preserved": True,
        "candidate_refit_scope": "train_plus_validation_only",
    }
    (release_dir / "model_run_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    TABLEAU_DIR.mkdir(parents=True, exist_ok=True)
    model_rows = comparison_rows(metadata, release_cols)
    write_csv(TABLEAU_DIR / "tableau_model_comparison.csv", model_rows)
    update_history(TABLEAU_DIR / "tableau_run_history.csv", model_rows)
    write_csv(TABLEAU_DIR / "tableau_calibration.csv", calibration_rows(metadata, release_cols))

    prepend_release_columns(
        CURRENT_DIR / "initial_test_predictions.csv",
        TABLEAU_DIR / "tableau_initial_predictions.csv",
        release_cols,
    )
    prepend_release_columns(
        CURRENT_DIR / "live_test_predictions.csv",
        TABLEAU_DIR / "tableau_live_predictions.csv",
        release_cols,
    )
    if (CURRENT_DIR / "live_elapsed_bucket_metrics.csv").exists():
        prepend_release_columns(
            CURRENT_DIR / "live_elapsed_bucket_metrics.csv",
            TABLEAU_DIR / "tableau_live_elapsed_metrics.csv",
            release_cols,
        )
    shutil.copy2(metadata_path, TABLEAU_DIR / "current_three_model_metadata.json")
    shutil.copy2(release_dir / "model_run_manifest.json", TABLEAU_DIR / "current_model_run_manifest.json")

    print("\n=== THREE-MODEL RELEASE COMPLETE ===")
    print(f"Release ID    : {release_id}")
    print(f"Dataset SHA   : {dataset_hash}")
    print(f"Dataset rows  : {profile['rows']}")
    print(f"Dataset cutoff: {cutoff}")
    print(f"Archived      : {release_dir}")
    print(f"Tableau       : {TABLEAU_DIR}")
    print("\nStable Tableau files:")
    for name in (
        "tableau_model_comparison.csv",
        "tableau_initial_predictions.csv",
        "tableau_live_predictions.csv",
        "tableau_live_elapsed_metrics.csv",
        "tableau_calibration.csv",
        "tableau_run_history.csv",
    ):
        if (TABLEAU_DIR / name).exists():
            print(f"  - {TABLEAU_DIR / name}")
    print("\n[OK] Test partitions were not used for tuning or candidate refit.")


if __name__ == "__main__":
    main()
