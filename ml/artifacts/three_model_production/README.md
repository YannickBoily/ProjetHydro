# Production model artifacts

This directory is intentionally kept in Git, but the binary model files are not bundled in this source merge.
Promote a reviewed local release with:

```powershell
python scripts/promote_three_model_release.py ml/releases/<release-id>
```

Then commit the generated `production_manifest.json`, `three_model_metadata.json`, and the three `candidate_model.json` files so Streamlit Cloud can load them.
