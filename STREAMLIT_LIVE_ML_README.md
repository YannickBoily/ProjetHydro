# Streamlit — inférence live avec les 3 modèles AFT

Cette intégration sépare **entraînement** et **inférence**.

- Les modèles sont entraînés hors ligne avec `scripts/run_three_model_release.py`.
- Une release validée est explicitement promue vers `ml/artifacts/three_model_production/`.
- Streamlit charge cette release et calcule les estimations directement à partir des snapshots courants de Supabase.
- Ouvrir la page Streamlit **ne réentraîne jamais** XGBoost.

## 1. Promouvoir la release actuelle

Pour la release validée pendant cette session :

```powershell
python scripts/promote_three_model_release.py `
  ml/releases/aft-3model-20261015-c783b769
```

Le dossier de production créé est volontairement minimal :

```text
ml/artifacts/three_model_production/
├── production_manifest.json
├── three_model_metadata.json
├── initial_no_eta/candidate_model.json
├── initial_first_eta/candidate_model.json
└── live_latest_eta/candidate_model.json
```

Pour Streamlit Community Cloud, ces fichiers doivent être commit/push dans le dépôt avec le code.

## 2. Ce que fait la page « Prévisions ML »

À l'ouverture de la page, le dashboard récupère dans Supabase l'historique du **présent épisode continu** de chaque panne active. Les limites d'épisode sont reconstruites à partir de tous les `collection_runs` réussis, y compris les snapshots contenant zéro panne.

Les trois estimations sont ensuite calculées :

1. `initial_no_eta` — durée totale prédite à partir du premier snapshot, sans ETA Hydro.
2. `initial_first_eta` — durée totale prédite à partir du premier snapshot et de l'ETA visible à ce moment.
3. `live_latest_eta` — temps restant prédit au dernier snapshot, avec la dernière ETA Hydro déjà observée à cet instant.

L'ETA live est propagée uniquement du passé vers le futur à l'intérieur du même épisode. Une révision future ne peut donc jamais contaminer une ancienne prédiction.

## 3. Mise à jour des données

Aucun réentraînement n'est requis lorsqu'un nouveau snapshot arrive. Supabase reçoit les nouvelles données comme avant; Streamlit utilisera les nouvelles valeurs au prochain rafraîchissement/cache expiry.

## 4. Nouvelle release plus tard

Quand davantage de données sont disponibles :

```powershell
python scripts/run_three_model_release.py raw_outage_snapshots_rows.csv
```

Évaluer la nouvelle release. Si elle doit remplacer la production :

```powershell
python scripts/promote_three_model_release.py ml/releases/<nouvelle-release>
```

Puis commit/push `ml/artifacts/three_model_production/`.

## 5. Ancien job `app_ml_predictions`

`scripts/predict_outage_duration.py` reste présent pour compatibilité, mais si les anciens artifacts v2 sont absents il quitte proprement sans erreur. La page ML actuelle n'utilise plus les anciennes prédictions matérialisées dans `app_ml_predictions`.

## 6. Vérification locale

```powershell
python -m pytest -q
streamlit run dashboard/streamlit_app.py
```

La page `Prévisions ML` doit afficher l'identifiant de la release promue et les estimations initiales/live des pannes actives.
