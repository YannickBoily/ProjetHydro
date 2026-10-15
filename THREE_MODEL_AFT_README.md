# ProjetHydro — workflow AFT à trois modèles

Le nouveau workflow entraîne trois modèles sur **les mêmes partitions temporelles** :

| Modèle | Moment | ETA Hydro | Cible |
|---|---|---|---|
| `initial_no_eta` | première observation | aucune | durée totale |
| `initial_first_eta` | première observation | ETA visible au premier snapshot seulement | durée totale |
| `live_latest_eta` | chaque snapshot admissible | dernière ETA déjà observée à cet instant | temps restant |

## Run propre en une commande

À la racine du projet :

```powershell
python scripts/run_three_model_release.py raw_outage_snapshots_rows.csv `
  --label "aft-3model-2026-10-16"
```

Pour les prochaines données, remplacer `raw_outage_snapshots_rows.csv` par le nouvel export complet Supabase puis relancer simplement :

```powershell
python scripts/run_three_model_release.py raw_outage_snapshots_rows.csv
```

Sans `--label`, la release reçoit automatiquement un nom basé sur le cutoff et le SHA-256 du dataset.

## Protection contre le leakage

- Split temporel purgé train / validation / test.
- Un même `outage_id` ne peut pas apparaître dans plusieurs partitions.
- Le test n'est utilisé ni pour choisir les hyperparamètres ni pour refitter les modèles candidats.
- `initial_first_eta` utilise uniquement l'ETA du **premier snapshot réel**; aucune valeur future n'est récupérée pour combler un `NaN` initial.
- `live_latest_eta` fait uniquement un **forward-fill dans le temps à l'intérieur du même épisode**. Une ETA révisée à 10:40 ne peut pas être utilisée à 10:20.
- Le modèle live donne à chaque épisode un poids total de 1 afin qu'une panne longue avec beaucoup de snapshots ne domine pas l'entraînement.

## Artifacts

La release est archivée sous :

```text
ml/releases/<release_id>/
├── initial_no_eta/
│   ├── evaluation_model.json
│   ├── candidate_model.json
│   └── validation_search.csv
├── initial_first_eta/
│   ├── evaluation_model.json
│   ├── candidate_model.json
│   └── validation_search.csv
├── live_latest_eta/
│   ├── evaluation_model.json
│   ├── candidate_model.json
│   └── validation_search.csv
├── initial_test_predictions.csv
├── live_test_predictions.csv
├── live_elapsed_bucket_metrics.csv
├── three_model_metadata.json
└── model_run_manifest.json
```

`evaluation_model.json` est entraîné sur train seulement et produit les métriques du test. `candidate_model.json` est refitté sur train + validation, mais **jamais sur le test**.

## Tableau

Tableau doit pointer vers les noms stables sous `tableau_exports/` :

```text
tableau_model_comparison.csv
tableau_initial_predictions.csv
tableau_live_predictions.csv
tableau_live_elapsed_metrics.csv
tableau_calibration.csv
tableau_run_history.csv
```

`tableau_model_comparison.csv` contient les scopes comparables : baseline et modèles initiaux sur le test complet, comparaison appariée avec l'ETA initiale Hydro, puis modèle live vs dernière ETA Hydro sur les mêmes snapshots.

## Interprétation recommandée

`initial_no_eta` mesure la valeur prédictive de tes données sans l'ETA officielle. `initial_first_eta` mesure la capacité du ML à recalibrer l'ETA initiale. `live_latest_eta` mesure la capacité du ML à recalibrer l'information officielle la plus récente tout au long de la panne.
