# ProjetHydro

Pipeline automatisé de collecte, d'historisation et d'analyse des pannes électriques au Québec, accompagné d'un dashboard Streamlit interactif.

Le projet collecte les données publiques d'Hydro-Québec **chaque heure** avec GitHub Actions, conserve l'historique de production dans **Supabase/PostgreSQL**, maintient des tables analytiques optimisées et expose les résultats dans un dashboard permettant d'explorer les pannes actives et historiques.

> L'historique du projet débute le **31 mars 2026**.

## Architecture

```mermaid
flowchart LR
    A[Données Hydro-Québec] --> B[Collecte Python<br/>chaque heure]
    B --> C[Snapshot normalisé + manifeste]
    C --> H[Archive brute GitHub Actions<br/>30 jours]
    C --> D[(Supabase / PostgreSQL)]
    D --> E[Refresh SQL incrémental]
    E --> F[Tables analytiques]
    F --> G[Dashboard Streamlit]
    J[Référentiel géospatial<br/>municipalités / MRC / régions] --> D
    I[Maintenance périodique] --> E
```

### Pipeline de production

1. `scripts/fetch_outages.py` récupère le snapshot courant, normalise les champs, classe les causes et crée un `snapshot_id` unique. Le manifeste `current_snapshot_meta.json` permet de représenter explicitement un snapshot valide contenant **0 panne**.
2. `.github/workflows/hydro.yml` exécute la collecte **toutes les heures**.
3. `scripts/archive_snapshot.py` compresse le snapshot, calcule ses checksums SHA-256 et `.github/workflows/hydro.yml` le conserve comme **artifact GitHub Actions pendant 30 jours avant toute tentative de synchro Supabase**.
4. `scripts/sync_to_supabase.py` synchronise le snapshot vers `raw_outage_snapshots` et journalise son état dans `collection_runs`. La migration des anciennes observations vers des identifiants `legacy:*` est automatique et idempotente.
5. `scripts/refresh_supabase_analytics.py` met à jour de façon incrémentale les tables utilisées par l'application. Les pannes actives correspondent exactement au **dernier snapshot réussi**, sans fenêtre temporelle approximative.
6. `dashboard/streamlit_app.py` orchestre l'interface Streamlit; la configuration, l'accès aux données, les helpers et les composants visuels sont séparés dans `dashboard/config.py`, `dashboard/data_access.py`, `dashboard/view_helpers.py` et `dashboard/components.py`.
7. `.github/workflows/hydro_maintenance.yml` effectue une maintenance hebdomadaire et force la réconciliation des analyses plus coûteuses.
8. `scripts/check_pipeline_health.py` contrôle la fraîcheur du pipeline après les traitements, publie un résumé dans GitHub Actions et fait échouer le workflow uniquement lorsqu'un signal critique est détecté.

## Tables principales

| Table | Rôle |
| --- | --- |
| `collection_runs` | Journal des snapshots : identifiant, heure, version source, statut et nombre de pannes |
| `raw_outage_snapshots` | Historique brut des observations de pannes, reliées à un `snapshot_id` |
| `dim_municipalities` | Référentiel municipal enrichi avec MRC et région |
| `app_latest_outages` | Dernière observation connue de chaque panne |
| `app_active_outages` | Pannes considérées actives au dernier snapshot |
| `app_daily_summary` | Agrégations quotidiennes utilisées pour les tendances |
| `app_data_quality_report` | Contrôles et indicateurs de qualité des données |
| `app_refresh_state` | Horodatages des refresh analytiques incrémentaux et lourds |

Les tables `app_latest_outages` et `app_active_outages` sont maintenues de façon incrémentale afin d'éviter de retraiter l'ensemble de l'historique à chaque collecte. Les analyses plus lourdes, notamment les agrégations quotidiennes et le rapport de qualité, sont rafraîchies périodiquement et peuvent être reconstruites lors de la maintenance. Les requêtes PostgreSQL du refresh sont versionnées séparément dans `sql/postgres/`; `scripts/refresh_supabase_analytics.py` reste un orchestrateur Python léger.

## Convention des fuseaux horaires

Le pipeline utilise maintenant des timestamps PostgreSQL `TIMESTAMPTZ` et applique explicitement les conventions suivantes :

- `captured_at` est généré par ProjetHydro en **UTC** ;
- `start_time` et `estimated_restore` fournis sans offset par Hydro-Québec sont interprétés comme des heures locales `America/Toronto`, puis convertis en UTC pour le stockage ;
- le dashboard reconvertit les horodatages dans `America/Toronto` pour l'affichage ;
- `app_daily_summary` utilise la **date civile du Québec**, et non la date UTC, pour classer une capture dans une journée.

La migration des anciennes colonnes `TIMESTAMP` vers `TIMESTAMPTZ` est automatique et idempotente. Lors du premier refresh après migration, `app_latest_outages` est reconstruit afin de recalculer les durées avec les timestamps corrigés.

## Sauvegarde et reprise d'un snapshot

Le workflow horaire crée une archive `csv.gz` avant la synchro Supabase puis l'envoie dans les artifacts du run GitHub Actions. Chaque manifeste contient le `snapshot_id`, le nombre de pannes et des checksums SHA-256. La rétention est configurée à **30 jours**.

Si une synchro Supabase échoue après une collecte réussie :

1. télécharger l'artifact `hydro-raw-<run_id>-<attempt>` du run concerné ;
2. extraire l'artifact ;
3. restaurer le snapshot à partir de son manifeste JSON :

```bash
python scripts/restore_snapshot_archive.py chemin/vers/le_manifeste.json
```

4. relancer la synchro normale :

```bash
python scripts/sync_to_supabase.py
python scripts/refresh_supabase_analytics.py
```

Les contraintes d'unicité et `snapshot_id` rendent ce rejeu idempotent.

## Enrichissement géospatial

Les observations Hydro-Québec fournissent un identifiant municipal et des coordonnées. Le script `scripts/build_municipality_reference_geo.py` permet de construire un référentiel en associant ces points à des polygones municipaux, puis d'ajouter notamment :

- le nom de la municipalité ;
- la MRC ;
- la région administrative ;
- des indicateurs de couverture et de qualité du géocodage.

Ce référentiel est ensuite synchronisé vers `dim_municipalities` et utilisé par les analyses et les cartes du dashboard.

## Dashboard

Le dashboard Streamlit permet notamment de consulter :

- les pannes actives et le nombre de clients touchés ;
- les pannes récentes et l'historique disponible ;
- les cartes par municipalité, MRC et région ;
- l'évolution temporelle des pannes et des clients affectés ;
- la distribution des causes ;
- les indicateurs de qualité des données ;
- une page **Santé du pipeline** avec la fraîcheur des collectes, les refresh analytiques, les erreurs sur 24 h et les anomalies de volume.

En production, le dashboard lit les données dans Supabase/PostgreSQL. Pour le développement local, il peut également utiliser les exports CSV générés à partir du warehouse DuckDB.


## Monitoring opérationnel

La page **Santé du pipeline** s'appuie sur `collection_runs`, `app_refresh_state`, `app_active_outages` et `app_latest_outages`. Elle affiche notamment :

- l'âge de la dernière collecte réussie ;
- l'âge du dernier refresh incrémental ;
- l'âge du dernier refresh analytique lourd ;
- les runs réussis et en erreur sur les dernières 24 heures ;
- le nombre de pannes et de clients affectés dans le snapshot actif ;
- les variations anormales entre les deux derniers snapshots réussis.

Les seuils par défaut sont volontairement simples : collecte et refresh incrémental en avertissement après **75 minutes** et critiques après **120 minutes** ; refresh lourd en avertissement après **30 heures** et critique après **48 heures**. Une chute brutale du volume de pannes génère un avertissement mais ne transforme jamais un snapshot valide à 0 panne en erreur de collecte.

Les workflows de production utilisent le même groupe `concurrency` afin d'éviter que la maintenance et la collecte horaire modifient simultanément les tables analytiques. Ils définissent également des permissions minimales, un timeout et un contrôle de santé final.

## Workflow local avec DuckDB

DuckDB reste disponible comme environnement analytique local et reproductible. Les calculs de durée, les dates civiles du Québec et le regroupement des snapshots utilisent les mêmes conventions analytiques que PostgreSQL.

```bash
# Installer les dépendances runtime/pipeline verrouillées
python -m pip install -r requirements.txt

# Pour exécuter les tests
python -m pip install -r requirements-dev.txt

# Pour reconstruire le référentiel géospatial
python -m pip install -r requirements-geo.txt

# Collecter un snapshot et l'ajouter à l'historique CSV local
HYDRO_WRITE_LOCAL_HISTORY=1 python scripts/fetch_outages.py

# Construire le warehouse local
python scripts/build_warehouse.py

# Exporter les tables analytiques en CSV
python scripts/export_tables.py

# Lancer le dashboard
streamlit run dashboard/streamlit_app.py
```

Sous Windows PowerShell, la variable d'environnement peut être définie avant la collecte avec :

```powershell
$env:HYDRO_WRITE_LOCAL_HISTORY="1"
python scripts/fetch_outages.py
```

## Configuration Supabase

La connexion de production utilise principalement les variables suivantes :

```text
SUPABASE_DB_URL
SUPABASE_DB_HOSTADDR   # optionnelle selon l'environnement réseau
```

Le dashboard peut aussi recevoir ces valeurs via les secrets Streamlit. Les identifiants de connexion ne doivent jamais être ajoutés au dépôt Git.

## Structure du projet

```text
ProjetHydro/
├── .github/workflows/              # CI, automatisation horaire et maintenance
├── dashboard/
│   ├── streamlit_app.py            # Orchestration des pages Streamlit
│   ├── config.py                   # Constantes et configuration UI
│   ├── data_access.py              # CSV + Supabase/PostgreSQL
│   ├── view_helpers.py             # Normalisation et formatage
│   └── components.py               # Graphiques, cartes et composants UI
├── scripts/
│   ├── fetch_outages.py            # Collecte, retries HTTP et normalisation
│   ├── archive_snapshot.py         # Archive brute + checksums
│   ├── restore_snapshot_archive.py # Restauration/rejeu d'un artifact
│   ├── time_utils.py               # Conventions UTC / America/Toronto
│   ├── sync_to_supabase.py         # Synchronisation PostgreSQL
│   ├── refresh_supabase_analytics.py
│   ├── check_pipeline_health.py    # Contrôle opérationnel après chaque run
│   ├── build_warehouse.py          # Workflow DuckDB local
│   ├── export_tables.py
│   └── build_municipality_reference_geo.py
├── sql/
│   ├── *.sql                       # Transformations du warehouse DuckDB
│   └── postgres/                   # Requêtes du refresh Supabase/PostgreSQL
├── supabase/                       # Schéma et optimisation PostgreSQL
├── tests/                          # Tests automatisés et parité analytique
├── requirements.txt                # Dépendances runtime/pipeline verrouillées
├── requirements-dev.txt            # Dépendances de test verrouillées
└── requirements-geo.txt            # Dépendances géospatiales verrouillées
```

## Technologies

**Python · pandas · PostgreSQL · Supabase · SQL · Streamlit · Plotly · GitHub Actions · DuckDB · données géospatiales · ETL · Data Quality**

## Qualité et exploitation

Le pipeline comprend plusieurs mécanismes destinés à rendre les traitements plus robustes :

- snapshots atomiques identifiés par `snapshot_id`, y compris lorsqu'aucune panne n'est active ;
- journal `collection_runs` avec statuts `pending`, `success` et `error` ;
- déduplication des observations par panne et timestamp de capture ;
- archive brute de chaque collecte avant la synchro Supabase, conservée 30 jours dans GitHub Actions ;
- restauration contrôlée d'un snapshot archivé avec vérification SHA-256 ;
- synchronisation avec une petite fenêtre de reprise pour récupérer d'éventuelles arrivées tardives ;
- sessions HTTP avec retries, backoff exponentiel et gestion des statuts `429/500/502/503/504` ;
- stockage `TIMESTAMPTZ` avec séparation explicite UTC / heure locale du Québec ;
- rafraîchissement incrémental des tables les plus consultées ;
- index PostgreSQL pour les accès fréquents ;
- rapport de qualité des données ;
- maintenance périodique pour réconcilier les tables analytiques ;
- monitoring opérationnel avec fraîcheur des collectes/refresh, détection de variations anormales et résumé GitHub Actions ;
- workflows sérialisés par `concurrency`, permissions minimales et timeouts explicites ;
- CI GitHub Actions exécutant la compilation Python et `pytest` sur les pushes et pull requests ;
- dépendances Python verrouillées par usage (`runtime`, `dev`, `geo`) pour rendre les builds reproductibles ;
- parité des principales conventions analytiques DuckDB/PostgreSQL (durées décimales, date Québec, regroupement des captures).

## Licence et source des données

Les données utilisées dans ce projet proviennent des données publiques d'Hydro-Québec.

Elles sont distribuées sous licence **Creative Commons Attribution – NonCommercial 4.0 International (CC BY-NC 4.0)**. Elles doivent notamment être attribuées à leur source et ne doivent pas être utilisées à des fins commerciales.

- Source : Hydro-Québec – Données ouvertes : https://donnees.hydroquebec.com/explore/dataset/pannes-interruptions/information/?flg=fr-fr
- Licence : CC BY-NC 4.0
- Texte de la licence : https://creativecommons.org/licenses/by-nc/4.0/

Les traitements réalisés dans ce dépôt peuvent inclure la collecte, le nettoyage, la transformation, l'agrégation et l'analyse des données. Toute erreur d'interprétation ou d'analyse relève de l'auteur de ce projet et non d'Hydro-Québec.
