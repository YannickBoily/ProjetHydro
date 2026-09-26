"""Configuration partagée du dashboard Streamlit."""

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]

RAW_FILE = ROOT_DIR / "data" / "raw" / "hydroquebec_history.csv"

ACTIVE_FILE = ROOT_DIR / "data" / "processed" / "active_outages.csv"

LATEST_FILE = ROOT_DIR / "data" / "processed" / "latest_outages.csv"

DAILY_FILE = ROOT_DIR / "data" / "processed" / "daily_summary.csv"

QUALITY_FILE = ROOT_DIR / "data" / "processed" / "data_quality_report.csv"

CAUSE_TRANSLATIONS = {
    "unknown": "Inconnue",
    "other": "Autre",
    "equipment": "Bris d’équipement",
    "vegetation": "Végétation",
    "accident": "Accident",
    "weather": "Conditions météorologiques",
    "animal": "Animal",
}

STATUS_TRANSLATIONS = {
    "new": "Nouvelle panne",
    "assigned": "Travaux assignés",
    "en_route": "Équipe en route",
    "working": "Équipe au travail",
}

QUALITY_TRANSLATIONS = {
    "pass": "Réussi",
    "fail": "Échec",
    "info": "Information",
    "critical": "Critique",
    "warning": "Avertissement",
}

CHECK_TRANSLATIONS = {
    "missing_outage_id": "ID de panne manquant",
    "missing_captured_at": "Moment de capture manquant",
    "negative_customers_affected": "Clients affectés négatifs",
    "invalid_coordinates": "Coordonnées invalides",
    "estimated_restore_before_start_time": "Rétablissement estimé avant le début",
    "captured_at_before_start_time": "Capture avant le début de la panne",
    "duplicate_outage_id_captured_at": "Doublon panne + capture",
    "unknown_cause_rows": "Cause inconnue",
}

COLUMN_LABELS = {
    "outage_id": "ID de panne",
    "short_outage_id": "ID court",
    "customers_affected": "Clients affectés",
    "start_time": "Début",
    "estimated_restore": "Rétablissement estimé",
    "status_fr": "Statut",
    "analysis_cause_label_fr": "Cause",
    "latest_raw_cause_label_fr": "Cause brute",
    "history_cause_label_fr": "Cause",
    "municipality_label": "Municipalité",
    "mrc_name": "MRC",
    "region_name": "Région",
    "active_capture_at": "Capture active",
    "latest_row_captured_at": "Dernière capture",
    "captured_at": "Capture",
    "first_capture_at": "Première capture",
    "last_capture_at": "Dernière capture",
    "capture_count": "Captures",
    "observed_duration_hours": "Durée observée, h",
    "outage_age_hours_at_capture": "Âge, h",
    "restore_eta_hours_at_capture": "ETA rétablissement, h",
    "lon": "Longitude",
    "lat": "Latitude",
    "is_major_outage_fr": "Panne majeure",
    "has_known_cause_fr": "Cause connue",
    "is_geocoded_fr": "Géocodée",
    "date": "Date",
    "max_active_outages_estimate": "Pannes actives max",
    "max_customers_affected": "Clients affectés max",
    "new_outages_detected": "Nouvelles pannes",
    "max_municipalities_affected": "Municipalités touchées max",
    "max_major_outages": "Pannes majeures max",
    "snapshots_count": "Captures",
    "avg_active_outages_estimate": "Pannes actives moy.",
    "avg_customers_affected": "Clients affectés moy.",
    "check_name_fr": "Contrôle",
    "severity_fr": "Sévérité",
    "status_quality_fr": "Statut",
    "rows_affected": "Lignes affectées",
    "failed_rate_pct": "Taux affecté, %",
    "description": "Description",
}

PLOT_TEMPLATE = "plotly_dark"

SOURCE_LIMIT_CHECKS = {"unknown_cause_rows"}

QUEBEC_TIMEZONE = "America/Toronto"

CACHE_TTL_SECONDS = 900

ACTIVE_CACHE_TTL_SECONDS = 900

RECENT_CACHE_TTL_SECONDS = 3600

DAILY_CACHE_TTL_SECONDS = 21600

QUALITY_CACHE_TTL_SECONDS = 21600

DEFAULT_HISTORY_DAYS = 90

DEFAULT_HISTORY_ROWS_LIMIT = 10_000

TIMESTAMP_COLUMNS = (
    "start_time",
    "estimated_restore",
    "captured_at",
    "active_capture_at",
    "latest_row_captured_at",
    "first_capture_at",
    "last_capture_at",
    "known_cause_last_seen_at",
    "created_at",
)

NUMERIC_COLUMNS = (
    "customers_affected",
    "municipality_id",
    "capture_count",
    "observed_duration_hours",
    "outage_age_hours_at_capture",
    "outage_age_hours_at_latest_capture",
    "restore_eta_hours_at_capture",
    "restore_eta_hours_at_latest_capture",
    "lon",
    "lat",
    "rows_affected",
    "total_rows",
    "failed_rate_pct",
    "snapshots_count",
    "max_active_outages_estimate",
    "avg_active_outages_estimate",
    "max_customers_affected",
    "avg_customers_affected",
    "max_municipalities_affected",
    "max_major_outages",
    "new_outages_detected",
    "raw_rows_count",
    "unique_outages_observed",
    "unknown_cause_rows",
    "municipalities_observed",
)

DATA_REQUEST_URL = "https://docs.google.com/forms/d/e/1FAIpQLSdSkwsCNaj0u_gIbzXQXTjGklIA4b40KodbTQ2n-H4oiJHwDw/viewform?usp=publish-editor"

ACCENT_COLOR = "#38bdf8"

MAP_STYLE = "carto-darkmatter"

MAP_MARKER_MIN_SIZE = 4

MAP_MARKER_MAX_SIZE = 18

CAUSE_COLORS = {
    "Inconnue": "#64748b",
    "Autre": "#a78bfa",
    "Bris d’équipement": "#f59e0b",
    "Végétation": "#22c55e",
    "Accident": "#ef4444",
    "Conditions météorologiques": "#38bdf8",
    "Animal": "#f472b6",
}

QUALITY_DESCRIPTION_FR = {
    "missing_outage_id": "Chaque observation doit posséder un identifiant de panne.",
    "missing_captured_at": "Chaque observation doit contenir un moment de capture.",
    "negative_customers_affected": "Le nombre de clients affectés ne peut pas être négatif.",
    "invalid_coordinates": (
        "Les coordonnées doivent se trouver dans une plage géographique valide."
    ),
    "estimated_restore_before_start_time": (
        "Le rétablissement estimé ne doit pas précéder le début de la panne."
    ),
    "captured_at_before_start_time": (
        "La capture ne doit pas précéder le début déclaré de la panne."
    ),
    "duplicate_outage_id_captured_at": (
        "Une panne ne doit apparaître qu’une fois par moment de capture."
    ),
    "unknown_cause_rows": "La source ne fournit pas toujours la cause au moment de la capture.",
}
