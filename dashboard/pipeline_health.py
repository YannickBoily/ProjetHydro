"""Pure helpers used by the dashboard and operational health checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd


COLLECTION_WARNING_MINUTES = 75
COLLECTION_CRITICAL_MINUTES = 120
ANALYTICS_WARNING_MINUTES = 75
ANALYTICS_CRITICAL_MINUTES = 120
HEAVY_WARNING_HOURS = 30
HEAVY_CRITICAL_HOURS = 48
DROP_WARNING_PERCENT = 80.0
DROP_MIN_PREVIOUS_OUTAGES = 20
SPIKE_WARNING_PERCENT = 300.0
SPIKE_MIN_ABSOLUTE_INCREASE = 50


@dataclass(frozen=True)
class HealthAlert:
    level: str
    code: str
    message: str


def _utc_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp


def age_minutes(value: Any, now: Any = None) -> float | None:
    timestamp = _utc_timestamp(value)
    if timestamp is None:
        return None

    if now is None:
        now_ts = pd.Timestamp(datetime.now(timezone.utc))
    else:
        now_ts = _utc_timestamp(now)
        if now_ts is None:
            return None

    return max(0.0, float((now_ts - timestamp).total_seconds() / 60.0))


def classify_age(
    minutes: float | None,
    warning_minutes: float,
    critical_minutes: float,
) -> str:
    if minutes is None:
        return "critical"
    if minutes >= critical_minutes:
        return "critical"
    if minutes >= warning_minutes:
        return "warning"
    return "good"


def outage_change_pct(current: Any, previous: Any) -> float | None:
    try:
        current_value = float(current)
        previous_value = float(previous)
    except (TypeError, ValueError):
        return None

    if pd.isna(current_value) or pd.isna(previous_value) or previous_value <= 0:
        return None

    return 100.0 * (current_value - previous_value) / previous_value


def assess_pipeline_health(row: dict[str, Any] | pd.Series, now: Any = None) -> dict[str, Any]:
    """Classify freshness and anomalies for one pipeline-health snapshot."""
    values = dict(row)

    collection_age = age_minutes(values.get("latest_success_captured_at"), now=now)
    analytics_age = age_minutes(values.get("incremental_refreshed_at"), now=now)
    heavy_age = age_minutes(values.get("heavy_refreshed_at"), now=now)

    collection_status = classify_age(
        collection_age,
        COLLECTION_WARNING_MINUTES,
        COLLECTION_CRITICAL_MINUTES,
    )
    analytics_status = classify_age(
        analytics_age,
        ANALYTICS_WARNING_MINUTES,
        ANALYTICS_CRITICAL_MINUTES,
    )
    heavy_status = classify_age(
        heavy_age,
        HEAVY_WARNING_HOURS * 60,
        HEAVY_CRITICAL_HOURS * 60,
    )

    alerts: list[HealthAlert] = []

    if collection_status != "good":
        alerts.append(
            HealthAlert(
                collection_status,
                "collection_freshness",
                "La dernière collecte réussie est trop ancienne."
                if collection_age is not None
                else "Aucune collecte réussie n'est disponible.",
            )
        )

    if analytics_status != "good":
        alerts.append(
            HealthAlert(
                analytics_status,
                "analytics_freshness",
                "Le dernier refresh analytique incrémental est trop ancien."
                if analytics_age is not None
                else "Aucun refresh analytique incrémental n'est enregistré.",
            )
        )

    if heavy_status != "good":
        alerts.append(
            HealthAlert(
                heavy_status,
                "heavy_refresh_freshness",
                "Le refresh analytique lourd est en retard."
                if heavy_age is not None
                else "Aucun refresh analytique lourd n'est enregistré.",
            )
        )

    last_status = str(values.get("last_run_status") or "").strip().lower()
    if last_status and last_status != "success":
        alerts.append(
            HealthAlert(
                "critical" if last_status == "error" else "warning",
                "last_run_status",
                f"Le dernier run de collecte est en statut « {last_status} ».",
            )
        )

    consecutive_errors = int(values.get("consecutive_errors") or 0)
    if consecutive_errors >= 3:
        alerts.append(
            HealthAlert(
                "critical",
                "consecutive_errors",
                f"{consecutive_errors} collectes en erreur se sont succédé depuis le dernier succès.",
            )
        )
    elif consecutive_errors > 0:
        alerts.append(
            HealthAlert(
                "warning",
                "consecutive_errors",
                f"{consecutive_errors} collecte(s) en erreur depuis le dernier succès.",
            )
        )

    current_count = values.get("latest_success_outage_count")
    previous_count = values.get("previous_success_outage_count")
    change_pct = outage_change_pct(current_count, previous_count)

    try:
        current_count_num = int(current_count)
        previous_count_num = int(previous_count)
    except (TypeError, ValueError):
        current_count_num = 0
        previous_count_num = 0

    if previous_count_num >= DROP_MIN_PREVIOUS_OUTAGES:
        if current_count_num == 0:
            alerts.append(
                HealthAlert(
                    "warning",
                    "outage_count_drop",
                    "Le dernier snapshot est passé à 0 panne après un snapshot non vide; vérifier si cette chute est attendue.",
                )
            )
        elif change_pct is not None and change_pct <= -DROP_WARNING_PERCENT:
            alerts.append(
                HealthAlert(
                    "warning",
                    "outage_count_drop",
                    f"Le nombre de pannes a chuté de {abs(change_pct):.1f} % entre les deux derniers snapshots réussis.",
                )
            )

    if (
        change_pct is not None
        and change_pct >= SPIKE_WARNING_PERCENT
        and current_count_num - previous_count_num >= SPIKE_MIN_ABSOLUTE_INCREASE
    ):
        alerts.append(
            HealthAlert(
                "warning",
                "outage_count_spike",
                f"Le nombre de pannes a augmenté de {change_pct:.1f} % entre les deux derniers snapshots réussis.",
            )
        )

    severity_order = {"good": 0, "warning": 1, "critical": 2}
    overall_status = "good"
    for alert in alerts:
        if severity_order.get(alert.level, 0) > severity_order[overall_status]:
            overall_status = alert.level

    return {
        "overall_status": overall_status,
        "collection_status": collection_status,
        "analytics_status": analytics_status,
        "heavy_status": heavy_status,
        "collection_age_minutes": collection_age,
        "analytics_age_minutes": analytics_age,
        "heavy_age_minutes": heavy_age,
        "outage_change_pct": change_pct,
        "alerts": alerts,
    }
