"""Causal snapshot history shared by dynamic training and inference."""
from __future__ import annotations
import numpy as np
import pandas as pd
from ml.dataset import _parse_datetime_columns


def annotate_observed_history(history: pd.DataFrame) -> pd.DataFrame:
    """Input must contain one or more continuous episodes, with episode_id.

    Callers must supply observed snapshots only, in particular no future rows.
    Missing initial values remain missing. Targets/end-of-episode metadata are
    neither required nor read here.
    """
    required = {'episode_id', 'captured_at', 'start_time', 'customers_affected', 'status'}
    missing = required.difference(history.columns)
    if missing:
        raise ValueError(f'Missing history columns: {sorted(missing)}')
    df = _parse_datetime_columns(history)
    if df[['episode_id', 'captured_at', 'start_time']].isna().any().any():
        raise ValueError('History needs valid episode ids and timestamps.')
    if df.duplicated(['episode_id', 'captured_at']).any():
        raise ValueError('Duplicate episode/capture rows in history.')
    df = df.sort_values(['episode_id', 'captured_at'], kind='stable').reset_index(drop=True)
    df['first_capture'] = df.groupby('episode_id')['captured_at'].transform('min')
    if 'estimated_restore' not in df:
        df['estimated_restore'] = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns, UTC]')
    eta = df['estimated_restore']
    df['has_current_hydro_eta'] = eta.notna()
    df['hydro_remaining_hours'] = (eta - df['captured_at']).dt.total_seconds() / 3600
    df['hydro_eta_overdue'] = eta.notna() & (eta < df['captured_at'])
    df['operational_phase'] = np.where(eta.notna(), 'with_hydro_eta', 'without_current_hydro_eta')
    group = df.groupby('episode_id', sort=False)
    df['observation_number'] = group.cumcount() + 1
    df['is_first_observation'] = df['observation_number'].eq(1)
    df['elapsed_since_first_capture_hours'] = (df['captured_at'] - df['first_capture']).dt.total_seconds() / 3600
    df['elapsed_since_reported_start_hours'] = (df['captured_at'] - df['start_time']).dt.total_seconds() / 3600
    # Compare only to earlier nonmissing ETAs; do not backfill from the future.
    last_eta = group['estimated_restore'].ffill()
    previous_eta = last_eta.groupby(df['episode_id']).shift()
    revision = eta.notna() & previous_eta.notna() & eta.ne(previous_eta)
    df['hydro_eta_revision_count'] = revision.groupby(df['episode_id']).cumsum()
    df['hydro_latest_revision_hours'] = np.where(
        revision, (eta - previous_eta).dt.total_seconds() / 3600, 0.0)
    df['hydro_eta_ever_seen'] = df['has_current_hydro_eta'].groupby(df['episode_id']).cummax()
    df['customers_change_from_previous'] = pd.to_numeric(df['customers_affected'], errors='coerce').groupby(df['episode_id']).diff()
    previous_status = group['status'].shift()
    status_change = df['observation_number'].gt(1) & df['status'].fillna('__missing__').ne(previous_status.fillna('__missing__'))
    df['status_change_count'] = status_change.groupby(df['episode_id']).cumsum()
    # Current status spell and first observed work status: past/current only.
    normalized_status = df['status'].fillna('__missing__').astype(str).str.strip().str.lower()
    previous_normalized = normalized_status.groupby(df['episode_id']).shift()
    new_spell = df['observation_number'].eq(1) | normalized_status.ne(previous_normalized)
    spell_start = df['captured_at'].where(new_spell).groupby(df['episode_id']).ffill()
    df['hours_in_current_status'] = (df['captured_at'] - spell_start).dt.total_seconds() / 3600
    working = normalized_status.eq('working')
    working_seen_count = working.groupby(df['episode_id']).cumsum()
    first_work_time = df['captured_at'].where(working & working_seen_count.eq(1)).groupby(df['episode_id']).ffill()
    df['has_working_status_seen'] = first_work_time.notna()
    df['hours_since_first_working_status'] = (df['captured_at'] - first_work_time).dt.total_seconds() / 3600
    df['has_known_current_status'] = normalized_status.ne('__missing__') & normalized_status.ne('') & normalized_status.ne('unknown')
    customers = pd.to_numeric(df['customers_affected'], errors='coerce').clip(lower=0)
    # Preserve missing first-observation values; never backfill an initial count.
    initial = customers.groupby(df['episode_id']).transform(lambda values: values.iloc[0])
    peak = customers.groupby(df['episode_id']).cummax().groupby(df['episode_id']).ffill()
    df['customers_initial_observed'] = initial
    df['customers_peak_observed_so_far'] = peak
    df['customers_change_from_initial'] = customers - initial
    df['customers_fraction_of_initial'] = customers / initial.where(initial.gt(0))
    df['customers_fraction_of_peak_so_far'] = customers / peak.where(peak.gt(0))
    df['customers_decline_fraction_from_peak'] = (peak-customers) / peak.where(peak.gt(0))
    return df
