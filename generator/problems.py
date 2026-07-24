"""Inject deterministic offline and streaming data-quality problems."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def inject_duplicates(
    df: pd.DataFrame,
    rate: float,
    key_cols: list[str],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Duplicate `rate` fraction of rows (same key columns, identical values)."""
    if rate <= 0 or df.empty:
        return df
    if not 0 <= rate < 1:
        raise ValueError("Duplicate rate must be in the range [0, 1).")
    missing_keys = set(key_cols) - set(df.columns)
    if missing_keys:
        raise ValueError(f"Duplicate keys are absent from the DataFrame: {sorted(missing_keys)}")
    n_dup = max(1, int(len(df) * rate))
    dup_idx = rng.choice(len(df), size=n_dup, replace=False)
    dup_rows = df.iloc[dup_idx].copy()
    return pd.concat([df, dup_rows], ignore_index=True)


def inject_stream_duplicates(
    events: list[dict],
    rate: float,
    rng: np.random.Generator,
) -> list[dict]:
    """Re-emit a fraction of events with the same ID and a later arrival time."""
    if rate <= 0 or not events:
        return events
    if not 0 <= rate < 1:
        raise ValueError("Duplicate rate must be in the range [0, 1).")
    n_dup = max(1, int(len(events) * rate))
    dup_indices = rng.choice(len(events), size=n_dup, replace=False)
    extras = []
    for index in dup_indices:
        duplicate = events[index].copy()
        created_ts = duplicate.get("created_ts")
        if created_ts:
            parsed = datetime.fromisoformat(created_ts)
            duplicate["created_ts"] = (
                parsed + timedelta(minutes=int(rng.integers(1, 4)))
            ).isoformat()
        duplicate["_is_replay"] = True
        extras.append(duplicate)
    return sorted(events + extras, key=lambda event: event.get("created_ts", ""))


def apply_volume_skew(df: pd.DataFrame, tickers_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Skew volume: VN30 gets vn30_volume_share, banking+real_estate dominate value."""
    df = df.copy()
    vn30_tickers = set(tickers_df[tickers_df["vn30"]]["ticker_id"])
    vn30_share = cfg["vn30_volume_share"]

    total_vol = df["volume"].sum()
    if total_vol == 0:
        return df

    # Redistribute: VN30 gets 80% of total volume
    vn30_mask = df["ticker_id"].isin(vn30_tickers)
    non_vn30_mask = ~vn30_mask

    vn30_total = total_vol * vn30_share
    non_vn30_total = total_vol * (1 - vn30_share)

    vn30_cur = df.loc[vn30_mask, "volume"].sum() or 1
    non_vn30_cur = df.loc[non_vn30_mask, "volume"].sum() or 1

    df.loc[vn30_mask, "volume"] = (df.loc[vn30_mask, "volume"] / vn30_cur * vn30_total).astype(int)
    df.loc[non_vn30_mask, "volume"] = (
        df.loc[non_vn30_mask, "volume"] / non_vn30_cur * non_vn30_total
    ).astype(int)

    # Update value = close * volume (for rows where value exists)
    has_value = df["value"].notna()
    df.loc[has_value, "value"] = (df.loc[has_value, "close"] * df.loc[has_value, "volume"]).astype(
        float
    )

    return df


def tag_schema_version(df: pd.DataFrame, schema_change_date) -> pd.DataFrame:
    """Add _schema_version column: 1 before schema_change_date, 2 on/after."""
    df = df.copy()
    df["_schema_version"] = df["trade_date"].apply(lambda d: 2 if d >= schema_change_date else 1)
    return df
