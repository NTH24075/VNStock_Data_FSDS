"""Data generators — OHLCV, foreign flow, corporate actions, financial ratios, streaming events.

All generators use a seeded numpy RNG for full reproducibility.
Drift simulation (03) is injected via the volatility_multiplier on the price walk.
"""

import uuid
from datetime import date, datetime, timedelta, time
from typing import Optional

import numpy as np
import pandas as pd

from generator.calendar import generate_trading_calendar


def _price_range_for_exchange(exchange: str) -> tuple[float, float]:
    """Realistic initial close price range by exchange (VND)."""
    ranges = {"HOSE": (8000, 150000), "HNX": (6000, 80000), "UPCOM": (3000, 50000)}
    return ranges.get(exchange, (10000, 50000))


class Generator:
    """Produces offline and streaming datasets for the VN stock market simulation."""

    def __init__(self, config: dict, seed_tickers: list[dict]):
        self.cfg = config
        self.rng = np.random.default_rng(config["random_seed"])
        self.tickers = self._select_tickers(seed_tickers)
        # Trading calendar: split evenly before and after schema_change_date
        self.schema_change_date = date.fromisoformat(config["schema_change_date"])
        half_days = config["days_history"] // 2
        start_date = self.schema_change_date - timedelta(days=half_days * 2)  # ~half before change
        self.trading_days = generate_trading_calendar(
            start=start_date,
            n_days=config["days_history"],
        )
        self.n_days = len(self.trading_days)

    # ------------------------------------------------------------------
    # Ticker selection
    # ------------------------------------------------------------------

    def _select_tickers(self, seed: list[dict]) -> pd.DataFrame:
        n = min(self.cfg["n_tickers"], len(seed))
        rows = []
        for t in seed[:n]:
            rows.append({
                "ticker_id": t["ticker_id"],
                "ticker": t["ticker"],
                "company_name": t.get("company_name", t["ticker"]),
                "exchange": t.get("exchange", "HOSE"),
                "icb_l1": t.get("icb_l1", "Financials"),
                "icb_l2": t.get("icb_l2", ""),
                "listing_date": t.get("listing_date", "2020-01-01"),
                "is_active": t.get("is_active", True),
            })
        df = pd.DataFrame(rows)
        df["vn30"] = False
        df.iloc[: self.cfg["vn30_count"], df.columns.get_loc("vn30")] = True
        return df

    # ------------------------------------------------------------------
    # OHLCV generation
    # ------------------------------------------------------------------

    def generate_ohlcv(self) -> pd.DataFrame:
        """Generate daily OHLCV via geometric random walk per ticker."""
        rows = []
        drift_cfg = self.cfg.get("drift", {})
        drift_on = drift_cfg.get("enabled", False)
        drift_start = date.fromisoformat(drift_cfg["start_date"]) if drift_on else None
        vol_mult = drift_cfg.get("volatility_multiplier", 1.0)
        small_amp = drift_cfg.get("smallcap_amplifier", 1.0)

        for _, t in self.tickers.iterrows():
            low0, high0 = _price_range_for_exchange(t["exchange"])
            close = self.rng.uniform(low0, high0)

            # Industry-based mu/sigma
            if t["icb_l1"] == "Financials":
                mu, sigma = -0.0001, 0.012
            elif t["icb_l1"] == "Real Estate":
                mu, sigma = 0.0000, 0.022
            else:
                mu, sigma = 0.0002, 0.016

            for day_idx, td in enumerate(self.trading_days):
                td_date = td if isinstance(td, date) else td.date()

                # Drift amplification
                sigma_eff = sigma
                if drift_on and td_date >= drift_start:
                    ramp = _drift_ramp(day_idx, self.trading_days, drift_start, drift_cfg)
                    sigma_eff = sigma * (1 + (vol_mult - 1) * ramp)
                    if not t["vn30"]:
                        sigma_eff *= small_amp

                z = self.rng.normal(0, 1)
                close = close * np.exp(mu + sigma_eff * z)

                limit = {"HOSE": 0.07, "HNX": 0.10, "UPCOM": 0.15}[t["exchange"]]
                intraday_range = close * self.rng.uniform(0.005, limit * 0.6)
                open_ = close * (1 + self.rng.uniform(-0.01, 0.01))
                high = max(open_, close) + abs(intraday_range) * self.rng.uniform(0.3, 1.0)
                low = min(open_, close) - abs(intraday_range) * self.rng.uniform(0.3, 1.0)
                if low <= 0:
                    low = min(open_, close) * 0.95

                base_vol = 500_000 if t["vn30"] else 50_000
                volume = int(max(100, self.rng.lognormal(np.log(base_vol), 0.8)))

                # Value only for v2 schema
                value = float(round(close * volume)) if td_date >= self.schema_change_date else None
                foreign_room = round(close * self.rng.uniform(0.01, 0.49), 2) if td_date >= self.schema_change_date else None

                rows.append({
                    "ticker_id": t["ticker_id"],
                    "trade_date": td_date,
                    "open": round(open_, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "close": round(close, 2),
                    "volume": volume,
                    "value": value,
                    "foreign_room": foreign_room,
                })

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Foreign flow generation
    # ------------------------------------------------------------------

    def generate_foreign_flow(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in ohlcv.iterrows():
            is_vn30 = row["ticker_id"] in self.tickers[self.tickers["vn30"]]["ticker_id"].values
            base_val = row["close"] * row["volume"] * (0.15 if is_vn30 else 0.03)
            buy_val = base_val * self.rng.uniform(0.4, 0.6)
            sell_val = base_val * self.rng.uniform(0.4, 0.6)
            avg_price = row["close"]
            rows.append({
                "ticker_id": row["ticker_id"],
                "trade_date": row["trade_date"],
                "foreign_buy_vol": int(buy_val / avg_price),
                "foreign_sell_vol": int(sell_val / avg_price),
                "foreign_buy_value": round(buy_val),
                "foreign_sell_value": round(sell_val),
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Corporate actions generation
    # ------------------------------------------------------------------

    def generate_corporate_actions(self) -> pd.DataFrame:
        actions_per_month = 2 + int(self.cfg["n_tickers"] / 200)  # ~4 for 400 tickers
        rows = []
        tickers = self.tickers["ticker_id"].tolist()
        months = sorted(set((d.year, d.month) for d in self.trading_days))
        action_idx = 0

        for yr, mo in months:
            month_days = [d for d in self.trading_days if d.year == yr and d.month == mo]
            if not month_days:
                continue
            for _ in range(actions_per_month):
                ticker = self.rng.choice(tickers)
                ex_date = self.rng.choice(month_days)
                action_type = self.rng.choice(["cash_dividend", "stock_dividend", "split"], p=[0.6, 0.25, 0.15])
                ratio = {"cash_dividend": self.rng.uniform(0.05, 0.20),
                         "stock_dividend": self.rng.uniform(0.05, 0.30),
                         "split": self.rng.choice([2.0, 3.0, 4.0])}[action_type]
                rows.append({
                    "action_id": f"CA{action_idx:05d}",
                    "ticker_id": ticker,
                    "action_type": action_type,
                    "ex_date": ex_date,
                    "ratio": round(ratio, 4),
                    "announced_ts": datetime.combine(ex_date, time(9, 0)) - timedelta(days=int(self.rng.integers(5, 20))),
                })
                action_idx += 1
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Financial ratios generation
    # ------------------------------------------------------------------

    def generate_financial_ratios(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        rows = []
        tickers = ohlcv["ticker_id"].unique()
        quarters = sorted(set(f"{d.year}-Q{(d.month - 1) // 3 + 1}" for d in self.trading_days))

        for ticker in tickers:
            ticker_close = ohlcv[ohlcv["ticker_id"] == ticker]["close"]
            avg_close = ticker_close.mean() if len(ticker_close) > 0 else 25000
            for q in quarters:
                eps = avg_close * self.rng.uniform(0.005, 0.08)
                rows.append({
                    "ticker_id": ticker,
                    "report_quarter": q,
                    "eps": round(eps, 2),
                    "pe": round(avg_close / max(eps, 0.01), 2),
                    "pb": round(self.rng.uniform(0.5, 5.0), 2),
                    "roe": round(self.rng.uniform(0.05, 0.35), 4),
                    "published_ts": datetime.strptime(f"{q.split('-')[0]}-{(int(q.split('-')[1][1]) * 3):02d}-28", "%Y-%m-%d"),
                })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Streaming events generation
    # ------------------------------------------------------------------

    def generate_streaming_events(self, ohlcv: pd.DataFrame) -> list[dict]:
        """Generate intraday trade/quote/index_update events for each trading day."""
        events = []
        base_rate = self.cfg["base_events_per_min"]
        burst_mult = self.cfg["burst_multiplier"]
        burst_windows = self.cfg.get("burst_windows", [])
        late_rate = self.cfg["late_arrival_rate"]
        late_range = self.cfg.get("late_delay_sec_min_max", [5, 45])

        ticker_pool = self.tickers["ticker_id"].tolist()
        exchanges = self.tickers.set_index("ticker_id")["exchange"].to_dict()

        for td in self.trading_days:
            td_date = td if isinstance(td, date) else td.date()
            day_ohlcv = ohlcv[ohlcv["trade_date"] == td_date]
            if day_ohlcv.empty:
                continue

            # Sessions: ATO 09:00-09:15, continuous 09:15-11:30 + 13:00-14:30, ATC 14:30-14:45
            sessions = [
                ("ATO", time(9, 0), time(9, 15), burst_mult),
                ("continuous", time(9, 15), time(11, 30), 1.0),
                ("continuous", time(13, 0), time(14, 30), 1.0),
                ("ATC", time(14, 30), time(14, 45), burst_mult),
            ]

            for session_type, t_start, t_end, burst_factor in sessions:
                minutes = int((datetime.combine(td_date, t_end) - datetime.combine(td_date, t_start)).total_seconds() / 60)
                n_events = max(1, int(base_rate * minutes * burst_factor))
                for _ in range(n_events):
                    ticker = self.rng.choice(ticker_pool)
                    event_ts = datetime.combine(td_date, time(
                        self.rng.integers(t_start.hour, t_end.hour if t_end.hour > t_start.hour else t_start.hour + 1),
                        self.rng.integers(0, 59),
                        self.rng.integers(0, 59),
                    ))
                    created_ts = event_ts
                    if self.rng.random() < late_rate:
                        delay = self.rng.integers(late_range[0], late_range[1] + 1)
                        created_ts = event_ts + timedelta(seconds=int(delay))

                    ticker_row = day_ohlcv[day_ohlcv["ticker_id"] == ticker]
                    base_price = ticker_row["close"].iloc[0] if not ticker_row.empty else 25000

                    if self.rng.random() < 0.85:  # trade event
                        events.append({
                            "event_id": str(uuid.uuid4()),
                            "event_type": "trade",
                            "event_timestamp": event_ts.isoformat(),
                            "created_ts": created_ts.isoformat(),
                            "session_type": session_type,
                            "ticker": ticker,
                            "exchange": exchanges.get(ticker, "HOSE"),
                            "price": round(base_price * self.rng.uniform(0.98, 1.02), 2),
                            "quantity": int(self.rng.integers(10, 50000)),
                            "side": self.rng.choice(["buy", "sell"]),
                            "trade_id": str(uuid.uuid4()),
                        })
                    elif self.rng.random() < 0.80:  # quote event
                        bid = round(base_price * self.rng.uniform(0.97, 0.995), 2)
                        ask = round(base_price * self.rng.uniform(1.005, 1.03), 2)
                        events.append({
                            "event_id": str(uuid.uuid4()),
                            "event_type": "quote",
                            "event_timestamp": event_ts.isoformat(),
                            "created_ts": created_ts.isoformat(),
                            "session_type": session_type,
                            "ticker": ticker,
                            "exchange": exchanges.get(ticker, "HOSE"),
                            "bid_price": bid,
                            "bid_qty": int(self.rng.integers(100, 100000)),
                            "ask_price": ask,
                            "ask_qty": int(self.rng.integers(100, 100000)),
                        })
                    else:  # index update
                        idx_val = 1000 + self.rng.normal(0, 5)
                        events.append({
                            "event_id": str(uuid.uuid4()),
                            "event_type": "index_update",
                            "event_timestamp": event_ts.isoformat(),
                            "created_ts": created_ts.isoformat(),
                            "session_type": session_type,
                            "ticker": None,
                            "exchange": "HOSE",
                            "index_name": self.rng.choice(["VNINDEX", "VN30"]),
                            "index_value": round(float(idx_val), 2),
                            "index_change_pct": round(float(self.rng.normal(0, 0.5)), 2),
                        })

        return events


def _drift_ramp(day_idx: int, trading_days: list, drift_start: date, drift_cfg: dict) -> float:
    """Return 0.0 to 1.0 ramp factor for drift transition."""
    mode = drift_cfg.get("mode", "gradual")
    ramp_days = drift_cfg.get("ramp_trading_days", 10)

    drift_day_indices = [i for i, d in enumerate(trading_days) if d >= drift_start]
    if not drift_day_indices:
        return 0.0
    first_drift = drift_day_indices[0]
    if day_idx < first_drift:
        return 0.0
    if mode == "abrupt":
        return 1.0
    offset = day_idx - first_drift
    return min(1.0, offset / ramp_days)
