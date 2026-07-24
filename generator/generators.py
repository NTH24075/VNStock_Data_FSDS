"""Data generators — OHLCV, foreign flow, corporate actions, financial ratios, streaming events.

All generators use a seeded numpy RNG for full reproducibility.
Drift simulation (03) is injected via the volatility_multiplier on the price walk.
"""

import uuid
from datetime import date, datetime, time, timedelta

import numpy as np
import pandas as pd

from generator.calendar import generate_trading_calendar, is_trading_day


def _price_range_for_exchange(exchange: str) -> tuple[float, float]:
    """Realistic initial close price range by exchange (VND)."""
    ranges = {"HOSE": (8000, 150000), "HNX": (6000, 80000), "UPCOM": (3000, 50000)}
    return ranges.get(exchange, (10000, 50000))


class Generator:
    """Produces offline and streaming datasets for the VN stock market simulation."""

    def __init__(self, config: dict, seed_tickers: list[dict]):
        """Initialize a reproducible generator and its trading-day window."""
        self.cfg = config
        self.rng = np.random.default_rng(config["random_seed"])
        self.tickers = self._select_tickers(seed_tickers)
        self.schema_change_date = date.fromisoformat(config["schema_change_date"])
        self.trading_days = _calendar_around_schema_change(
            self.schema_change_date,
            config["days_history"],
            old_schema_share=config.get("old_schema_share", 0.60),
        )
        self.n_days = len(self.trading_days)

    # ------------------------------------------------------------------
    # Ticker selection
    # ------------------------------------------------------------------

    def _select_tickers(self, seed: list[dict]) -> pd.DataFrame:
        """Select the configured universe and tag the configured VN30 subset."""
        if not seed:
            raise ValueError("Ticker seed must contain at least one ticker.")
        n = min(self.cfg["n_tickers"], len(seed))
        rows = []
        for t in seed[:n]:
            rows.append(
                {
                    "ticker_id": t["ticker_id"],
                    "ticker": t["ticker"],
                    "company_name": t.get("company_name", t["ticker"]),
                    "exchange": t.get("exchange", "HOSE"),
                    "icb_l1": t.get("icb_l1", "Financials"),
                    "icb_l2": t.get("icb_l2", ""),
                    "listing_date": t.get("listing_date", "2020-01-01"),
                    "is_active": t.get("is_active", True),
                }
            )
        df = pd.DataFrame(rows)
        df["vn30"] = False
        vn30_count = min(self.cfg["vn30_count"], len(df))
        df.iloc[:vn30_count, df.columns.get_loc("vn30")] = True
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
                new_close = close * np.exp(mu + sigma_eff * z)

                # Enforce exchange daily price limit (±7% HOSE, ±10% HNX, ±15% UPCOM)
                limit = {"HOSE": 0.07, "HNX": 0.10, "UPCOM": 0.15}[t["exchange"]]
                new_close = max(new_close, close * (1 - limit))
                new_close = min(new_close, close * (1 + limit))

                intraday_range = new_close * self.rng.uniform(0.005, limit * 0.6)
                open_ = new_close * (1 + self.rng.uniform(-0.01, 0.01))
                high = max(open_, new_close) + abs(intraday_range) * self.rng.uniform(0.3, 1.0)
                low = min(open_, new_close) - abs(intraday_range) * self.rng.uniform(0.3, 1.0)
                if low <= 0:
                    low = min(open_, new_close) * 0.95

                base_vol = 500_000 if t["vn30"] else 50_000
                volume = int(max(100, self.rng.lognormal(np.log(base_vol), 0.8)))

                # Value only for v2 schema
                value = (
                    float(round(new_close * volume)) if td_date >= self.schema_change_date else None
                )
                foreign_room = (
                    round(new_close * self.rng.uniform(0.01, 0.49), 2)
                    if td_date >= self.schema_change_date
                    else None
                )

                rows.append(
                    {
                        "ticker_id": t["ticker_id"],
                        "trade_date": td_date,
                        "open": round(open_, 2),
                        "high": round(high, 2),
                        "low": round(low, 2),
                        "close": round(new_close, 2),
                        "volume": volume,
                        "value": value,
                        "foreign_room": foreign_room,
                    }
                )

                # Carry forward capped close for next day's walk
                close = new_close

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Foreign flow generation
    # ------------------------------------------------------------------

    def generate_foreign_flow(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        """Generate one foreign-flow record per unique ticker and trading day."""
        source = ohlcv.drop_duplicates(subset=["ticker_id", "trade_date"]).copy()
        vn30_ids = set(self.tickers.loc[self.tickers["vn30"], "ticker_id"])
        participation = np.where(source["ticker_id"].isin(vn30_ids), 0.15, 0.03)
        base_value = source["close"].to_numpy() * source["volume"].to_numpy() * participation
        buy_value = base_value * self.rng.uniform(0.4, 0.6, len(source))
        sell_value = base_value * self.rng.uniform(0.4, 0.6, len(source))
        close = source["close"].to_numpy()

        return pd.DataFrame(
            {
                "ticker_id": source["ticker_id"].to_numpy(),
                "trade_date": source["trade_date"].to_numpy(),
                "foreign_buy_vol": (buy_value / close).astype(np.int64),
                "foreign_sell_vol": (sell_value / close).astype(np.int64),
                "foreign_buy_value": np.round(buy_value).astype(np.int64),
                "foreign_sell_value": np.round(sell_value).astype(np.int64),
            }
        )

    # ------------------------------------------------------------------
    # Corporate actions generation
    # ------------------------------------------------------------------

    def generate_corporate_actions(self) -> pd.DataFrame:
        """Generate monthly dividend/split actions across the ticker universe."""
        actions_per_month = 2 + int(self.cfg["n_tickers"] / 200)  # ~4 for 400 tickers
        rows = []
        tickers = self.tickers["ticker_id"].tolist()
        months = sorted({(d.year, d.month) for d in self.trading_days})
        action_idx = 0

        for yr, mo in months:
            month_days = [d for d in self.trading_days if d.year == yr and d.month == mo]
            if not month_days:
                continue
            for _ in range(actions_per_month):
                ticker = self.rng.choice(tickers)
                ex_date = self.rng.choice(month_days)
                action_type = self.rng.choice(
                    ["cash_dividend", "stock_dividend", "split"], p=[0.6, 0.25, 0.15]
                )
                ratio = {
                    "cash_dividend": self.rng.uniform(0.05, 0.20),
                    "stock_dividend": self.rng.uniform(0.05, 0.30),
                    "split": self.rng.choice([2.0, 3.0, 4.0]),
                }[action_type]
                rows.append(
                    {
                        "action_id": f"CA{action_idx:05d}",
                        "ticker_id": ticker,
                        "action_type": action_type,
                        "ex_date": ex_date,
                        "ratio": round(ratio, 4),
                        "announced_ts": datetime.combine(ex_date, time(9, 0))
                        - timedelta(days=int(self.rng.integers(5, 20))),
                    }
                )
                action_idx += 1
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Financial ratios generation
    # ------------------------------------------------------------------

    def generate_financial_ratios(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        """Generate one set of basic financial ratios per ticker and quarter."""
        rows = []
        tickers = ohlcv["ticker_id"].unique()
        quarters = sorted({f"{d.year}-Q{(d.month - 1) // 3 + 1}" for d in self.trading_days})

        for ticker in tickers:
            ticker_close = ohlcv[ohlcv["ticker_id"] == ticker]["close"]
            avg_close = ticker_close.mean() if len(ticker_close) > 0 else 25000
            for q in quarters:
                eps = avg_close * self.rng.uniform(0.005, 0.08)
                rows.append(
                    {
                        "ticker_id": ticker,
                        "report_quarter": q,
                        "eps": round(eps, 2),
                        "pe": round(avg_close / max(eps, 0.01), 2),
                        "pb": round(self.rng.uniform(0.5, 5.0), 2),
                        "roe": round(self.rng.uniform(0.05, 0.35), 4),
                        "published_ts": datetime.strptime(
                            f"{q.split('-')[0]}-{(int(q.split('-')[1][1]) * 3):02d}-28", "%Y-%m-%d"
                        ),
                    }
                )
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Streaming events generation
    # ------------------------------------------------------------------

    def generate_streaming_events(self, ohlcv: pd.DataFrame) -> list[dict]:
        """Generate intraday trade/quote/index_update events for each trading day."""
        events = []
        base_rate = self.cfg["base_events_per_min"]
        burst_mult = self.cfg["burst_multiplier"]
        late_rate = self.cfg["late_arrival_rate"]
        late_range = self.cfg.get("late_delay_sec_min_max", [5, 45])
        stream_days = min(self.cfg.get("stream_days", 1), len(self.trading_days))

        ticker_pool = self.tickers["ticker_id"].tolist()
        exchanges = self.tickers.set_index("ticker_id")["exchange"].to_dict()

        for td in self.trading_days[-stream_days:]:
            td_date = td if isinstance(td, date) else td.date()
            day_ohlcv = ohlcv[ohlcv["trade_date"] == td_date]
            if day_ohlcv.empty:
                continue
            day_lookup = (
                day_ohlcv.drop_duplicates(subset=["ticker_id"])
                .set_index("ticker_id")[["close", "low", "high"]]
                .to_dict("index")
            )

            # Sessions: ATO 09:00-09:15, continuous 09:15-11:30 + 13:00-14:30, ATC 14:30-14:45
            sessions = [
                ("ATO", time(9, 0), time(9, 15), burst_mult),
                ("continuous", time(9, 15), time(11, 30), 1.0),
                ("continuous", time(13, 0), time(14, 30), 1.0),
                ("ATC", time(14, 30), time(14, 45), burst_mult),
            ]

            for session_type, t_start, t_end, burst_factor in sessions:
                session_start = datetime.combine(td_date, t_start)
                session_end = datetime.combine(td_date, t_end)
                seconds = int((session_end - session_start).total_seconds())
                minutes = seconds // 60
                n_events = max(1, int(base_rate * minutes * burst_factor))
                for _ in range(n_events):
                    ticker = self.rng.choice(ticker_pool)
                    event_ts = session_start + timedelta(seconds=int(self.rng.integers(0, seconds)))
                    created_ts = event_ts
                    # Congestion makes late events more likely in auction bursts.
                    late_probability = min(
                        1.0,
                        late_rate * (1.5 if burst_factor > 1 else 0.8),
                    )
                    if self.rng.random() < late_probability:
                        delay = self.rng.integers(late_range[0], late_range[1] + 1)
                        created_ts = event_ts + timedelta(seconds=int(delay))

                    ticker_values = day_lookup.get(ticker)
                    base_price = ticker_values["close"] if ticker_values else 25000
                    low_price = ticker_values["low"] if ticker_values else base_price * 0.98
                    high_price = ticker_values["high"] if ticker_values else base_price * 1.02

                    if self.rng.random() < 0.85:  # trade event
                        events.append(
                            {
                                "event_id": self._uuid(),
                                "event_type": "trade",
                                "event_timestamp": event_ts.isoformat(),
                                "created_ts": created_ts.isoformat(),
                                "session_type": session_type,
                                "ticker": ticker,
                                "exchange": exchanges.get(ticker, "HOSE"),
                                "price": round(self.rng.uniform(low_price, high_price), 2),
                                "quantity": int(self.rng.integers(10, 50000)),
                                "side": self.rng.choice(["buy", "sell"]),
                                "trade_id": self._uuid(),
                            }
                        )
                    elif self.rng.random() < 0.80:  # quote event
                        bid = round(base_price * self.rng.uniform(0.97, 0.995), 2)
                        ask = round(base_price * self.rng.uniform(1.005, 1.03), 2)
                        events.append(
                            {
                                "event_id": self._uuid(),
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
                            }
                        )
                    else:  # index update
                        idx_val = 1000 + self.rng.normal(0, 5)
                        events.append(
                            {
                                "event_id": self._uuid(),
                                "event_type": "index_update",
                                "event_timestamp": event_ts.isoformat(),
                                "created_ts": created_ts.isoformat(),
                                "session_type": session_type,
                                "ticker": None,
                                "exchange": "HOSE",
                                "index_name": self.rng.choice(["VNINDEX", "VN30"]),
                                "index_value": round(float(idx_val), 2),
                                "index_change_pct": round(float(self.rng.normal(0, 0.5)), 2),
                            }
                        )

            self._align_intraday_with_daily(events, day_ohlcv, td_date)

        return events

    def _uuid(self) -> str:
        """Return a deterministic UUID from the seeded NumPy generator."""
        raw = self.rng.integers(0, 256, size=16, dtype=np.uint8).tobytes()
        return str(uuid.UUID(bytes=raw))

    def _align_intraday_with_daily(
        self,
        events: list[dict],
        day_ohlcv: pd.DataFrame,
        trade_date: date,
    ) -> None:
        """Make intraday trade totals and final ATC prices match daily OHLCV."""
        day_prefix = trade_date.isoformat()
        trade_positions: dict[str, list[int]] = {}
        for index, event in enumerate(events):
            if event["event_type"] == "trade" and event["event_timestamp"].startswith(day_prefix):
                trade_positions.setdefault(event["ticker"], []).append(index)

        daily_rows = day_ohlcv.drop_duplicates(subset=["ticker_id", "trade_date"])
        for _, daily in daily_rows.iterrows():
            positions = trade_positions.get(daily["ticker_id"], [])
            if not positions:
                continue
            total_volume = int(daily["volume"])
            weights = self.rng.dirichlet(np.ones(len(positions)))
            if total_volume >= len(positions):
                quantities = np.ones(len(positions), dtype=np.int64)
                quantities += self.rng.multinomial(
                    total_volume - len(positions),
                    weights,
                )
            else:
                quantities = self.rng.multinomial(total_volume, weights)
            for position, quantity in zip(positions, quantities, strict=True):
                events[position]["quantity"] = int(quantity)

            atc_positions = [
                position for position in positions if events[position]["session_type"] == "ATC"
            ]
            final_position = max(
                atc_positions or positions,
                key=lambda position: events[position]["event_timestamp"],
            )
            events[final_position]["price"] = round(float(daily["close"]), 2)


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


def _calendar_around_schema_change(
    schema_change_date: date,
    n_days: int,
    old_schema_share: float,
) -> list[date]:
    """Build an exact trading-day split around the schema change boundary."""
    if n_days <= 0:
        raise ValueError("days_history must be greater than zero.")
    if not 0 < old_schema_share < 1:
        raise ValueError("old_schema_share must be between zero and one.")

    old_days_count = int(round(n_days * old_schema_share))
    new_days_count = n_days - old_days_count
    old_days = []
    cursor = schema_change_date - timedelta(days=1)
    while len(old_days) < old_days_count:
        if is_trading_day(cursor):
            old_days.append(cursor)
        cursor -= timedelta(days=1)
    old_days.reverse()
    new_days = generate_trading_calendar(schema_change_date, new_days_count)
    return old_days + new_days
