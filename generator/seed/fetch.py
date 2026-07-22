"""Fetch real ticker reference data from vnstock and freeze as JSON seed file.

Uses vnstock >= 4.0 API (vnstock.api.listing).
"""

import json
from collections import Counter
from pathlib import Path

SEED_DIR = Path(__file__).parent
SEED_FILE = SEED_DIR / "tickers_reference.json"


def fetch_and_freeze():
    """Fetch ticker list from vnstock and save to seed file. Run once."""
    try:
        from vnstock.api.listing import Listing

        l = Listing()
        symbols = l.all_symbols()

        # Get exchange mapping
        exch_df = l.symbols_by_exchange("HOSE")
        exch_df = exch_df.drop_duplicates(subset="symbol")
        exch_map = dict(zip(exch_df["symbol"], exch_df["exchange"]))

        records = []
        for _, row in symbols.iterrows():
            sym = row["symbol"]
            records.append({
                "ticker_id": sym,
                "ticker": sym,
                "company_name": str(row.get("organ_name", sym)),
                "exchange": exch_map.get(sym, "HOSE"),
                "icb_l1": "",
                "icb_l2": "",
                "listing_date": "2020-01-01",
                "is_active": True,
            })

        with open(SEED_FILE, "w") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        exch_counts = Counter(r["exchange"] for r in records)
        print(f"Fetched {len(records)} tickers → {SEED_FILE}")
        print(f"  By exchange: {dict(exch_counts)}")

    except ImportError:
        print("vnstock not installed. Install with: uv add vnstock")
        raise
    except Exception as e:
        print(f"vnstock fetch failed: {e}")
        print("Creating empty seed file — fill manually or fix vnstock.")
        with open(SEED_FILE, "w") as f:
            json.dump([], f)


if __name__ == "__main__":
    fetch_and_freeze()
