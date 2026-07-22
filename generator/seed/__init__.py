"""Seed data — frozen reference tickers from vnstock for reproducibility."""

from pathlib import Path

SEED_DIR = Path(__file__).parent


def load_seed_tickers() -> list[dict]:
    """Load the frozen ticker reference JSON (snapshotted from vnstock once)."""
    seed_file = SEED_DIR / "tickers_reference.json"
    if not seed_file.exists():
        raise FileNotFoundError(
            f"Seed file not found: {seed_file}. "
            "Run 'python -m generator.seed.fetch' to create it from vnstock."
        )
    import json

    with open(seed_file) as f:
        return json.load(f)
