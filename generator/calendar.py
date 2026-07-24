"""HOSE trading calendar — Monday–Friday, minus Vietnamese public holidays."""

from datetime import date, timedelta

# Vietnamese public holidays (fixed solar + lunar approximated for simulation window)
VN_HOLIDAYS = {
    # Solar fixed
    date(2025, 1, 1),  # Tết Dương lịch
    date(2025, 4, 30),  # Reunification Day
    date(2025, 5, 1),  # Labour Day
    date(2025, 9, 2),  # National Day
    # Lunar Tet 2025 (approximate: Jan 28-31)
    date(2025, 1, 28),
    date(2025, 1, 29),
    date(2025, 1, 30),
    date(2025, 1, 31),
    # Hung Kings Festival (10/3 lunar ≈ Apr 7)
    date(2025, 4, 7),
    # Lunar Tet 2026 (approximate: Feb 16-19)
    date(2026, 2, 16),
    date(2026, 2, 17),
    date(2026, 2, 18),
    date(2026, 2, 19),
    # Hung Kings 2026
    date(2026, 4, 26),
    # 2025 extra holiday bridging
    date(2025, 5, 2),
}


def is_trading_day(d: date) -> bool:
    """Return True if d is a HOSE trading day."""
    if d.weekday() >= 5:  # Saturday or Sunday
        return False
    return d not in VN_HOLIDAYS


def generate_trading_calendar(start: date, n_days: int) -> list[date]:
    """Generate the next `n_days` trading days from `start` (inclusive)."""
    days = []
    current = start
    while len(days) < n_days:
        if is_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days
