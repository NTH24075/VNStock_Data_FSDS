from datetime import date

from generator.calendar import generate_trading_calendar, is_trading_day


class TestIsTradingDay:
    def test_weekday_monday_is_trading(self):
        assert is_trading_day(date(2025, 7, 21))  # Monday

    def test_saturday_is_not_trading(self):
        assert not is_trading_day(date(2025, 7, 19))  # Saturday

    def test_sunday_is_not_trading(self):
        assert not is_trading_day(date(2025, 7, 20))  # Sunday

    def test_vietnam_holiday_is_not_trading(self):
        assert not is_trading_day(date(2025, 1, 1))  # Tet Duong Lich

    def test_reunification_day_is_not_trading(self):
        assert not is_trading_day(date(2025, 4, 30))

    def test_lunar_tet_is_not_trading(self):
        assert not is_trading_day(date(2025, 1, 29))


class TestGenerateTradingCalendar:
    def test_returns_n_days(self):
        result = generate_trading_calendar(date(2025, 7, 1), 10)
        assert len(result) == 10

    def test_all_are_trading_days(self):
        result = generate_trading_calendar(date(2025, 7, 1), 30)
        for d in result:
            assert is_trading_day(d)

    def test_dates_are_sequential(self):
        result = generate_trading_calendar(date(2025, 7, 1), 5)
        for i in range(len(result) - 1):
            assert result[i] < result[i + 1]

    def test_skips_weekend(self):
        result = generate_trading_calendar(date(2025, 7, 18), 5)  # Friday
        assert date(2025, 7, 18) in result  # Friday
        assert date(2025, 7, 19) not in result  # Saturday
        assert date(2025, 7, 20) not in result  # Sunday
