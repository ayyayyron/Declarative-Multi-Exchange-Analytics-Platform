from datetime import UTC, datetime, timedelta
from decimal import Decimal

from dashboard.queries import age_seconds, calculate_spread


def test_symmetric_spread_uses_midpoint() -> None:
    difference, spread_bps = calculate_spread(
        binance_price=Decimal("100"),
        mexc_price=Decimal("101"),
    )

    assert difference == Decimal("1")
    assert spread_bps.quantize(Decimal("0.0001")) == Decimal("99.5025")


def test_age_seconds_is_non_negative() -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)

    assert age_seconds(now - timedelta(seconds=7), now) == 7
    assert age_seconds(now + timedelta(seconds=1), now) == 0
