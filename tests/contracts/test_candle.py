from decimal import Decimal

import pytest
from pydantic import ValidationError

from common.contracts.candle import CandleSnapshot


def values() -> dict[str, object]:
    return {
        "source": "binance",
        "symbol": "BTCUSDT",
        "interval": "1m",
        "window_start_ms": 1792317600000,
        "window_end_ms": 1792317660000,
        "open": Decimal("100"),
        "high": Decimal("102"),
        "low": Decimal("99"),
        "close": Decimal("101"),
        "volume": Decimal("0.1234567890"),
        "trade_count": 3,
        "revision": 1,
        "is_final": False,
    }


def test_candle_preserves_decimal_precision() -> None:
    candle = CandleSnapshot.model_validate(values())
    assert candle.volume == Decimal("0.1234567890")
    assert candle.interval == "1m"


@pytest.mark.parametrize(
    "change",
    [
        {"window_start_ms": 1792317600001},
        {"window_end_ms": 1792317660001},
        {"interval": "15m"},
        {"low": "101"},
        {"high": "100"},
        {"volume": "NaN"},
        {"price": "100"},
        {"open": "Infinity"},
        {"quantity": "1"},
        {"trade_count": 0},
        {"revision": True},
        {"revision": 0},
        {"is_final": "false"},
        {"source": "unknown"},
    ],
)
def test_invalid_candle_is_rejected(change: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        CandleSnapshot.model_validate(values() | change)


def test_five_minute_boundaries() -> None:
    candle = CandleSnapshot.model_validate(
        values()
        | {
            "interval": "5m",
            "window_end_ms": 1792317900000,
        }
    )
    assert candle.window_end_ms - candle.window_start_ms == 300_000
