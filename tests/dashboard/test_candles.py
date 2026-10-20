from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock

import pytest

from dashboard.charts import candle_chart, candle_table
from dashboard.queries import StoredCandle, fetch_candles


def sample_candle(final: bool = False) -> StoredCandle:
    start = datetime(2026, 10, 19, 14, 33, tzinfo=UTC)
    return StoredCandle(
        window_start=start,
        window_end=start + timedelta(minutes=1),
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("0.1234567890"),
        trade_count=4,
        revision=2,
        is_final=final,
        updated_at=datetime.now(UTC),
    )


def test_fetch_candles_preserves_decimals_and_utc_boundaries() -> None:
    candle = sample_candle()
    connection = Mock()
    connection.execute.return_value.fetchall.return_value = [
        (
            int(candle.window_start.timestamp() * 1000),
            int(candle.window_end.timestamp() * 1000),
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.trade_count,
            candle.revision,
            candle.is_final,
            candle.updated_at,
        )
    ]
    result = fetch_candles(connection, "ETHUSDT", "mexc")
    assert result == [candle]
    assert connection.execute.call_args.args[1] == ("ETHUSDT", "mexc", 200)
    query = connection.execute.call_args.args[0]
    assert "interval='1m'" in query and "LIMIT %s" in query
    assert "ORDER BY window_start_ms" in query


@pytest.mark.parametrize("limit", [0, -1, 2001, True])
def test_candle_read_is_bounded(limit: int) -> None:
    connection = Mock()
    with pytest.raises(ValueError, match="limit"):
        fetch_candles(connection, "BTCUSDT", "binance", limit)
    connection.execute.assert_not_called()


def test_unknown_exchange_is_rejected() -> None:
    with pytest.raises(ValueError, match="exchange"):
        fetch_candles(Mock(), "BTCUSDT", "unknown")


def test_candle_chart_has_wick_body_doji_and_volume_with_utc_axis() -> None:
    price_chart, volume_chart = candle_chart([sample_candle()])
    price, volume = price_chart.to_dict(), volume_chart.to_dict()
    assert [layer["mark"]["type"] for layer in price["layer"]] == [
        "rule",
        "bar",
        "tick",
    ]
    assert price["layer"][0]["encoding"]["x"]["scale"]["type"] == "utc"
    assert volume["encoding"]["y"]["field"] == "volume"
    assert (
        price["layer"][0]["encoding"]["x"]["scale"] == volume["encoding"]["x"]["scale"]
    )
    records = next(iter(price["datasets"].values()))
    assert records[0]["start_utc"] == "2026-10-19 14:33:00 UTC"
    assert records[0]["status"] == "Developing"


def test_table_keeps_exact_volume_and_final_status() -> None:
    frame = candle_table([sample_candle(final=True)])
    assert frame.iloc[0]["Volume (base asset)"] == "0.1234567890"
    assert frame.iloc[0]["Status"] == "Final"
    assert frame.iloc[0]["End (UTC, exclusive)"] == "2026-10-19 14:34:00"
