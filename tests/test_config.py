from pathlib import Path

import pytest
from pydantic import ValidationError

from common.config import Settings


def test_settings_build_exchange_subscriptions() -> None:
    settings = Settings(_env_file=None, market_symbols="btcusdt, ETHUSDT ")

    assert settings.symbols == ("BTCUSDT", "ETHUSDT")
    assert settings.binance_streams == ("btcusdt@trade", "ethusdt@trade")
    assert settings.mexc_trade_channels == (
        "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT",
        "spot@public.aggre.deals.v3.api.pb@100ms@ETHUSDT",
    )
    assert settings.local_data_dir == Path("data")
    assert not settings.live_pipeline_fail_on_data_loss


@pytest.mark.parametrize("symbols", ["btcusdt,", "BTC-USDT", ""])
def test_settings_reject_invalid_market_symbols(symbols: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, market_symbols=symbols)
