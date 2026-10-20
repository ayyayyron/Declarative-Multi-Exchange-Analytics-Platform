from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import psycopg
import psycopg_pool
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from common import config
from common.config import Settings
from dashboard import queries
from dashboard.queries import LatestTrade, PipelineStatus
from tests.dashboard.test_candles import sample_candle

APP = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"


def prepare_dashboard(monkeypatch: pytest.MonkeyPatch, symbol: str) -> MagicMock:
    st.cache_resource.clear()
    settings = Settings(_env_file=None, dashboard_symbol=symbol)
    monkeypatch.setattr(config, "get_settings", lambda: settings)
    pool = MagicMock()
    monkeypatch.setattr(psycopg_pool, "ConnectionPool", MagicMock(return_value=pool))
    now = datetime.now(UTC)
    latest = {
        source: LatestTrade(
            source,
            symbol,
            f"{source}:1",
            now,
            now,
            Decimal("100"),
            Decimal("0.01"),
            "buy",
        )
        for source in ("binance", "mexc")
    }
    monkeypatch.setattr(queries, "fetch_latest_trades", lambda *a: latest)
    monkeypatch.setattr(
        queries,
        "fetch_recent_prices",
        lambda *a: [{"trade_time": now, "source": "BINANCE", "price": 100.0}],
    )
    monkeypatch.setattr(
        queries,
        "fetch_recent_trades",
        lambda *a, **kw: [{"Time (UTC)": now, "Price (USDT)": 100.0}],
    )
    monkeypatch.setattr(
        queries,
        "fetch_pipeline_status",
        lambda *a: PipelineStatus(now, 10, 0, "batch=1"),
    )
    fetch = MagicMock(
        side_effect=lambda connection, market, source: [
            sample_candle(final=source == "mexc")
        ]
    )
    monkeypatch.setattr(queries, "fetch_candles", fetch)
    return fetch


@pytest.mark.parametrize("symbol", ["BTCUSDT", "ETHUSDT"])
def test_existing_views_and_candles_render_and_switch_exchange(
    monkeypatch: pytest.MonkeyPatch, symbol: str
) -> None:
    fetch = prepare_dashboard(monkeypatch, symbol)
    app = AppTest.from_file(str(APP)).run(timeout=15)
    assert not app.exception
    assert len(app.get("vega_lite_chart")) == 3
    assert len(app.dataframe) == 2
    assert {"Live trade prices", "Recent trades", "Pipeline", "1-minute candles"} <= {
        element.value for element in app.subheader
    }
    assert (
        next(m.value for m in app.metric if m.label == "Candle status") == "DEVELOPING"
    )
    assert app.dataframe[-1].value.iloc[0]["Volume (base asset)"] == "0.1234567890"
    app.selectbox(key="candle_exchange").select("MEXC").run(timeout=15)
    assert not app.exception
    assert fetch.call_args.args[1:] == (symbol, "mexc")
    assert next(m.value for m in app.metric if m.label == "Candle status") == "FINAL"


def test_empty_candles_leave_existing_trade_views_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = prepare_dashboard(monkeypatch, "BTCUSDT")
    fetch.side_effect = None
    fetch.return_value = []
    app = AppTest.from_file(str(APP)).run(timeout=15)
    assert not app.exception
    assert any("No 1-minute candles" in message.value for message in app.info)
    assert len(app.get("vega_lite_chart")) == 1
    assert len(app.dataframe) == 1


def test_missing_candle_migration_does_not_break_trade_views(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = prepare_dashboard(monkeypatch, "BTCUSDT")
    fetch.side_effect = psycopg.errors.UndefinedTable("not installed")
    app = AppTest.from_file(str(APP)).run(timeout=15)
    assert not app.exception
    assert any("make db-migrate" in message.value for message in app.info)
    assert len(app.dataframe) == 1
