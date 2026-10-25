from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
        side_effect=lambda connection, market, source, interval: [
            sample_candle(final=source == "mexc", interval=interval)
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
    assert {"Live trade prices", "Recent trades", "Pipeline", "Market candles"} <= {
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
    app.selectbox(key="candle_interval").select("5m").run(timeout=15)
    assert not app.exception
    assert fetch.call_args.kwargs == {"interval": "5m"}
    assert fetch.call_args.args[1:] == (symbol, "mexc")
    assert (
        app.dataframe[-1].value.iloc[0]["End (UTC, exclusive)"] == "2026-10-19 14:35:00"
    )
    app.selectbox(key="candle_interval").select("1m").run(timeout=15)
    assert fetch.call_args.kwargs == {"interval": "1m"}


def test_empty_candles_leave_existing_trade_views_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = prepare_dashboard(monkeypatch, "BTCUSDT")
    fetch.side_effect = None
    fetch.return_value = []
    app = AppTest.from_file(str(APP)).run(timeout=15)
    assert not app.exception
    assert any("No 1m candles" in message.value for message in app.info)
    app.selectbox(key="candle_interval").select("5m").run(timeout=15)
    assert any("CANDLE_INTERVAL=5m" in message.value for message in app.info)
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


@pytest.mark.parametrize(
    "interval,slots", [("15m", 3), ("1h", 12), ("1d", 288), ("1w", 2016)]
)
def test_rollup_selection_displays_partial_coverage_without_a_fake_revision(
    monkeypatch: pytest.MonkeyPatch, interval: str, slots: int
) -> None:
    fetch = prepare_dashboard(monkeypatch, "BTCUSDT")
    candle = sample_candle()
    fetch.side_effect = None
    fetch.return_value = [
        replace(
            candle,
            window_end=candle.window_start + timedelta(minutes=5 * slots),
            revision=None,
            base_windows=1,
            expected_windows=slots,
            all_observed_final=True,
            is_final=False,
        )
    ]
    app = AppTest.from_file(str(APP)).run(timeout=15)
    app.selectbox(key="candle_interval").select(interval).run(timeout=15)
    assert not app.exception
    assert fetch.call_args.kwargs == {"interval": interval}
    assert next(m.value for m in app.metric if m.label == "Candle status") == "PARTIAL"
    assert any("Partial base-window coverage" in item.value for item in app.warning)
    assert any(
        "computed from current 5m snapshots" in item.value for item in app.caption
    )
    assert app.dataframe[-1].value.iloc[0]["Observed 5m windows"] == f"1/{slots}"
    assert app.dataframe[-1].value.iloc[0]["Revision"] == "Computed"


def test_empty_rollup_directs_user_to_five_minute_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = prepare_dashboard(monkeypatch, "BTCUSDT")
    fetch.side_effect = None
    fetch.return_value = []
    app = AppTest.from_file(str(APP)).run(timeout=15)
    app.selectbox(key="candle_interval").select("1d").run(timeout=15)
    assert not app.exception
    assert any("CANDLE_INTERVAL=5m" in item.value for item in app.info)


def test_missing_rollup_migration_shows_setup_help(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = prepare_dashboard(monkeypatch, "BTCUSDT")
    fetch.side_effect = psycopg.errors.UndefinedFunction("not installed")
    app = AppTest.from_file(str(APP)).run(timeout=15)
    app.selectbox(key="candle_interval").select("1w").run(timeout=15)
    assert not app.exception
    assert any("make db-migrate" in item.value for item in app.info)
