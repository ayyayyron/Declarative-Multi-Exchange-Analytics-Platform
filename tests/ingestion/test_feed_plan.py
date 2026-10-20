import asyncio
from functools import partial
from pathlib import Path

import pytest

from common.config import Settings
from common.contracts import Exchange
from ingestion.configuration import load_configuration
from ingestion.feed_plan import (
    FeedSpec,
    SourceSelection,
    build_feed_plan,
    build_sessions,
    legacy_feed_plan,
    selected_sources,
    validate_feeds,
)
from ingestion.runtime import SourceStats
from tests.fakes import RecordingPublisher

EXAMPLE = Path(__file__).resolve().parents[2] / "config/feeds.example.yaml"


@pytest.mark.parametrize(
    ("selection", "expected"),
    [("binance", ("binance",)), ("mexc", ("mexc",)), ("both", ("binance", "mexc"))],
)
def test_source_selection_keeps_existing_cli_behavior(
    selection: SourceSelection, expected: tuple[Exchange, ...]
) -> None:
    assert selected_sources(selection) == expected


def test_yaml_expands_to_independent_exchange_symbol_feeds() -> None:
    feeds = build_feed_plan(load_configuration(EXAMPLE))
    assert [(feed.source, feed.symbol) for feed in feeds] == [
        ("binance", "BTCUSDT"),
        ("binance", "ETHUSDT"),
        ("mexc", "BTCUSDT"),
        ("mexc", "ETHUSDT"),
    ]


def test_sessions_share_publisher_but_have_independent_stats() -> None:
    feeds = build_feed_plan(load_configuration(EXAMPLE))
    settings = Settings(_env_file=None)
    publisher = RecordingPublisher()
    stats = {
        feed.feed_id: SourceStats(feed.source, symbol=feed.symbol) for feed in feeds
    }
    stop_event = asyncio.Event()

    sessions = build_sessions(feeds, settings, publisher, stats, stop_event, 5)

    assert set(sessions) == set(stats)
    assert len({id(stat) for stat in stats.values()}) == 4
    for feed in feeds:
        session = sessions[feed.feed_id]
        assert isinstance(session, partial)
        assert publisher in session.args
        assert stats[feed.feed_id] in session.args
        assert stats[feed.feed_id].feed_id == feed.feed_id
        assert stop_event in session.args
        assert session.args[-1] == 5
        if feed.source == "binance":
            assert session.args[0].endswith(f"/{feed.symbol.lower()}@trade")
        else:
            assert session.args[1].endswith(f"@100ms@{feed.symbol}")
    assert not publisher.frames
    assert all(stat.connections == 0 for stat in stats.values())


def test_legacy_environment_mode_now_supports_multiple_symbols() -> None:
    settings = Settings(_env_file=None, market_symbols="BTCUSDT,ETHUSDT")
    assert len(legacy_feed_plan("both", settings)) == 4


@pytest.mark.parametrize(
    "symbols", ["BTCUSDT,BTCUSDT", ",".join(f"COIN{i}USDT" for i in range(11))]
)
def test_legacy_mode_also_rejects_duplicates_and_excessive_feeds(symbols: str) -> None:
    with pytest.raises(ValueError):
        legacy_feed_plan("both", Settings(_env_file=None, market_symbols=symbols))


def test_empty_plan_is_rejected() -> None:
    with pytest.raises(ValueError, match="between 1 and 20"):
        validate_feeds(())


@pytest.mark.parametrize("symbol", ["btcUsdt", "BTC/USDT", "X", "BTC-USDT"])
def test_invalid_canonical_symbols_are_rejected(symbol: str) -> None:
    with pytest.raises(ValueError, match="canonical symbol"):
        FeedSpec("binance", symbol)
