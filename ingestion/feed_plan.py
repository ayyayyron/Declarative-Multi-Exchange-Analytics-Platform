"""Expand watchlists into feed specifications and prepare connector calls."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Literal

from common.config import Settings
from common.contracts import Exchange
from ingestion.connectors.binance.client import (
    build_binance_stream_url,
    run_binance_session,
)
from ingestion.connectors.mexc.client import run_mexc_session
from ingestion.kafka_publisher import FramePublisher
from ingestion.runtime import SessionRunner, SourceStats

if TYPE_CHECKING:
    from ingestion.configuration import IngestionConfiguration

MAX_FEEDS = 20
SourceSelection = Literal["binance", "mexc", "both"]


@dataclass(frozen=True)
class FeedSpec:
    source: Exchange
    symbol: str

    def __post_init__(self) -> None:
        if self.source not in ("binance", "mexc"):
            raise ValueError(f"Unsupported exchange: {self.source}")
        if not re.fullmatch(r"[A-Z0-9]{3,20}", self.symbol):
            raise ValueError(f"Invalid canonical symbol: {self.symbol}")

    @property
    def feed_id(self) -> str:
        return f"{self.source}:{self.symbol}:trade"


def selected_sources(selection: SourceSelection) -> tuple[Exchange, ...]:
    if selection == "both":
        return "binance", "mexc"
    return (selection,)


def validate_feeds(feeds: tuple[FeedSpec, ...]) -> None:
    if not 1 <= len(feeds) <= MAX_FEEDS:
        raise ValueError(f"Configure between 1 and {MAX_FEEDS} logical trade feeds")
    if len({feed.feed_id for feed in feeds}) != len(feeds):
        raise ValueError(
            "Duplicate exchange/pair trade feeds; combine overlapping watchlists"
        )


def build_feed_plan(configuration: IngestionConfiguration) -> tuple[FeedSpec, ...]:
    """Expand selected exchange/pair combinations and enforce the process feed limit."""
    feeds: list[FeedSpec] = []
    for watchlist in configuration.watchlists:
        for source in watchlist.exchanges:
            if source not in configuration.exchanges:
                raise ValueError(
                    f"Watchlist {watchlist.name} references "
                    f"unconfigured exchange {source}"
                )
            for pair in watchlist.pairs:
                feeds.append(FeedSpec(source, pair.replace("/", "")))
    result = tuple(feeds)
    validate_feeds(result)
    return result


def legacy_feed_plan(
    selection: SourceSelection, settings: Settings
) -> tuple[FeedSpec, ...]:
    feeds = tuple(
        FeedSpec(source, symbol)
        for source in selected_sources(selection)
        for symbol in settings.symbols
    )
    validate_feeds(feeds)
    return feeds


def build_sessions(
    feeds: tuple[FeedSpec, ...],
    settings: Settings,
    publisher: FramePublisher,
    stats: Mapping[str, SourceStats],
    stop_event: asyncio.Event,
    max_frames: int | None,
) -> dict[str, SessionRunner]:
    """Bind each feed's connector arguments without opening sockets.

    The returned callables share a publisher but retain independent
    statistics.
    """
    validate_feeds(feeds)
    sessions: dict[str, SessionRunner] = {}
    for feed in feeds:
        if feed.source == "binance":
            url = build_binance_stream_url(
                settings.binance_ws_base_url, f"{feed.symbol.lower()}@trade"
            )
            sessions[feed.feed_id] = partial(
                run_binance_session,
                url,
                publisher,
                stats[feed.feed_id],
                stop_event,
                max_frames,
            )
        else:
            channel = (
                "spot@public.aggre.deals.v3.api.pb@"
                f"{settings.mexc_trade_update_speed}@{feed.symbol}"
            )
            sessions[feed.feed_id] = partial(
                run_mexc_session,
                settings.mexc_ws_url,
                channel,
                publisher,
                stats[feed.feed_id],
                stop_event,
                float(settings.mexc_heartbeat_interval_seconds),
                max_frames,
            )
    return sessions
