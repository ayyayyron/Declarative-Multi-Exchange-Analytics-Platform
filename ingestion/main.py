"""Run configured capture workers with one Kafka publisher and coordinated shutdown."""

import argparse
import asyncio
import json
import signal
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from common.config import Settings, get_settings
from common.logging_config import configure_logging, get_logger
from ingestion.configuration import load_configuration
from ingestion.feed_plan import (
    FeedSpec,
    SourceSelection,
    build_feed_plan,
    build_sessions,
    legacy_feed_plan,
    validate_feeds,
)
from ingestion.kafka_publisher import KafkaPublisher, build_producer
from ingestion.metrics import log_metrics, report_metrics
from ingestion.runtime import BackoffPolicy, SourceStats, supervise_source

LOGGER = get_logger(__name__)
DEFAULT_CONFIG = Path("config/feeds.yaml")


@dataclass(frozen=True)
class CliArgs:
    config: Path | None
    source: SourceSelection | None
    max_frames: int | None
    dry_run: bool


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def parse_args() -> CliArgs:
    parser = argparse.ArgumentParser(
        description="Publish configured exchange trades to Kafka"
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--config", type=Path, help="YAML watchlist file (default: config/feeds.yaml)"
    )
    selection.add_argument(
        "--source",
        choices=("binance", "mexc", "both"),
        help="environment mode: use MARKET_SYMBOLS instead of YAML",
    )
    parser.add_argument(
        "--max-frames",
        "--max-events",
        dest="max_frames",
        type=positive_int,
        help="captured frames per feed (--max-events is a legacy alias)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and print the feed plan without opening network connections",
    )
    namespace = parser.parse_args()
    return CliArgs(
        config=namespace.config,
        source=cast(SourceSelection | None, namespace.source),
        max_frames=namespace.max_frames,
        dry_run=namespace.dry_run,
    )


def resolve_feeds(args: CliArgs, settings: Settings) -> tuple[FeedSpec, ...]:
    """Choose YAML watchlists or explicit environment-based market selection."""
    if args.source is not None:
        return legacy_feed_plan(args.source, settings)
    return build_feed_plan(load_configuration(args.config or DEFAULT_CONFIG))


def install_signal_handlers(stop_event: asyncio.Event) -> Callable[[], None]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    for current_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(current_signal, stop_event.set)
        except NotImplementedError:
            continue
        installed.append(current_signal)

    def remove_handlers() -> None:
        for current_signal in installed:
            loop.remove_signal_handler(current_signal)
            signal.signal(current_signal, previous[current_signal])

    return remove_handlers


async def run(
    feeds: tuple[FeedSpec, ...], max_frames: int | None, settings: Settings
) -> None:
    """Schedule one supervisor per feed, then flush deliveries when workers stop."""
    validate_feeds(feeds)
    if max_frames is not None and max_frames < 1:
        raise ValueError("max_frames must be positive")
    stop_event = asyncio.Event()
    producer = build_producer(
        str(settings.kafka_bootstrap_servers), settings.kafka_producer_client_id
    )
    publisher = KafkaPublisher(
        frame_topic=settings.kafka_raw_frames_topic,
        flush_timeout_seconds=settings.kafka_flush_timeout_seconds,
        producer=producer,
        enqueue_timeout_seconds=float(settings.kafka_enqueue_timeout_seconds),
        stop_event=stop_event,
    )
    stats = {
        feed.feed_id: SourceStats(feed.source, symbol=feed.symbol) for feed in feeds
    }
    workers: list[asyncio.Task[None]] = []
    metrics_task: asyncio.Task[None] | None = None
    remove_signal_handlers: Callable[[], None] | None = None

    try:
        sessions = build_sessions(
            feeds, settings, publisher, stats, stop_event, max_frames
        )
        backoff = BackoffPolicy(
            base_seconds=float(settings.producer_reconnect_base_seconds),
            max_seconds=float(settings.producer_reconnect_max_seconds),
        )
        remove_signal_handlers = install_signal_handlers(stop_event)
        workers = [
            asyncio.create_task(
                supervise_source(
                    sessions[feed.feed_id], stop_event, stats[feed.feed_id], backoff
                ),
                name=f"{feed.feed_id}-supervisor",
            )
            for feed in feeds
        ]
        metrics_task = asyncio.create_task(
            report_metrics(
                stop_event,
                stats,
                publisher,
                float(settings.producer_metrics_interval_seconds),
            ),
            name="ingestion-metrics",
        )
        await asyncio.gather(*workers)
    finally:
        stop_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        try:
            if metrics_task is not None:
                await metrics_task
        finally:
            try:
                publisher.close()
            finally:
                if remove_signal_handlers is not None:
                    remove_signal_handlers()
                log_metrics(stats, publisher)


def main() -> None:
    """Resolve the capture plan; print it for dry-run or start the async runtime."""
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    try:
        feeds = resolve_feeds(args, settings)
    except ValueError as error:
        LOGGER.error("%s", error)
        raise SystemExit(2) from None
    if args.dry_run:
        print(
            json.dumps(
                [
                    {
                        "feed_id": feed.feed_id,
                        "exchange": feed.source,
                        "symbol": feed.symbol,
                        "event_type": "trade",
                    }
                    for feed in feeds
                ],
                indent=2,
            )
        )
        return
    LOGGER.info(
        "Starting ingestion: feeds=%d max_frames_per_feed=%s",
        len(feeds),
        args.max_frames,
    )
    try:
        asyncio.run(run(feeds, args.max_frames, settings))
    except KeyboardInterrupt:
        LOGGER.info("Ingestion stopped")


if __name__ == "__main__":
    main()
