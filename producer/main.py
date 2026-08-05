import argparse
import asyncio
import signal
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Literal, cast

from common.config import Settings, get_settings
from common.logging_config import configure_logging, get_logger
from common.models import Exchange
from producer.binance_client import build_binance_stream_url, run_binance_session
from producer.kafka_publisher import KafkaPublisher, build_producer
from producer.metrics import log_metrics, report_metrics
from producer.mexc_client import run_mexc_session
from producer.runtime import (
    BackoffPolicy,
    SessionRunner,
    SourceStats,
    supervise_source,
)

LOGGER = get_logger(__name__)
SourceSelection = Literal["binance", "mexc", "both"]


@dataclass(frozen=True)
class CliArgs:
    source: SourceSelection
    max_events: int | None


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def parse_args() -> CliArgs:
    parser = argparse.ArgumentParser(description="Publish exchange trades to Kafka")
    parser.add_argument(
        "--source",
        choices=("binance", "mexc", "both"),
        default="binance",
    )
    parser.add_argument(
        "--max-events",
        type=positive_int,
        help="stop after at least this many canonical events per source",
    )
    namespace = parser.parse_args()
    return CliArgs(
        source=cast(SourceSelection, namespace.source),
        max_events=cast(int | None, namespace.max_events),
    )


def selected_sources(selection: SourceSelection) -> tuple[Exchange, ...]:
    if selection == "both":
        return "binance", "mexc"
    return (selection,)


def build_sessions(
    sources: tuple[Exchange, ...],
    settings: Settings,
    publisher: KafkaPublisher,
    stats: dict[Exchange, SourceStats],
    stop_event: asyncio.Event,
    max_events: int | None,
) -> dict[Exchange, SessionRunner]:
    sessions: dict[Exchange, SessionRunner] = {}

    if "binance" in sources:
        if len(settings.binance_streams) != 1:
            raise ValueError("The Binance producer supports exactly one symbol")
        url = build_binance_stream_url(
            settings.binance_ws_base_url,
            settings.binance_streams[0],
        )
        sessions["binance"] = partial(
            run_binance_session,
            url,
            publisher,
            stats["binance"],
            stop_event,
            max_events,
        )

    if "mexc" in sources:
        if len(settings.mexc_trade_channels) != 1:
            raise ValueError("The MEXC producer supports exactly one symbol")
        sessions["mexc"] = partial(
            run_mexc_session,
            settings.mexc_ws_url,
            settings.mexc_trade_channels[0],
            publisher,
            stats["mexc"],
            stop_event,
            float(settings.mexc_heartbeat_interval_seconds),
            max_events,
        )

    return sessions


def install_signal_handlers(stop_event: asyncio.Event) -> Callable[[], None]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for current_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(current_signal, stop_event.set)
        except NotImplementedError:
            continue
        installed.append(current_signal)

    def remove_handlers() -> None:
        for current_signal in installed:
            loop.remove_signal_handler(current_signal)

    return remove_handlers


async def run(selection: SourceSelection, max_events: int | None) -> None:
    settings = get_settings()
    sources = selected_sources(selection)
    stop_event = asyncio.Event()
    remove_signal_handlers = install_signal_handlers(stop_event)
    producer = build_producer(
        str(settings.kafka_bootstrap_servers),
        settings.kafka_producer_client_id,
    )
    publisher = KafkaPublisher(
        trade_topic=settings.kafka_raw_trades_topic,
        dead_letter_topic=settings.kafka_dlq_topic,
        flush_timeout_seconds=settings.kafka_flush_timeout_seconds,
        producer=producer,
    )
    stats = {source: SourceStats(source) for source in sources}
    sessions = build_sessions(
        sources,
        settings,
        publisher,
        stats,
        stop_event,
        max_events,
    )
    backoff = BackoffPolicy(
        base_seconds=float(settings.producer_reconnect_base_seconds),
        max_seconds=float(settings.producer_reconnect_max_seconds),
    )
    workers = [
        asyncio.create_task(
            supervise_source(
                session=sessions[source],
                stop_event=stop_event,
                stats=stats[source],
                backoff=backoff,
            ),
            name=f"{source}-supervisor",
        )
        for source in sources
    ]
    metrics_task = asyncio.create_task(
        report_metrics(
            stop_event,
            stats,
            publisher,
            float(settings.producer_metrics_interval_seconds),
        ),
        name="producer-metrics",
    )

    try:
        await asyncio.gather(*workers)
    finally:
        stop_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        await metrics_task
        try:
            publisher.close()
        finally:
            remove_signal_handlers()
            log_metrics(stats, publisher)

    if publisher.stats.failed:
        raise RuntimeError(f"Kafka failed to deliver {publisher.stats.failed} events")


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    try:
        asyncio.run(run(args.source, args.max_events))
    except KeyboardInterrupt:
        LOGGER.info("Producer stopped")


if __name__ == "__main__":
    main()
