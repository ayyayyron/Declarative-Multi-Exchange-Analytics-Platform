import argparse
import asyncio
from dataclasses import dataclass
from typing import cast

from common.config import get_settings
from common.logging_config import configure_logging, get_logger
from producer.binance_client import build_binance_stream_url, stream_binance_trades
from producer.kafka_publisher import KafkaTradePublisher, build_producer

LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class CliArgs:
    max_events: int | None


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def parse_args() -> CliArgs:
    parser = argparse.ArgumentParser(description="Publish Binance trades to Kafka")
    parser.add_argument("--max-events", type=positive_int)
    namespace = parser.parse_args()
    return CliArgs(max_events=cast(int | None, namespace.max_events))


async def run(max_events: int | None) -> None:
    settings = get_settings()
    if len(settings.binance_streams) != 1:
        raise ValueError("The initial Binance producer supports exactly one symbol")

    producer = build_producer(
        str(settings.kafka_bootstrap_servers),
        settings.kafka_producer_client_id,
    )
    publisher = KafkaTradePublisher(
        topic=settings.kafka_raw_trades_topic,
        flush_timeout_seconds=settings.kafka_flush_timeout_seconds,
        producer=producer,
    )
    url = build_binance_stream_url(
        settings.binance_ws_base_url,
        settings.binance_streams[0],
    )

    try:
        received = await stream_binance_trades(url, publisher, max_events)
    finally:
        publisher.close()

    LOGGER.info(
        "Binance run complete: received=%d queued=%d delivered=%d failed=%d",
        received,
        publisher.stats.queued,
        publisher.stats.delivered,
        publisher.stats.failed,
    )
    if publisher.stats.failed:
        raise RuntimeError(f"Kafka failed to deliver {publisher.stats.failed} events")


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    try:
        asyncio.run(run(args.max_events))
    except KeyboardInterrupt:
        LOGGER.info("Binance producer stopped")


if __name__ == "__main__":
    main()
