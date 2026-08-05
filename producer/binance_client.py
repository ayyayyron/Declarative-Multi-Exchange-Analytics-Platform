import time

from websockets.asyncio.client import connect

from common.logging_config import get_logger
from producer.adapters.base import SourcePayloadError
from producer.adapters.binance import parse_binance_trade
from producer.kafka_publisher import KafkaTradePublisher

LOGGER = get_logger(__name__)


def build_binance_stream_url(base_url: str, stream_name: str) -> str:
    return f"{base_url.rstrip('/')}/{stream_name}"


async def stream_binance_trades(
    url: str,
    publisher: KafkaTradePublisher,
    max_events: int | None = None,
) -> int:
    event_count = 0
    async with connect(url, open_timeout=10, close_timeout=10) as websocket:
        LOGGER.info("Connected to Binance stream: %s", url)
        async for message in websocket:
            if not isinstance(message, str):
                raise SourcePayloadError("Binance payload must be a text frame")

            event = parse_binance_trade(
                message,
                received_at_ms=time.time_ns() // 1_000_000,
            )
            publisher.publish(event)
            event_count += 1

            if max_events is not None and event_count >= max_events:
                break

    return event_count
