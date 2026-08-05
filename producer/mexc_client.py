import json
import time
from typing import Any

from websockets.asyncio.client import connect

from common.logging_config import get_logger
from producer.adapters.base import SourcePayloadError
from producer.adapters.mexc import parse_mexc_trade_frame
from producer.kafka_publisher import KafkaTradePublisher

LOGGER = get_logger(__name__)


def build_mexc_subscription(channel: str) -> str:
    return json.dumps(
        {"method": "SUBSCRIPTION", "params": [channel]},
        separators=(",", ":"),
    )


def validate_mexc_subscription(raw_message: str, channel: str) -> None:
    try:
        response: Any = json.loads(raw_message)
    except json.JSONDecodeError as error:
        raise SourcePayloadError("MEXC subscription response is not JSON") from error

    if not isinstance(response, dict):
        raise SourcePayloadError("MEXC subscription response must be an object")
    if response.get("code") != 0 or response.get("msg") != channel:
        raise SourcePayloadError(f"MEXC subscription failed: {raw_message}")


async def stream_mexc_trades(
    url: str,
    channel: str,
    publisher: KafkaTradePublisher,
    max_events: int | None = None,
) -> int:
    event_count = 0
    async with connect(url, open_timeout=10, close_timeout=10) as websocket:
        await websocket.send(build_mexc_subscription(channel))
        acknowledgement = await websocket.recv()
        if not isinstance(acknowledgement, str):
            raise SourcePayloadError("MEXC subscription response must be a text frame")
        validate_mexc_subscription(acknowledgement, channel)
        LOGGER.info("Subscribed to MEXC stream: %s", channel)

        async for message in websocket:
            if isinstance(message, str):
                continue

            events = parse_mexc_trade_frame(
                message,
                received_at_ms=time.time_ns() // 1_000_000,
            )
            for event in events:
                publisher.publish(event)
            event_count += len(events)

            if max_events is not None and event_count >= max_events:
                break

    return event_count
