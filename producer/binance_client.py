import asyncio
import time

from pydantic import ValidationError
from websockets.asyncio.client import connect

from common.logging_config import get_logger
from common.models import FailureStage
from producer.adapters.base import SourcePayloadError
from producer.adapters.binance import parse_binance_trade
from producer.dead_letter import build_dead_letter
from producer.kafka_publisher import EventPublisher
from producer.runtime import SessionExit, SourceStats, receive_or_stop

LOGGER = get_logger(__name__)


def build_binance_stream_url(base_url: str, stream_name: str) -> str:
    return f"{base_url.rstrip('/')}/{stream_name}"


def process_binance_message(
    message: str | bytes,
    source_channel: str,
    received_at_ms: int,
    publisher: EventPublisher,
    stats: SourceStats,
) -> None:
    stats.frames_received += 1
    stats.last_received_at_ms = received_at_ms
    try:
        if not isinstance(message, str):
            raise SourcePayloadError("Binance payload must be a text frame")
        event = parse_binance_trade(message, received_at_ms)
    except (SourcePayloadError, ValidationError) as error:
        failure_stage: FailureStage = (
            "source_decode"
            if isinstance(error, SourcePayloadError)
            else "contract_validation"
        )
        publisher.publish_dead_letter(
            build_dead_letter(
                source="binance",
                source_channel=source_channel,
                payload=message,
                error=error,
                failure_stage=failure_stage,
                failed_at_ms=received_at_ms,
            )
        )
        stats.events_rejected += 1
        return

    stats.events_decoded += 1
    publisher.publish_trade(event)
    stats.events_published += 1
    stats.last_event_time_ms = event.trade_time_ms


async def run_binance_session(
    url: str,
    publisher: EventPublisher,
    stats: SourceStats,
    stop_event: asyncio.Event,
    max_events: int | None = None,
) -> SessionExit:
    if max_events is not None and stats.events_published >= max_events:
        return SessionExit.LIMIT_REACHED

    async with connect(url, open_timeout=10, close_timeout=10) as websocket:
        stats.connections += 1
        LOGGER.info("Connected to Binance stream: %s", url)
        while True:
            message = await receive_or_stop(websocket, stop_event)
            if message is None:
                return SessionExit.STOPPED

            process_binance_message(
                message,
                source_channel=url,
                received_at_ms=time.time_ns() // 1_000_000,
                publisher=publisher,
                stats=stats,
            )
            if max_events is not None and stats.events_published >= max_events:
                return SessionExit.LIMIT_REACHED
