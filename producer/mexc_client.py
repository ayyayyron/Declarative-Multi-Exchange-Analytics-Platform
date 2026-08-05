import asyncio
import json
import time
from typing import Any, Protocol

from pydantic import ValidationError
from websockets.asyncio.client import connect

from common.logging_config import get_logger
from common.models import FailureStage
from producer.adapters.base import SourcePayloadError
from producer.adapters.mexc import parse_mexc_trade_frame
from producer.dead_letter import build_dead_letter
from producer.kafka_publisher import EventPublisher
from producer.runtime import (
    SessionExit,
    SourceStats,
    receive_or_stop,
    wait_for_stop,
)

LOGGER = get_logger(__name__)
MEXC_PING = '{"method":"PING"}'


class Sendable(Protocol):
    async def send(self, message: str | bytes) -> None: ...


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


def process_mexc_frame(
    frame: bytes,
    source_channel: str,
    received_at_ms: int,
    publisher: EventPublisher,
    stats: SourceStats,
) -> None:
    stats.frames_received += 1
    stats.last_received_at_ms = received_at_ms
    try:
        events = parse_mexc_trade_frame(frame, received_at_ms)
    except (SourcePayloadError, ValidationError) as error:
        failure_stage: FailureStage = (
            "source_decode"
            if isinstance(error, SourcePayloadError)
            else "contract_validation"
        )
        publisher.publish_dead_letter(
            build_dead_letter(
                source="mexc",
                source_channel=source_channel,
                payload=frame,
                error=error,
                failure_stage=failure_stage,
                failed_at_ms=received_at_ms,
            )
        )
        stats.events_rejected += 1
        return

    stats.events_decoded += len(events)
    for event in events:
        publisher.publish_trade(event)
        stats.events_published += 1
        stats.last_event_time_ms = event.trade_time_ms


async def send_mexc_heartbeats(
    websocket: Sendable,
    stop_event: asyncio.Event,
    stats: SourceStats,
    interval_seconds: float,
) -> None:
    while not await wait_for_stop(stop_event, interval_seconds):
        await websocket.send(MEXC_PING)
        stats.heartbeats_sent += 1


async def consume_mexc_frames(
    websocket: Any,
    channel: str,
    publisher: EventPublisher,
    stats: SourceStats,
    stop_event: asyncio.Event,
    max_events: int | None,
) -> SessionExit:
    while True:
        message = await receive_or_stop(websocket, stop_event)
        if message is None:
            return SessionExit.STOPPED
        if isinstance(message, str):
            continue

        process_mexc_frame(
            message,
            source_channel=channel,
            received_at_ms=time.time_ns() // 1_000_000,
            publisher=publisher,
            stats=stats,
        )
        if max_events is not None and stats.events_published >= max_events:
            return SessionExit.LIMIT_REACHED


async def run_mexc_session(
    url: str,
    channel: str,
    publisher: EventPublisher,
    stats: SourceStats,
    stop_event: asyncio.Event,
    heartbeat_interval_seconds: float,
    max_events: int | None = None,
) -> SessionExit:
    if max_events is not None and stats.events_published >= max_events:
        return SessionExit.LIMIT_REACHED

    async with connect(url, open_timeout=10, close_timeout=10) as websocket:
        stats.connections += 1
        await websocket.send(build_mexc_subscription(channel))
        acknowledgement = await receive_or_stop(websocket, stop_event)
        if acknowledgement is None:
            return SessionExit.STOPPED
        if not isinstance(acknowledgement, str):
            raise SourcePayloadError("MEXC subscription response must be a text frame")
        validate_mexc_subscription(acknowledgement, channel)
        LOGGER.info("Subscribed to MEXC stream: %s", channel)

        async with asyncio.TaskGroup() as task_group:
            heartbeat = task_group.create_task(
                send_mexc_heartbeats(
                    websocket,
                    stop_event,
                    stats,
                    heartbeat_interval_seconds,
                )
            )
            consumer = task_group.create_task(
                consume_mexc_frames(
                    websocket,
                    channel,
                    publisher,
                    stats,
                    stop_event,
                    max_events,
                )
            )
            result = await consumer
            heartbeat.cancel()

        return result
