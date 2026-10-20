"""Manage MEXC subscriptions and heartbeats; preserve market payloads without
decoding.
"""

import asyncio
import json
import time
from typing import Any, Protocol

from websockets.asyncio.client import connect

from common.logging_config import get_logger
from ingestion.capture import capture_message
from ingestion.connectors.base import SubscriptionError
from ingestion.kafka_publisher import FramePublisher
from ingestion.runtime import (
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
        raise SubscriptionError("MEXC subscription response is not JSON") from error

    if not isinstance(response, dict):
        raise SubscriptionError("MEXC subscription response must be an object")
    if response.get("code") != 0 or response.get("msg") != channel:
        raise SubscriptionError(f"MEXC subscription failed: {raw_message}")


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
    publisher: FramePublisher,
    stats: SourceStats,
    stop_event: asyncio.Event,
    max_frames: int | None,
) -> SessionExit:
    """Skip known control responses and capture other frames until stopped or
    limited.
    """
    while True:
        message = await receive_or_stop(websocket, stop_event)
        if message is None:
            return SessionExit.STOPPED
        if isinstance(message, str):
            try:
                control = json.loads(message)
            except json.JSONDecodeError:
                control = None
            if (
                isinstance(control, dict)
                and control.get("code") == 0
                and control.get("msg") in ("PONG", channel)
            ):
                continue

        capture_message(
            message,
            source_channel=channel,
            received_at_ms=time.time_ns() // 1_000_000,
            publisher=publisher,
            stats=stats,
        )
        if max_frames is not None and stats.frames_published >= max_frames:
            return SessionExit.LIMIT_REACHED


async def run_mexc_session(
    url: str,
    channel: str,
    publisher: FramePublisher,
    stats: SourceStats,
    stop_event: asyncio.Event,
    heartbeat_interval_seconds: float,
    max_frames: int | None = None,
) -> SessionExit:
    """Subscribe, validate the acknowledgement, and run receipt alongside heartbeats."""
    if max_frames is not None and stats.frames_published >= max_frames:
        return SessionExit.LIMIT_REACHED

    async with connect(url, open_timeout=10, close_timeout=10) as websocket:
        stats.connections += 1
        await websocket.send(build_mexc_subscription(channel))
        acknowledgement = await receive_or_stop(websocket, stop_event)
        if acknowledgement is None:
            return SessionExit.STOPPED
        if not isinstance(acknowledgement, str):
            raise SubscriptionError("MEXC subscription response must be a text frame")
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
                    max_frames,
                )
            )
            result = await consumer
            heartbeat.cancel()

        return result
