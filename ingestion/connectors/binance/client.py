"""Maintain a Binance trade WebSocket and forward received messages to raw capture."""

import asyncio
import time

from websockets.asyncio.client import connect

from common.logging_config import get_logger
from ingestion.capture import capture_message
from ingestion.kafka_publisher import FramePublisher
from ingestion.runtime import SessionExit, SourceStats, receive_or_stop

LOGGER = get_logger(__name__)


def build_binance_stream_url(base_url: str, stream_name: str) -> str:
    return f"{base_url.rstrip('/')}/{stream_name}"


async def run_binance_session(
    url: str,
    publisher: FramePublisher,
    stats: SourceStats,
    stop_event: asyncio.Event,
    max_frames: int | None = None,
) -> SessionExit:
    """Capture one subscription until shutdown or its frame limit; retries
    belong to the supervisor.
    """
    if max_frames is not None and stats.frames_published >= max_frames:
        return SessionExit.LIMIT_REACHED

    async with connect(url, open_timeout=10, close_timeout=10) as websocket:
        stats.connections += 1
        LOGGER.info("Connected to Binance stream: %s", url)
        while True:
            message = await receive_or_stop(websocket, stop_event)
            if message is None:
                return SessionExit.STOPPED

            await capture_message(
                message,
                source_channel=url,
                received_at_ms=time.time_ns() // 1_000_000,
                publisher=publisher,
                stats=stats,
            )
            if max_frames is not None and stats.frames_published >= max_frames:
                return SessionExit.LIMIT_REACHED
