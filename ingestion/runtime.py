"""Supervise independent feed sessions and make socket waits interruptible."""

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from common.contracts import Exchange
from common.logging_config import get_logger

LOGGER = get_logger(__name__)


class SessionExit(Enum):
    STOPPED = "stopped"
    LIMIT_REACHED = "limit_reached"


@dataclass
class SourceStats:
    source: Exchange
    symbol: str | None = None
    connections: int = 0
    frames_received: int = 0
    frames_published: int = 0
    connection_failures: int = 0
    reconnects: int = 0
    heartbeats_sent: int = 0
    last_received_at_ms: int | None = None

    @property
    def feed_id(self) -> str:
        return f"{self.source}:{self.symbol}:trade" if self.symbol else self.source


@dataclass(frozen=True)
class BackoffPolicy:
    base_seconds: float
    max_seconds: float

    def __post_init__(self) -> None:
        if self.base_seconds <= 0 or self.max_seconds < self.base_seconds:
            raise ValueError("Reconnect backoff requires 0 < base <= maximum")

    def delay(self, attempt: int, jitter: float) -> float:
        exponent = float(2 ** min(attempt, 30))
        capped: float = min(
            self.max_seconds,
            self.base_seconds * exponent,
        )
        bounded_jitter = min(max(jitter, 0.0), 1.0)
        return float(capped * (0.5 + 0.5 * bounded_jitter))


class Receivable(Protocol):
    async def recv(self) -> str | bytes: ...


SessionRunner = Callable[[], Awaitable[SessionExit]]
StopWaiter = Callable[[asyncio.Event, float], Awaitable[bool]]


async def wait_for_stop(stop_event: asyncio.Event, timeout: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout)
    except TimeoutError:
        return False
    return True


async def receive_or_stop(
    connection: Receivable,
    stop_event: asyncio.Event,
) -> str | bytes | None:
    """Wait for a frame or shutdown, cancelling and awaiting the unused child tasks."""
    receive_task = asyncio.create_task(connection.recv())
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        done, _ = await asyncio.wait(
            {receive_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            return None
        return receive_task.result()
    finally:
        for task in (receive_task, stop_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(receive_task, stop_task, return_exceptions=True)


async def supervise_source(
    session: SessionRunner,
    stop_event: asyncio.Event,
    stats: SourceStats,
    backoff: BackoffPolicy,
    stop_waiter: StopWaiter = wait_for_stop,
    random_value: Callable[[], float] = random.random,
) -> None:
    """Retry a failed session with bounded jittered backoff until stopped.

    Counters survive reconnection; this does not backfill missed exchange
    data.
    """
    attempt = 0
    while not stop_event.is_set():
        published_before = stats.frames_published
        try:
            await session()
            return
        except asyncio.CancelledError:
            raise
        except Exception as error:
            stats.connection_failures += 1
            if stop_event.is_set():
                return
            if stats.frames_published > published_before:
                attempt = 0
            delay = backoff.delay(attempt, random_value())
            stats.reconnects += 1
            LOGGER.warning(
                "source_reconnect source=%s symbol=%s feed_id=%s "
                "attempt=%d delay_seconds=%.3f "
                "error_type=%s error=%s",
                stats.source,
                stats.symbol,
                stats.feed_id,
                stats.reconnects,
                delay,
                type(error).__name__,
                str(error)[:500],
            )
            attempt += 1
            if await stop_waiter(stop_event, delay):
                return
