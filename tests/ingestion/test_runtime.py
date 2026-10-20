import asyncio

import pytest

from ingestion.runtime import (
    BackoffPolicy,
    SessionExit,
    SourceStats,
    receive_or_stop,
    supervise_source,
)


class BlockingConnection:
    async def recv(self) -> str:
        await asyncio.Event().wait()
        return "unreachable"


@pytest.mark.parametrize(
    ("attempt", "jitter", "expected"),
    [(0, 0.0, 0.5), (0, 1.0, 1.0), (10, 1.0, 8.0)],
)
def test_backoff_is_exponential_jittered_and_capped(
    attempt: int,
    jitter: float,
    expected: float,
) -> None:
    policy = BackoffPolicy(base_seconds=1.0, max_seconds=8.0)

    assert policy.delay(attempt, jitter) == expected


def test_supervisor_reconnects_after_source_failure() -> None:
    async def scenario() -> tuple[int, list[float], SourceStats]:
        calls = 0
        delays: list[float] = []
        stop_event = asyncio.Event()
        stats = SourceStats("binance")

        async def session() -> SessionExit:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("simulated disconnect")
            return SessionExit.LIMIT_REACHED

        async def skip_wait(_: asyncio.Event, delay: float) -> bool:
            delays.append(delay)
            return False

        await supervise_source(
            session,
            stop_event,
            stats,
            BackoffPolicy(1.0, 8.0),
            stop_waiter=skip_wait,
            random_value=lambda: 0.0,
        )
        return calls, delays, stats

    calls, delays, stats = asyncio.run(scenario())

    assert calls == 2
    assert delays == [0.5]
    assert stats.connection_failures == 1
    assert stats.reconnects == 1


def test_failing_source_does_not_cancel_healthy_source() -> None:
    async def scenario() -> tuple[SourceStats, SourceStats]:
        stop_event = asyncio.Event()
        failed = SourceStats("binance")
        healthy = SourceStats("mexc")

        async def failing_session() -> SessionExit:
            raise ConnectionError("simulated Binance failure")

        async def healthy_session() -> SessionExit:
            healthy.frames_published += 1
            stop_event.set()
            return SessionExit.LIMIT_REACHED

        async def yield_to_peer(event: asyncio.Event, _: float) -> bool:
            await asyncio.sleep(0)
            return event.is_set()

        await asyncio.gather(
            supervise_source(
                failing_session,
                stop_event,
                failed,
                BackoffPolicy(1.0, 8.0),
                stop_waiter=yield_to_peer,
                random_value=lambda: 0.0,
            ),
            supervise_source(
                healthy_session,
                stop_event,
                healthy,
                BackoffPolicy(1.0, 8.0),
            ),
        )
        return failed, healthy

    failed, healthy = asyncio.run(scenario())

    assert failed.connection_failures == 1
    assert healthy.frames_published == 1


def test_stop_event_interrupts_blocked_receive() -> None:
    async def scenario() -> str | bytes | None:
        stop_event = asyncio.Event()
        stop_event.set()
        return await receive_or_stop(BlockingConnection(), stop_event)

    assert asyncio.run(scenario()) is None


def test_cancelling_receiver_cleans_up_child_tasks() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        finished = asyncio.Event()

        class Connection:
            async def recv(self) -> str:
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    finished.set()
                raise AssertionError("unreachable")

        receiver = asyncio.create_task(receive_or_stop(Connection(), asyncio.Event()))
        async with asyncio.timeout(1):
            await entered.wait()
            receiver.cancel()
            with pytest.raises(asyncio.CancelledError):
                await receiver
        assert finished.is_set()
        assert asyncio.all_tasks() == {asyncio.current_task()}

    asyncio.run(scenario())
