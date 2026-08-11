import signal
from typing import cast

from pyspark.sql.streaming.query import StreamingQuery

from streaming.query_runtime import await_query_termination


class FinishedQuery:
    name = "finished-query"
    isActive = True
    stopped = False

    def awaitTermination(self, _: int) -> bool:
        self.isActive = False
        return True

    def stop(self) -> None:
        self.stopped = True


class InterruptedQuery:
    name = "interrupted-query"
    isActive = True
    stopped = False

    def awaitTermination(self, _: int) -> bool:
        signal.raise_signal(signal.SIGINT)
        return False

    def stop(self) -> None:
        self.isActive = False
        self.stopped = True


def test_completed_query_does_not_need_explicit_stop() -> None:
    query = FinishedQuery()

    await_query_termination(cast(StreamingQuery, query))

    assert not query.stopped


def test_interrupt_stops_active_query_cleanly() -> None:
    query = InterruptedQuery()

    await_query_termination(cast(StreamingQuery, query))

    assert query.stopped
