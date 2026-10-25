import signal
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.utils import StreamingQueryException

from processing.spark.query_runtime import await_query_termination
from tests.processing.spark.test_candles import spark as spark


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


@pytest.mark.parametrize("initially_active", [False, True])
def test_terminal_failure_is_propagated_and_signal_handlers_restored(
    initially_active: bool,
) -> None:
    query = MagicMock()
    query.isActive = initially_active
    error = RuntimeError("query failed")

    def await_termination(_: int) -> None:
        if query.isActive:
            query.isActive = False
            return
        raise error

    query.awaitTermination.side_effect = await_termination
    before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    with pytest.raises(RuntimeError, match="query failed"):
        await_query_termination(query)
    assert all(signal.getsignal(sig) == previous for sig, previous in before.items())
    query.stop.assert_not_called()


def test_already_completed_query_is_checked_without_stopping() -> None:
    query = MagicMock()
    query.isActive = False
    await_query_termination(query)
    query.awaitTermination.assert_called_once_with(1)
    query.stop.assert_not_called()


def test_real_spark_failure_before_monitor_is_not_reported_as_success(
    spark: SparkSession, tmp_path: Path
) -> None:
    def fail_batch(batch: object, batch_id: int) -> None:
        raise RuntimeError("intentional monitor failure")

    query = (
        spark.readStream.format("rate")
        .load()
        .writeStream.foreachBatch(fail_batch)
        .option("checkpointLocation", str(tmp_path / "failed-query"))
        .start()
    )
    try:
        with pytest.raises(StreamingQueryException):
            query.awaitTermination(30)
        assert not query.isActive
        with pytest.raises(StreamingQueryException):
            await_query_termination(query)
    finally:
        query.stop()
