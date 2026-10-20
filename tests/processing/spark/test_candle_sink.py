import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from pyspark.sql import Row, SparkSession
from pyspark.sql.utils import StreamingQueryException

from common.config import Settings
from processing.spark.candle_sink import (
    CandleBatchResult,
    candle_query_name,
    persist_candle_batch,
    write_candle_batch,
)
from processing.spark.experimental_candles import prototype_candles
from tests.contracts.test_candle import values
from tests.processing.spark.test_candles import spark as spark
from tests.processing.spark.test_candles import trade_rows

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 for an isolated PostgreSQL database",
)


@pytest.fixture(scope="module")
def database() -> Iterator[str]:
    original = str(Settings().postgres_dsn)
    name = "candle_sink_test_" + uuid4().hex
    with psycopg.connect(original, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    isolated = urlunsplit(urlsplit(original)._replace(path=f"/{name}"))
    try:
        with psycopg.connect(isolated) as connection:
            for path in sorted(Path("migrations").glob("*.sql")):
                connection.execute(path.read_text())
        yield isolated
    finally:
        with psycopg.connect(original, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
def dsn(database: str) -> str:
    with psycopg.connect(database) as connection:
        connection.execute("TRUNCATE raw_market.candle_query_history CASCADE")
    return database


def snapshot(**changes: object) -> Row:
    return Row(**(values() | changes))


def stored(dsn: str) -> tuple[object, ...]:
    with psycopg.connect(dsn) as connection:
        result = connection.execute(
            "SELECT revision,volume,trade_count,is_final,last_batch_id "
            "FROM raw_market.candles WHERE source='binance' AND symbol='BTCUSDT' "
            "AND interval='1m' AND window_start_ms=1792317600000"
        ).fetchone()
    assert result is not None
    return result


def test_insert_replacement_retry_and_stale_revision(dsn: str) -> None:
    history = str(uuid4())
    assert persist_candle_batch(dsn, history, 0, [snapshot()]) == CandleBatchResult(
        1, 1, 0, 0
    )
    update = snapshot(revision=2, volume=Decimal("0.2"), trade_count=4)
    assert persist_candle_batch(dsn, history, 1, [update]) == CandleBatchResult(
        1, 1, 0, 0
    )
    assert stored(dsn) == (2, Decimal("0.2"), 4, False, 1)

    def must_not_read() -> Iterator[Row]:
        raise AssertionError("Committed retries must not consume rows")
        yield snapshot()

    assert persist_candle_batch(dsn, history, 1, must_not_read()) is None
    assert persist_candle_batch(dsn, history, 2, [update]) == CandleBatchResult(
        1, 0, 1, 0
    )
    assert persist_candle_batch(dsn, history, 3, [snapshot()]) == CandleBatchResult(
        1, 0, 0, 1
    )
    assert stored(dsn) == (2, Decimal("0.2"), 4, False, 1)
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM raw_market.candle_batch_runs"
        ).fetchone() == (4,)
        assert connection.execute(
            "SELECT count(*) FROM raw_market.live_trades"
        ).fetchone() == (0,)


def test_conflicting_revision_rolls_back_other_candles_and_marker(dsn: str) -> None:
    history = str(uuid4())
    persist_candle_batch(dsn, history, 0, [snapshot()])
    with pytest.raises(ValueError, match="same revision"):
        persist_candle_batch(
            dsn,
            history,
            1,
            [
                snapshot(symbol="ETHUSDT"),
                snapshot(close=Decimal("100")),
            ],
        )
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM raw_market.candles"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM raw_market.candle_batch_runs"
        ).fetchone() == (1,)
    assert persist_candle_batch(dsn, history, 1, [snapshot()]) == CandleBatchResult(
        1, 0, 1, 0
    )


def test_failure_after_writes_rolls_back_registration_and_output(dsn: str) -> None:
    history = str(uuid4())

    def interrupted() -> Iterator[Row]:
        yield snapshot()
        raise RuntimeError("interrupted after first snapshot")

    with pytest.raises(RuntimeError, match="interrupted"):
        persist_candle_batch(dsn, history, 0, interrupted())
    with psycopg.connect(dsn) as connection:
        for table in ("candle_query_history", "candles", "candle_batch_runs"):
            assert connection.execute(
                sql.SQL("SELECT count(*) FROM raw_market.{}").format(
                    sql.Identifier(table)
                )
            ).fetchone() == (0,)
    assert persist_candle_batch(dsn, history, 0, [snapshot()]) == CandleBatchResult(
        1, 1, 0, 0
    )


def test_finalization_is_idempotent_and_cannot_reopen(dsn: str) -> None:
    history = str(uuid4())
    persist_candle_batch(dsn, history, 0, [snapshot()])
    final = snapshot(revision=2, is_final=True)
    persist_candle_batch(dsn, history, 1, [final])
    assert persist_candle_batch(dsn, history, 2, [final]) == CandleBatchResult(
        1, 0, 1, 0
    )
    for is_final in (False, True):
        with pytest.raises(ValueError, match="Finalized"):
            persist_candle_batch(
                dsn, history, 3, [snapshot(revision=3, is_final=is_final)]
            )
    assert stored(dsn) == (2, Decimal("0.1234567890"), 3, True, 1)


def test_history_reset_or_wrong_owner_is_rejected(dsn: str) -> None:
    history = str(uuid4())
    persist_candle_batch(dsn, history, 0, [snapshot()])
    with pytest.raises(RuntimeError, match="history differs"):
        persist_candle_batch(dsn, str(uuid4()), 0, [snapshot(revision=2)])
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "UPDATE raw_market.candle_query_history SET query_name='another-writer'"
        )
    with pytest.raises(RuntimeError, match="history differs"):
        persist_candle_batch(dsn, history, 1, [snapshot(revision=2)])


@pytest.mark.parametrize(
    "change",
    [
        {"volume": Decimal("0.01")},
        {"trade_count": 2},
        {"high": Decimal("101")},
    ],
)
def test_new_revision_cannot_regress_cumulative_values(
    dsn: str, change: dict[str, object]
) -> None:
    history = str(uuid4())
    persist_candle_batch(dsn, history, 0, [snapshot()])
    with pytest.raises(ValueError, match="regresses"):
        persist_candle_batch(dsn, history, 1, [snapshot(revision=2, **change)])
    assert stored(dsn)[0] == 1


def test_intervals_have_independent_owners_and_wrong_interval_rolls_back(
    dsn: str,
) -> None:
    first, second = str(uuid4()), str(uuid4())
    persist_candle_batch(dsn, first, 0, [snapshot()])
    five = snapshot(interval="5m", window_end_ms=1792317900000)
    with pytest.raises(ValueError, match="interval differs"):
        persist_candle_batch(dsn, first, 1, [five])
    assert persist_candle_batch(
        dsn, second, 0, [five], interval="5m"
    ) == CandleBatchResult(1, 1, 0, 0)


def test_concurrent_retries_commit_only_once(dsn: str) -> None:
    history = str(uuid4())
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(persist_candle_batch, dsn, history, 0, [snapshot()])
            for _ in range(2)
        ]
        results = [future.result(timeout=15) for future in futures]
    assert results.count(None) == 1
    assert results.count(CandleBatchResult(1, 1, 0, 0)) == 1


def test_empty_batch_has_a_retry_marker(dsn: str) -> None:
    history = str(uuid4())
    assert persist_candle_batch(dsn, history, 0, []) == CandleBatchResult(0, 0, 0, 0)
    assert persist_candle_batch(dsn, history, 0, []) is None


def test_spark_commit_before_checkpoint_recovery(
    dsn: str, spark: SparkSession, tmp_path: Path
) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    trades = trade_rows(spark)
    trades.filter("event_id IN ('a','b')").coalesce(1).write.mode("append").json(
        str(incoming)
    )
    settings = Settings(postgres_dsn=dsn, checkpoint_base_dir=tmp_path / "checkpoints")
    checkpoint = settings.checkpoint_base_dir / candle_query_name("1m")

    def commit_then_fail(batch, batch_id):
        write_candle_batch(batch, batch_id, settings)
        raise RuntimeError("simulated failure after database commit")

    def start_query(callback):
        stream = spark.readStream.schema(trades.schema).json(str(incoming))
        return (
            prototype_candles(stream)
            .writeStream.outputMode("update")
            .foreachBatch(callback)
            .option("checkpointLocation", str(checkpoint))
            .start()
        )

    query = start_query(commit_then_fail)
    try:
        with pytest.raises(StreamingQueryException, match="after database commit"):
            query.processAllAvailable()
    finally:
        query.stop()
    assert stored(dsn)[:4] == (1, Decimal("3"), 2, False)

    query = start_query(
        lambda batch, batch_id: write_candle_batch(batch, batch_id, settings)
    )
    try:
        query.processAllAvailable()
        assert stored(dsn)[:4] == (1, Decimal("3"), 2, False)
        trades.filter("event_id IN ('a','c','d')").coalesce(1).write.mode(
            "append"
        ).json(str(incoming))
        query.processAllAvailable()
        assert stored(dsn)[:4] == (2, Decimal("6"), 3, False)
    finally:
        query.stop()
