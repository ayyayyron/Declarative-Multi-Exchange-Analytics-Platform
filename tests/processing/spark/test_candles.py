import shutil
import sys
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from processing.spark.candles import aggregate_candles
from processing.spark.deduplication import deduplicate_trades


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("candle-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    yield session
    session.stop()


def trade_rows(spark: SparkSession) -> DataFrame:
    return (
        spark.createDataFrame(
            [
                ("b", "binance", "BTCUSDT", "2026-10-18 10:00:40", "102", "2"),
                ("a", "binance", "BTCUSDT", "2026-10-18 10:00:10", "100", "1"),
                ("c", "binance", "BTCUSDT", "2026-10-18 10:00:20", "99", "3"),
                ("d", "binance", "BTCUSDT", "2026-10-18 10:01:00", "103", "1"),
                ("e", "mexc", "BTCUSDT", "2026-10-18 10:00:10", "200", "1"),
                ("f", "binance", "ETHUSDT", "2026-10-18 10:00:10", "50", "1"),
            ],
            "event_id string, source string, symbol string, trade_time string, "
            "price string, quantity string",
        )
        .withColumn("trade_time", F.to_timestamp("trade_time"))
        .withColumn("price", F.col("price").cast("decimal(28,10)"))
        .withColumn("quantity", F.col("quantity").cast("decimal(28,10)"))
    )


@pytest.mark.parametrize(
    "interval,expected_counts",
    [
        ("1m", [3, 1]),
        ("5m", [4]),
        ("15m", [4]),
        ("1h", [4]),
    ],
)
def test_intervals_boundaries_and_market_isolation(
    spark: SparkSession, interval: str, expected_counts: list[int]
) -> None:
    result = aggregate_candles(trade_rows(spark), interval)
    rows = (
        result.filter("source = 'binance' AND symbol = 'BTCUSDT'")
        .orderBy("window_start")
        .collect()
    )
    assert [r.trade_count for r in rows] == expected_counts
    assert rows[0].open == Decimal("100")
    assert rows[0].low == Decimal("99")
    assert rows[0].close == Decimal("102" if interval == "1m" else "103")
    assert rows[0].high == Decimal("102" if interval == "1m" else "103")
    assert rows[0].volume == Decimal("6" if interval == "1m" else "7")
    assert rows[0].interval == interval
    duration = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}[interval]
    assert (rows[0].window_end - rows[0].window_start).total_seconds() == duration
    assert result.filter("source = 'mexc'").first().trade_count == 1
    assert result.filter("symbol = 'ETHUSDT'").first().trade_count == 1


def test_equal_timestamps_use_event_id_not_input_order(spark: SparkSession) -> None:
    trades = (
        trade_rows(spark)
        .filter("event_id IN ('a', 'b')")
        .withColumn("trade_time", F.to_timestamp(F.lit("2026-10-18 10:00:10")))
    )
    for direction in (F.asc("event_id"), F.desc("event_id")):
        row = aggregate_candles(trades.orderBy(direction).repartition(2)).first()
        assert row.open == Decimal("100")
        assert row.close == Decimal("102")


def test_unsupported_interval_fails_before_execution(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="Unsupported candle interval"):
        aggregate_candles(trade_rows(spark), "2m")


def test_batch_duplicates_do_not_inflate_candles(spark: SparkSession) -> None:
    trades = trade_rows(spark)
    duplicated = trades.unionByName(trades)
    result = (
        aggregate_candles(deduplicate_trades(duplicated))
        .filter("source = 'binance' AND symbol = 'BTCUSDT'")
        .orderBy("window_start")
        .collect()
    )
    assert [row.trade_count for row in result] == [3, 1]
    assert [row.volume for row in result] == [Decimal("6"), Decimal("1")]


def test_streaming_deduplication_across_batches_and_restart(
    spark: SparkSession, tmp_path: Path
) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    trades = trade_rows(spark)
    for index, ids in enumerate((["a", "b"], ["a", "d"])):
        trades.filter(F.col("event_id").isin(ids)).coalesce(1).write.json(
            str(tmp_path / f"staged-{index}")
        )
    received: list[Row] = []

    def collect_rows(batch: DataFrame, batch_id: int) -> None:
        received.extend(batch.collect())

    def start_query():
        raw = spark.readStream.schema(trades.schema).json(str(incoming))
        return (
            deduplicate_trades(raw)
            .writeStream.outputMode("append")
            .foreachBatch(collect_rows)
            .option("checkpointLocation", str(tmp_path / "checkpoint"))
            .start()
        )

    query = start_query()
    try:
        source = next((tmp_path / "staged-0").glob("part-*.json"))
        shutil.copyfile(source, incoming / "batch-0.json")
        query.processAllAvailable()
        assert {row.event_id for row in received} == {"a", "b"}
        assert len(received) == 2

        query.stop()
        query = start_query()
        source = next((tmp_path / "staged-1").glob("part-*.json"))
        shutil.copyfile(source, incoming / "batch-1.json")
        query.processAllAvailable()
        assert {row.event_id for row in received} == {"a", "b", "d"}
        assert len(received) == 3
        assert sum(row.quantity for row in received) == Decimal("4")
    finally:
        query.stop()


def test_streaming_revisions_recovery_and_watermark(
    spark: SparkSession, tmp_path: Path
) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    checkpoint = tmp_path / "checkpoint"
    updates: list[Row] = []
    batches = [
        [("a", "10:00:10", "100", "1"), ("b", "10:00:40", "102", "2")],
        [("c", "10:00:20", "99", "3"), ("d", "10:01:10", "103", "1")],
        [("e", "10:03:10", "105", "1")],
        [("f", "10:00:30", "200", "10"), ("g", "10:03:20", "106", "1")],
    ]
    for index, batch in enumerate(batches):
        staged = spark.createDataFrame(
            [
                (event_id, "binance", "BTCUSDT", f"2026-10-18 {time}", price, qty)
                for event_id, time, price, qty in batch
            ],
            "event_id string, source string, symbol string, trade_time string, "
            "price string, quantity string",
        )
        (
            staged.withColumn("trade_time", F.to_timestamp("trade_time"))
            .withColumn("price", F.col("price").cast("decimal(28,10)"))
            .withColumn("quantity", F.col("quantity").cast("decimal(28,10)"))
            .coalesce(1)
            .write.json(str(tmp_path / f"staged-{index}"))
        )

    def collect_updates(batch: DataFrame, batch_id: int) -> None:
        updates.extend(
            batch.withColumn("minute", F.date_format("window_start", "HH:mm"))
            .orderBy("window_start")
            .collect()
        )

    def start_query():
        trades = spark.readStream.schema(trade_rows(spark).schema).json(str(incoming))
        return (
            aggregate_candles(trades.withWatermark("trade_time", "2 minutes"))
            .writeStream.outputMode("update")
            .foreachBatch(collect_updates)
            .option("checkpointLocation", str(checkpoint))
            .start()
        )

    def publish(index: int) -> None:
        source = next((tmp_path / f"staged-{index}").glob("part-*.json"))
        shutil.copyfile(source, incoming / f"batch-{index}.json")
        query.processAllAvailable()

    query = start_query()
    try:
        publish(0)
        first = [row for row in updates if row.minute == "10:00"][-1]
        assert (first.open, first.close, first.volume, first.trade_count) == (
            Decimal("100"),
            Decimal("102"),
            Decimal("3"),
            2,
        )

        query.stop()
        query = start_query()
        query.processAllAvailable()
        assert len(updates) == 1

        publish(1)
        revised = [row for row in updates if row.minute == "10:00"][-1]
        assert (revised.open, revised.low, revised.close) == (
            Decimal("100"),
            Decimal("99"),
            Decimal("102"),
        )
        assert (revised.volume, revised.trade_count) == (Decimal("6"), 3)

        publish(2)
        assert query.lastProgress["eventTime"]["watermark"] == (
            "2026-10-18T10:01:10.000Z"
        )
        assert (
            sum(
                state["numRowsRemoved"]
                for progress in query.recentProgress
                for state in progress["stateOperators"]
            )
            >= 1
        )

        previous_updates = len([row for row in updates if row.minute == "10:00"])
        publish(3)
        assert (
            len([row for row in updates if row.minute == "10:00"]) == previous_updates
        )
        assert (
            sum(
                state["numRowsDroppedByWatermark"]
                for progress in query.recentProgress
                for state in progress["stateOperators"]
            )
            >= 1
        )
        current = [row for row in updates if row.minute == "10:03"][-1]
        assert (current.close, current.volume, current.trade_count) == (
            Decimal("106"),
            Decimal("2"),
            2,
        )
    finally:
        query.stop()
