import shutil
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from common.contracts.candle import CandleInterval
from processing.spark.candles import aggregate_candles
from processing.spark.experimental_candles import _update_candle, prototype_candles
from tests.processing.spark.test_candles import spark as spark
from tests.processing.spark.test_candles import trade_rows


def test_prototype_requires_streaming_input(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="streaming DataFrame"):
        prototype_candles(trade_rows(spark))


def test_unsupported_interval_fails_before_building_a_plan(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="Supported candle intervals"):
        prototype_candles(trade_rows(spark), interval="15m")


@pytest.mark.parametrize("interval", ["1m", "5m"])
def test_combined_state_duplicates_lateness_recovery_and_cleanup(
    spark: SparkSession, tmp_path: Path, interval: CandleInterval
) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    trades = trade_rows(spark)
    if interval == "5m":
        trades = trades.withColumn(
            "trade_time",
            F.when(
                F.col("event_id") == "b", F.to_timestamp(F.lit("2026-10-18 10:04:40"))
            )
            .when(
                F.col("event_id") == "c", F.to_timestamp(F.lit("2026-10-18 10:03:20"))
            )
            .when(
                F.col("event_id") == "d", F.to_timestamp(F.lit("2026-10-18 10:05:00"))
            )
            .otherwise(F.col("trade_time")),
        )
    future_time = "10:03:10" if interval == "1m" else "10:07:10"
    further_time = "10:06:10" if interval == "1m" else "10:12:10"
    batches = [
        trades.filter("event_id IN ('a', 'b', 'e', 'f')"),
        trades.filter("event_id IN ('a', 'c', 'd')"),
        trades.filter("event_id IN ('a', 'b')"),
        trades.filter("event_id = 'a'").select(
            F.lit("future").alias("event_id"),
            "source",
            "symbol",
            F.to_timestamp(F.lit(f"2026-10-18 {future_time}")).alias("trade_time"),
            "price",
            "quantity",
        ),
        trades.filter("event_id = 'c'"),
        trades.filter("event_id = 'a'").select(
            F.lit("further").alias("event_id"),
            "source",
            "symbol",
            F.to_timestamp(F.lit(f"2026-10-18 {further_time}")).alias("trade_time"),
            "price",
            "quantity",
        ),
        trades.filter("event_id = 'b'"),
    ]
    for index, batch in enumerate(batches):
        batch.coalesce(1).write.json(str(tmp_path / f"staged-{index}"))
    snapshots: list[Row] = []

    def collect_rows(batch: DataFrame, batch_id: int) -> None:
        rows = batch.collect()
        duration = 60_000 if interval == "1m" else 300_000
        assert all(row.interval == interval for row in rows)
        assert all(row.window_start_ms % duration == 0 for row in rows)
        assert all(row.window_end_ms == row.window_start_ms + duration for row in rows)
        snapshots.extend(rows)

    def start_query():
        raw = spark.readStream.schema(trades.schema).json(str(incoming))
        return (
            prototype_candles(raw, interval=interval)
            .writeStream.outputMode("update")
            .foreachBatch(collect_rows)
            .option("checkpointLocation", str(tmp_path / "checkpoint"))
            .start()
        )

    query = start_query()

    def publish(index: int) -> None:
        source = next((tmp_path / f"staged-{index}").glob("part-*.json"))
        shutil.copyfile(source, incoming / f"batch-{index}.json")
        query.processAllAvailable()

    def target_rows() -> list[Row]:
        return [
            r
            for r in snapshots
            if r.source == "binance"
            and r.symbol == "BTCUSDT"
            and r.window_start_ms == 1792317600000
        ]

    try:
        publish(0)
        assert len(snapshots) == 3
        first = target_rows()[-1]
        assert (first.open, first.close, first.volume, first.trade_count) == (
            Decimal("100"),
            Decimal("102"),
            Decimal("3"),
            2,
        )
        assert not first.is_final
        assert first.interval == interval
        assert first.window_end_ms - first.window_start_ms == (
            60_000 if interval == "1m" else 300_000
        )

        query.stop()
        query = start_query()
        query.processAllAvailable()
        assert len(snapshots) == 3

        publish(1)
        revised = target_rows()[-1]
        expected = (
            aggregate_candles(trades, interval)
            .filter("source = 'binance' AND symbol = 'BTCUSDT'")
            .orderBy("window_start")
            .first()
        )
        for name in ("open", "high", "low", "close", "volume", "trade_count"):
            assert revised[name] == expected[name]
        assert revised.revision == 2

        before_duplicates = len(snapshots)
        publish(2)
        assert len(snapshots) == before_duplicates

        publish(3)
        final = target_rows()[-1]
        assert final.is_final
        assert final.revision == 3
        assert (final.volume, final.trade_count) == (Decimal("6"), 3)
        assert any(
            s["numRowsRemoved"] > 0
            for p in query.recentProgress
            for s in p["stateOperators"]
        )

        count = len(target_rows())
        query.stop()
        query = start_query()
        publish(4)
        assert len(target_rows()) == count
        assert (
            sum(
                s["numRowsDroppedByWatermark"]
                for p in query.recentProgress
                for s in p["stateOperators"]
            )
            >= 1
        )

        publish(5)
        assert sum(s["numRowsTotal"] for s in query.lastProgress["stateOperators"]) == 1
        publish(6)
        assert len(target_rows()) == count
    finally:
        query.stop()


@pytest.mark.parametrize("interval,duration", [("1m", 60_000), ("5m", 300_000)])
def test_same_batch_duplicates_and_equal_time_ordering(
    interval: CandleInterval, duration: int
) -> None:
    import pandas as pd

    state = Mock()
    state.hasTimedOut = False
    state.exists = False
    state.update.side_effect = lambda _: setattr(state, "exists", True)
    state.getCurrentWatermarkMs.return_value = 0
    batch = pd.DataFrame(
        [
            ("b", 1000, Decimal("102"), Decimal("2")),
            ("a", 1000, Decimal("100"), Decimal("1")),
            ("b", 1000, Decimal("102"), Decimal("2")),
            ("c", 1000, Decimal("103"), Decimal("3")),
        ],
        columns=["event_id", "trade_time_ms", "price", "quantity"],
    )
    result = list(
        _update_candle(
            ("binance", "BTCUSDT", 0), iter([batch]), state, interval=interval
        )
    )
    row = result[0].iloc[0]
    assert row["interval"] == interval
    assert row["window_end_ms"] == duration
    state.setTimeoutTimestamp.assert_called_once_with(duration)
    assert (row["open"], row["close"], row["volume"], row["trade_count"]) == (
        Decimal("100"),
        Decimal("103"),
        Decimal("6"),
        3,
    )


def test_conflicting_duplicate_fails_instead_of_silently_choosing() -> None:
    import pandas as pd

    state = Mock()
    state.hasTimedOut = False
    state.exists = False
    state.getCurrentWatermarkMs.return_value = 0
    batch = pd.DataFrame(
        [
            ("a", 1000, Decimal("100"), Decimal("1")),
            ("a", 1000, Decimal("101"), Decimal("1")),
        ],
        columns=["event_id", "trade_time_ms", "price", "quantity"],
    )
    with pytest.raises(ValueError, match="Conflicting duplicate trade"):
        list(_update_candle(("binance", "BTCUSDT", 0), iter([batch]), state))
    state.update.assert_not_called()
