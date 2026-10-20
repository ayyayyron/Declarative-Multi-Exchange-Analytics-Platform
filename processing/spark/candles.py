"""Event-time OHLCV aggregation shared by bounded and streaming trade inputs."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CANDLE_DURATIONS = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
}


def aggregate_candles(trades: DataFrame, interval: str = "1m") -> DataFrame:
    """Group valid, typed trades into fixed UTC-aligned candles.

    Order open/close by trade time, then event ID for deterministic ties.
    Input must be deduplicated if unique-trade counts and volume are required.
    Streaming callers own the upstream watermark, output mode, and checkpoint;
    configure the Spark session timezone as UTC for consistent presentation.
    """
    if interval not in CANDLE_DURATIONS:
        raise ValueError(f"Unsupported candle interval: {interval!r}")

    event_order = F.struct("trade_time", "event_id")
    return (
        trades.groupBy(
            "source",
            "symbol",
            F.window("trade_time", CANDLE_DURATIONS[interval]).alias("candle"),
        )
        .agg(
            F.min_by("price", event_order).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", event_order).alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            "source",
            "symbol",
            F.lit(interval).alias("interval"),
            F.col("candle.start").alias("window_start"),
            F.col("candle.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
        )
    )
