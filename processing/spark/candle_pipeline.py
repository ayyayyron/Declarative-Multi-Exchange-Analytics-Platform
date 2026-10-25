"""Run one candle interval from raw Kafka with interval-specific recovery state."""

import argparse
from functools import partial
from typing import cast

from common.config import get_settings
from common.contracts.candle import CANDLE_MILLISECONDS, CandleInterval
from common.logging_config import configure_logging
from processing.spark.candle_sink import candle_query_name, write_candle_batch
from processing.spark.experimental_candles import prototype_candles
from processing.spark.normalization import normalize_raw_frames
from processing.spark.query_runtime import await_query_termination
from processing.spark.raw_source import read_raw_frames
from processing.spark.session import build_spark
from processing.spark.validation import valid_trade_records


def run(available_now: bool = False, *, interval: CandleInterval = "1m") -> None:
    """Start one interval with its own checkpoint and sink history."""
    if interval not in CANDLE_MILLISECONDS:
        raise ValueError("Supported candle intervals are 1m and 5m")
    settings = get_settings()
    name = candle_query_name(interval)
    spark = build_spark(settings, app_name=name)
    try:
        spark.sparkContext.setLogLevel("WARN")
        raw = read_raw_frames(spark, settings)
        valid = valid_trade_records(normalize_raw_frames(raw))
        candles = prototype_candles(
            valid,
            watermark_delay=f"{settings.candle_watermark_delay_seconds} seconds",
            interval=interval,
        )
        writer = (
            candles.writeStream.queryName(name)
            .outputMode("update")
            .foreachBatch(
                partial(write_candle_batch, settings=settings, interval=interval)
            )
            .option("checkpointLocation", str(settings.checkpoint_base_dir / name))
        )
        if available_now:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(
                processingTime=f"{settings.spark_trigger_interval_seconds} seconds"
            )
        query = writer.start()
        await_query_termination(query)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate raw Kafka trades into PostgreSQL candles"
    )
    parser.add_argument(
        "--interval",
        choices=("1m", "5m"),
        default="1m",
        help="candle interval (default: 1m)",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process input available at startup, then stop without forcing finality",
    )
    args = parser.parse_args()
    configure_logging(get_settings().log_level)
    run(
        available_now=bool(args.available_now),
        interval=cast(CandleInterval, args.interval),
    )


if __name__ == "__main__":
    main()
