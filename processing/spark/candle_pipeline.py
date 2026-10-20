"""Run opt-in one-minute candles from raw Kafka with separate recovery state."""

import argparse
from functools import partial

from common.config import get_settings
from common.logging_config import configure_logging
from processing.spark.candle_sink import candle_query_name, write_candle_batch
from processing.spark.experimental_candles import prototype_candles
from processing.spark.normalization import normalize_raw_frames
from processing.spark.query_runtime import await_query_termination
from processing.spark.raw_source import read_raw_frames
from processing.spark.session import build_spark
from processing.spark.validation import valid_trade_records


def run(available_now: bool = False) -> None:
    """Start a candle-only query; never reuse the live-trade checkpoint or sink."""
    settings = get_settings()
    name = candle_query_name("1m")
    spark = build_spark(settings, app_name=name)
    try:
        spark.sparkContext.setLogLevel("WARN")
        raw = read_raw_frames(spark, settings)
        valid = valid_trade_records(normalize_raw_frames(raw))
        candles = prototype_candles(
            valid, watermark_delay=f"{settings.candle_watermark_delay_seconds} seconds"
        )
        writer = (
            candles.writeStream.queryName(name)
            .outputMode("update")
            .foreachBatch(partial(write_candle_batch, settings=settings, interval="1m"))
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
        description="Aggregate raw Kafka trades into one-minute PostgreSQL candles"
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process input available at startup, then stop without forcing finality",
    )
    args = parser.parse_args()
    configure_logging(get_settings().log_level)
    run(available_now=bool(args.available_now))


if __name__ == "__main__":
    main()
