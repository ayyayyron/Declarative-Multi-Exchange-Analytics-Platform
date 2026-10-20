"""Inspect decoded batches using a separate checkpoint; never update serving tables."""

import argparse
from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame

from common.config import get_settings
from common.logging_config import configure_logging, get_logger
from processing.spark.kafka_source import read_trade_stream
from processing.spark.normalization import normalize_raw_frames
from processing.spark.query_runtime import await_query_termination
from processing.spark.raw_source import read_raw_frames
from processing.spark.session import build_spark
from processing.spark.validation import (
    decode_trade_records,
    invalid_trade_records,
    valid_trade_records,
)

LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class CliArgs:
    available_now: bool


def parse_args() -> CliArgs:
    parser = argparse.ArgumentParser(description="Inspect canonical Kafka trades")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process all currently available Kafka offsets and then stop",
    )
    namespace = parser.parse_args()
    return CliArgs(available_now=bool(namespace.available_now))


def inspect_batch(batch: DataFrame, batch_id: int) -> None:
    """Count and display valid/invalid samples from one finite micro-batch."""
    batch.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        valid = valid_trade_records(batch)
        invalid = invalid_trade_records(batch)
        valid_count = valid.count()
        invalid_count = invalid.count()
        LOGGER.info(
            "spark_batch batch_id=%d valid=%d invalid=%d",
            batch_id,
            valid_count,
            invalid_count,
        )
        if valid_count:
            valid.select(
                "source",
                "symbol",
                "trade_time",
                "price",
                "quantity",
                "trade_side",
                "partition",
                "offset",
            ).show(10, truncate=False)
        if invalid_count:
            invalid.select(
                "validation_error",
                "topic",
                "partition",
                "offset",
                "raw_value",
            ).show(10, truncate=120)
    finally:
        batch.unpersist()


def run(available_now: bool) -> None:
    settings = get_settings()
    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")
    raw = settings.serving_mode == "raw"
    checkpoint = settings.checkpoint_base_dir / (
        "raw-frame-inspection-v1" if raw else "trade-consumer"
    )
    decoded = (
        normalize_raw_frames(read_raw_frames(spark, settings))
        if raw
        else decode_trade_records(read_trade_stream(spark, settings))
    )
    writer = (
        decoded.writeStream.queryName("market-trade-consumer")
        .foreachBatch(inspect_batch)
        .option("checkpointLocation", str(checkpoint))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(
            processingTime=f"{settings.spark_trigger_interval_seconds} seconds"
        )

    query = writer.start()
    try:
        await_query_termination(query)
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    run(args.available_now)


if __name__ == "__main__":
    main()
