import argparse
import sys
from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from common.config import Settings, get_settings
from common.logging_config import configure_logging, get_logger
from streaming.kafka_trades import (
    decode_trade_records,
    invalid_trade_records,
    read_trade_stream,
    valid_trade_records,
)
from streaming.query_runtime import await_query_termination

KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3"
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


def build_spark(
    settings: Settings,
    app_name: str = "market-trade-consumer",
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            str(settings.spark_shuffle_partitions),
        )
        .config("spark.jars.packages", KAFKA_CONNECTOR)
        .getOrCreate()
    )


def inspect_batch(batch: DataFrame, batch_id: int) -> None:
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
    checkpoint = settings.checkpoint_base_dir / "trade-consumer"
    decoded = decode_trade_records(read_trade_stream(spark, settings))
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
