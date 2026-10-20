"""Legacy reader for canonical raw-trades input; raw capture uses raw_source instead."""

from pyspark.sql import DataFrame, SparkSession

from common.config import Settings


def read_trade_stream(
    spark: SparkSession,
    settings: Settings,
    fail_on_data_loss: bool = True,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_raw_trades_topic)
        .option("startingOffsets", settings.spark_starting_offsets)
        .option("maxOffsetsPerTrigger", settings.spark_max_offsets_per_trigger)
        .option("failOnDataLoss", str(fail_on_data_loss).lower())
        .load()
    )
