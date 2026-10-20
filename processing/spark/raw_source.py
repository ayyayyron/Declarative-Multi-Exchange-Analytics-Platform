"""Adapt Kafka records to the raw-frame DataFrame boundary expected by processing."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.config import Settings

# File/fixture adapters must produce this same shape; transport locations are
# nullable metadata, not business identities.
RAW_INPUT_SCHEMA = StructType(
    [
        StructField("frame_json", StringType()),
        StructField("source_kind", StringType(), False),
        StructField("source_location", StringType(), False),
        StructField("source_key", StringType()),
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def read_raw_frames(spark: SparkSession, settings: Settings) -> DataFrame:
    """Build the raw Kafka streaming source with explicit source-location
    metadata.

    This defines the input plan; processing starts when its streaming query
    starts.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_raw_frames_topic)
        .option("startingOffsets", settings.spark_starting_offsets)
        .option("maxOffsetsPerTrigger", settings.spark_max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("value").cast("string").alias("frame_json"),
            F.lit("kafka").alias("source_kind"),
            F.concat_ws(":", "topic", "partition", "offset").alias("source_location"),
            F.col("key").cast("string").alias("source_key"),
            "topic",
            F.col("partition").cast("long").alias("partition"),
            "offset",
            "timestamp",
        )
    )
