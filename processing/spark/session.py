"""Construct the local Spark runtime and resolve its Kafka JVM connector."""

import sys

from pyspark.sql import SparkSession

from common.config import Settings

KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3"


def build_spark(
    settings: Settings,
    app_name: str = "market-trade-consumer",
) -> SparkSession:
    """Create or reuse a UTC SparkSession using the configured resources and
    Python runtime.
    """
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
