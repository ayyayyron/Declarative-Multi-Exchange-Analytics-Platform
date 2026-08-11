import json
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common.config import get_settings
from common.logging_config import configure_logging, get_logger

KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3"
LOGGER = get_logger(__name__)
SMOKE_SCHEMA = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("value", StringType(), nullable=False),
    ]
)


def build_spark() -> SparkSession:
    settings = get_settings()
    return (
        SparkSession.builder.appName("phase-1-compatibility-smoke")
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config(
            "spark.sql.shuffle.partitions",
            str(settings.spark_shuffle_partitions),
        )
        .config("spark.jars.packages", KAFKA_CONNECTOR)
        .getOrCreate()
    )


def write_input(path: Path, filename: str, rows: list[dict[str, object]]) -> None:
    payload = "\n".join(json.dumps(row) for row in rows) + "\n"
    (path / filename).write_text(payload, encoding="utf-8")


def run_available_batch(
    spark: SparkSession,
    input_path: Path,
    output_path: Path,
    checkpoint_path: Path,
) -> None:
    query = (
        spark.readStream.schema(SMOKE_SCHEMA)
        .json(str(input_path))
        .writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path))
        .trigger(availableNow=True)
        .start(str(output_path))
    )
    query.awaitTermination()


def verify_kafka_connector(spark: SparkSession) -> None:
    settings = get_settings()
    row_count = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_raw_trades_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .count()
    )
    LOGGER.info("Kafka connector read completed: row_count=%d", row_count)


def verify_checkpoint_restart(spark: SparkSession, root: Path) -> None:
    input_path = root / "input"
    output_path = root / "parquet-output"
    checkpoint_path = root / "checkpoint"
    input_path.mkdir(parents=True)

    write_input(
        input_path,
        "batch-1.json",
        [{"event_id": 1, "value": "first"}, {"event_id": 2, "value": "second"}],
    )
    run_available_batch(spark, input_path, output_path, checkpoint_path)

    write_input(
        input_path,
        "batch-2.json",
        [{"event_id": 3, "value": "after-restart"}],
    )
    run_available_batch(spark, input_path, output_path, checkpoint_path)

    observed_ids = {
        row.event_id for row in spark.read.parquet(str(output_path)).collect()
    }
    if observed_ids != {1, 2, 3}:
        raise RuntimeError(
            f"Checkpoint restart produced unexpected IDs: {observed_ids}"
        )
    LOGGER.info("Checkpoint restart verified: event_ids=%s", sorted(observed_ids))


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    smoke_root = settings.local_data_dir / "_phase_1_smoke"
    shutil.rmtree(smoke_root, ignore_errors=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        verify_kafka_connector(spark)
        verify_checkpoint_restart(spark, smoke_root)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
