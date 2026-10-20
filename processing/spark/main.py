"""Start the serving query: raw-frame processing by default, canonical input
explicitly.
"""

import argparse
from dataclasses import dataclass
from functools import partial

from common.config import get_settings
from common.logging_config import configure_logging
from processing.spark.kafka_source import read_trade_stream
from processing.spark.normalization import normalize_raw_frames
from processing.spark.postgres_sink import write_live_batch
from processing.spark.query_runtime import await_query_termination
from processing.spark.raw_sink import QUERY_NAME, write_raw_batch
from processing.spark.raw_source import read_raw_frames
from processing.spark.session import build_spark
from processing.spark.validation import decode_trade_records


@dataclass(frozen=True)
class CliArgs:
    available_now: bool


def parse_args() -> CliArgs:
    parser = argparse.ArgumentParser(
        description="Stream validated Kafka trades into PostgreSQL"
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process all currently available Kafka offsets and then stop",
    )
    namespace = parser.parse_args()
    return CliArgs(available_now=bool(namespace.available_now))


def run(available_now: bool) -> None:
    """Build and start the selected query with its own sink and checkpoint
    history.

    Use available-now for bounded input; otherwise schedule repeated micro-
    batches.
    """
    settings = get_settings()
    spark = build_spark(settings, app_name="live-market-postgres")
    spark.sparkContext.setLogLevel("WARN")
    if settings.serving_mode == "raw":
        decoded = normalize_raw_frames(read_raw_frames(spark, settings))
        query_name = QUERY_NAME
        callback = partial(write_raw_batch, settings=settings)
    else:
        decoded = decode_trade_records(
            read_trade_stream(
                spark,
                settings,
                fail_on_data_loss=settings.live_pipeline_fail_on_data_loss,
            )
        )
        query_name = "live-market-postgres"
        callback = partial(write_live_batch, settings=settings)
    writer = (
        decoded.writeStream.queryName(query_name)
        .foreachBatch(callback)
        .option(
            "checkpointLocation",
            str(settings.checkpoint_base_dir / query_name),
        )
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
