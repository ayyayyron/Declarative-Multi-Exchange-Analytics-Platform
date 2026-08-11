import argparse
from dataclasses import dataclass
from functools import partial

from common.config import get_settings
from common.logging_config import configure_logging
from streaming.consumer import build_spark
from streaming.kafka_trades import decode_trade_records, read_trade_stream
from streaming.postgres_sink import write_live_batch
from streaming.query_runtime import await_query_termination


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
    settings = get_settings()
    spark = build_spark(settings, app_name="live-market-postgres")
    spark.sparkContext.setLogLevel("WARN")
    decoded = decode_trade_records(
        read_trade_stream(
            spark,
            settings,
            fail_on_data_loss=settings.live_pipeline_fail_on_data_loss,
        )
    )
    writer = (
        decoded.writeStream.queryName("live-market-postgres")
        .foreachBatch(partial(write_live_batch, settings=settings))
        .option(
            "checkpointLocation",
            str(settings.checkpoint_base_dir / "live-market-postgres"),
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
