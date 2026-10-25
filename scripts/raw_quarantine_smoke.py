import asyncio
import time

from common.config import get_settings
from ingestion.capture import capture_message
from ingestion.kafka_publisher import KafkaPublisher, build_producer
from ingestion.runtime import SourceStats


async def run() -> None:
    settings = get_settings()
    publisher = KafkaPublisher(
        settings.kafka_raw_frames_topic,
        settings.kafka_flush_timeout_seconds,
        build_producer(settings.kafka_bootstrap_servers, "raw-quarantine-smoke"),
        enqueue_timeout_seconds=float(settings.kafka_enqueue_timeout_seconds),
    )
    try:
        await capture_message(
            "not-json",
            "offline-quarantine-smoke",
            time.time_ns() // 1_000_000,
            publisher,
            SourceStats("binance", symbol="BTCUSDT"),
        )
    finally:
        publisher.close()
    print("Captured one invalid market frame. Run live-pipeline-once to quarantine it.")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
