import time

from common.config import get_settings
from common.logging_config import configure_logging, get_logger
from producer.binance_client import process_binance_message
from producer.kafka_publisher import KafkaPublisher, build_producer
from producer.runtime import SourceStats

LOGGER = get_logger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    producer = build_producer(
        str(settings.kafka_bootstrap_servers),
        settings.kafka_producer_client_id,
    )
    publisher = KafkaPublisher(
        trade_topic=settings.kafka_raw_trades_topic,
        dead_letter_topic=settings.kafka_dlq_topic,
        flush_timeout_seconds=settings.kafka_flush_timeout_seconds,
        producer=producer,
    )
    stats = SourceStats("binance")

    process_binance_message(
        "not-json",
        source_channel="offline-dlq-smoke",
        received_at_ms=time.time_ns() // 1_000_000,
        publisher=publisher,
        stats=stats,
    )
    publisher.close()

    if stats.events_rejected != 1 or publisher.stats.dead_letters_queued != 1:
        raise RuntimeError("DLQ smoke did not reject and queue exactly one event")
    if publisher.stats.failed:
        raise RuntimeError("Kafka failed to deliver the DLQ smoke event")
    LOGGER.info("DLQ smoke delivered one rejected Binance fixture")


if __name__ == "__main__":
    main()
