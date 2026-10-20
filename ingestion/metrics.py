import asyncio
from collections.abc import Mapping

from common.logging_config import get_logger
from ingestion.kafka_publisher import KafkaPublisher
from ingestion.runtime import SourceStats, wait_for_stop

LOGGER = get_logger(__name__)


def log_metrics(
    source_stats: Mapping[str, SourceStats],
    publisher: KafkaPublisher,
) -> None:
    for source in sorted(source_stats):
        stats = source_stats[source]
        LOGGER.info(
            "source_metrics source=%s symbol=%s feed_id=%s "
            "connections=%d frames_received=%d "
            "frames_published=%d "
            "connection_failures=%d reconnects=%d heartbeats_sent=%d "
            "last_received_at_ms=%s",
            stats.source,
            stats.symbol,
            stats.feed_id,
            stats.connections,
            stats.frames_received,
            stats.frames_published,
            stats.connection_failures,
            stats.reconnects,
            stats.heartbeats_sent,
            stats.last_received_at_ms,
        )

    kafka = publisher.stats
    LOGGER.info(
        "kafka_metrics queued=%d frames_queued=%d " "delivered=%d failed=%d",
        kafka.queued,
        kafka.frames_queued,
        kafka.delivered,
        kafka.failed,
    )


async def report_metrics(
    stop_event: asyncio.Event,
    source_stats: Mapping[str, SourceStats],
    publisher: KafkaPublisher,
    interval_seconds: float,
) -> None:
    while not await wait_for_stop(stop_event, interval_seconds):
        log_metrics(source_stats, publisher)
