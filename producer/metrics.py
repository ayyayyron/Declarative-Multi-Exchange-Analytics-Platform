import asyncio
from collections.abc import Mapping

from common.logging_config import get_logger
from common.models import Exchange
from producer.kafka_publisher import KafkaPublisher
from producer.runtime import SourceStats, wait_for_stop

LOGGER = get_logger(__name__)


def log_metrics(
    source_stats: Mapping[Exchange, SourceStats],
    publisher: KafkaPublisher,
) -> None:
    for source in sorted(source_stats):
        stats = source_stats[source]
        LOGGER.info(
            "source_metrics source=%s connections=%d frames_received=%d "
            "events_decoded=%d events_published=%d events_rejected=%d "
            "connection_failures=%d reconnects=%d heartbeats_sent=%d "
            "last_received_at_ms=%s last_event_time_ms=%s",
            source,
            stats.connections,
            stats.frames_received,
            stats.events_decoded,
            stats.events_published,
            stats.events_rejected,
            stats.connection_failures,
            stats.reconnects,
            stats.heartbeats_sent,
            stats.last_received_at_ms,
            stats.last_event_time_ms,
        )

    kafka = publisher.stats
    LOGGER.info(
        "kafka_metrics queued=%d trades_queued=%d dead_letters_queued=%d "
        "delivered=%d failed=%d",
        kafka.queued,
        kafka.trades_queued,
        kafka.dead_letters_queued,
        kafka.delivered,
        kafka.failed,
    )


async def report_metrics(
    stop_event: asyncio.Event,
    source_stats: Mapping[Exchange, SourceStats],
    publisher: KafkaPublisher,
    interval_seconds: float,
) -> None:
    while not await wait_for_stop(stop_event, interval_seconds):
        log_metrics(source_stats, publisher)
