"""Package received market messages for Kafka without interpreting provider fields."""

from common.contracts import RawFrame
from ingestion.kafka_publisher import FramePublisher
from ingestion.runtime import SourceStats


def capture_message(
    message: str | bytes,
    source_channel: str,
    received_at_ms: int,
    publisher: FramePublisher,
    stats: SourceStats,
) -> None:
    """Wrap and queue one frame, then update this feed's capture counters."""
    if stats.symbol is None:
        raise ValueError("Capture requires the configured subscription symbol")
    stats.frames_received += 1
    stats.last_received_at_ms = received_at_ms
    frame = RawFrame.capture(
        message,
        source=stats.source,
        symbol=stats.symbol,
        channel=source_channel,
        received_at_ms=received_at_ms,
    )
    publisher.publish_frame(frame)
    stats.frames_published += 1
