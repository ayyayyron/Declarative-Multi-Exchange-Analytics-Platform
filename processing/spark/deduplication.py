"""Business-event deduplication, independent of frame capture and Kafka offsets."""

from pyspark.sql import DataFrame


def deduplicate_trades(
    trades: DataFrame, watermark_delay: str = "2 minutes"
) -> DataFrame:
    """Keep one occurrence of each venue/symbol/event ID.

    Streaming identity state is bounded by a trade-time watermark; the default
    delay is illustrative. Repeated identities must describe the same trade,
    not corrections. Batch inputs deduplicate across the supplied rows only.
    Do not chain this stateful stream with an Update-mode candle aggregation.
    """
    identity = ["source", "symbol", "event_id"]
    if trades.isStreaming:
        return trades.withWatermark(
            "trade_time", watermark_delay
        ).dropDuplicatesWithinWatermark(identity)
    return trades.dropDuplicates(identity)
