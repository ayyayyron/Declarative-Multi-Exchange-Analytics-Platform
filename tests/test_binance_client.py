from producer.binance_client import (
    build_binance_stream_url,
    process_binance_message,
)
from producer.runtime import SourceStats
from tests.fakes import RecordingPublisher


def test_binance_stream_url_uses_raw_stream_path() -> None:
    assert (
        build_binance_stream_url(
            "wss://data-stream.binance.vision/ws",
            "btcusdt@trade",
        )
        == "wss://data-stream.binance.vision/ws/btcusdt@trade"
    )


def test_malformed_binance_message_is_published_to_dlq() -> None:
    publisher = RecordingPublisher()
    stats = SourceStats("binance")

    process_binance_message(
        "not-json",
        source_channel="btcusdt@trade",
        received_at_ms=1_735_689_600_150,
        publisher=publisher,
        stats=stats,
    )

    assert not publisher.trades
    assert publisher.dead_letters[0].failure_stage == "source_decode"
    assert publisher.dead_letters[0].original_payload == "not-json"
    assert stats.frames_received == 1
    assert stats.events_rejected == 1
