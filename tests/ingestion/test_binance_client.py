from ingestion.capture import capture_message
from ingestion.connectors.binance.client import build_binance_stream_url
from ingestion.runtime import SourceStats
from tests.fakes import RecordingPublisher


def test_binance_stream_url_uses_raw_stream_path() -> None:
    assert build_binance_stream_url("wss://example/ws", "btcusdt@trade") == (
        "wss://example/ws/btcusdt@trade"
    )


def test_invalid_json_is_captured_without_business_decoding() -> None:
    publisher = RecordingPublisher()
    stats = SourceStats("binance", symbol="BTCUSDT")
    capture_message("not-json", "btcusdt@trade", 1000, publisher, stats)
    assert publisher.frames[0].payload == "not-json"
    assert publisher.frames[0].subscription_symbol == "BTCUSDT"
    assert stats.frames_received == stats.frames_published == 1
