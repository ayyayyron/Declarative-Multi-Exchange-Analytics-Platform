from common.contracts import RawFrame
from ingestion.kafka_publisher import KafkaPublisher
from tests.fakes import FakeProducer


def test_raw_frame_is_published_without_changing_payload_or_capture_id() -> None:
    frame = RawFrame.capture(
        "not-json",
        source="binance",
        symbol="BTCUSDT",
        channel="btc@trade",
        received_at_ms=1000,
    )
    producer = FakeProducer()
    publisher = KafkaPublisher("raw-market-frames-v1", 10, producer)
    publisher.publish_frame(frame)
    publisher.close()
    topic, key, value = producer.records[0]
    assert topic == "raw-market-frames-v1" and key == "binance:BTCUSDT"
    assert isinstance(value, (str, bytes))
    assert RawFrame.model_validate_json(value) == frame
    assert publisher.stats.frames_queued == publisher.stats.delivered == 1
    assert publisher.stats.failed == 0
    assert producer.flush_timeout == 10.0
