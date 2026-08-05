import json
from pathlib import Path

from common.models import DeadLetterEvent
from producer.adapters.binance import parse_binance_trade
from producer.kafka_publisher import DeliveryCallback, KafkaPublisher

FIXTURES = Path(__file__).parent / "fixtures"


class FakeMessage:
    def topic(self) -> str:
        return "raw-trades"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 14


class FakeProducer:
    def __init__(self) -> None:
        self.records: list[tuple[str, str | bytes | None, str | bytes | None]] = []
        self.callback: DeliveryCallback | None = None
        self.flush_timeout: float | None = None

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        on_delivery: DeliveryCallback | None = None,
    ) -> None:
        self.records.append((topic, key, value))
        self.callback = on_delivery

    def poll(self, timeout: float = -1) -> int:
        if self.callback is None:
            return 0
        callback = self.callback
        self.callback = None
        callback(None, FakeMessage())
        return 1

    def flush(self, timeout: float = -1) -> int:
        self.flush_timeout = timeout
        self.poll(0)
        return 0


def test_trade_is_published_with_key_and_canonical_json() -> None:
    raw_message = (FIXTURES / "binance" / "btcusdt_trade.json").read_text().strip()
    event = parse_binance_trade(raw_message, received_at_ms=1_785_250_729_250)
    producer = FakeProducer()
    publisher = KafkaPublisher("raw-trades", "raw-trades-dlq", 10, producer)

    publisher.publish_trade(event)
    publisher.close()

    topic, key, value = producer.records[0]
    assert topic == "raw-trades"
    assert key == "binance:BTCUSDT"
    assert isinstance(value, str)
    assert json.loads(value)["event_id"] == "binance:BTCUSDT:6539522338"
    assert publisher.stats.queued == 1
    assert publisher.stats.trades_queued == 1
    assert publisher.stats.dead_letters_queued == 0
    assert publisher.stats.delivered == 1
    assert publisher.stats.failed == 0
    assert producer.flush_timeout == 10.0


def test_dead_letter_is_published_to_dlq_with_failure_key() -> None:
    producer = FakeProducer()
    publisher = KafkaPublisher("raw-trades", "raw-trades-dlq", 10, producer)
    event = DeadLetterEvent(
        failed_at_ms=1_735_689_600_150,
        failure_stage="source_decode",
        failure_type="SourcePayloadError",
        failure_message="invalid JSON",
        source="binance",
        source_channel="btcusdt@trade",
        original_payload_encoding="utf8-json",
        original_payload="not-json",
    )

    publisher.publish_dead_letter(event)

    topic, key, value = producer.records[0]
    assert topic == "raw-trades-dlq"
    assert key == "binance:source_decode"
    assert isinstance(value, str)
    assert json.loads(value)["failure_type"] == "SourcePayloadError"
    assert publisher.stats.dead_letters_queued == 1
