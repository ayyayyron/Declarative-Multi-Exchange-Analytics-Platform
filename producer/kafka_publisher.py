from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, cast

from confluent_kafka import Producer

from common.logging_config import get_logger
from common.models import DeadLetterEvent, TradeEvent

LOGGER = get_logger(__name__)


class DeliveryMessage(Protocol):
    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...


DeliveryCallback = Callable[[object | None, DeliveryMessage], None]


class ProducerClient(Protocol):
    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        on_delivery: DeliveryCallback | None = None,
    ) -> None: ...

    def poll(self, timeout: float = -1) -> int: ...

    def flush(self, timeout: float = -1) -> int: ...


class EventPublisher(Protocol):
    def publish_trade(self, event: TradeEvent) -> None: ...

    def publish_dead_letter(self, event: DeadLetterEvent) -> None: ...


@dataclass
class PublishStats:
    queued: int = 0
    trades_queued: int = 0
    dead_letters_queued: int = 0
    delivered: int = 0
    failed: int = 0


def build_producer(
    bootstrap_servers: str,
    client_id: str,
) -> ProducerClient:
    config: dict[str, object] = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
    }
    return cast(ProducerClient, Producer(config))


class KafkaPublisher:
    def __init__(
        self,
        trade_topic: str,
        dead_letter_topic: str,
        flush_timeout_seconds: int,
        producer: ProducerClient,
    ) -> None:
        self.trade_topic = trade_topic
        self.dead_letter_topic = dead_letter_topic
        self.flush_timeout_seconds = flush_timeout_seconds
        self.producer = producer
        self.stats = PublishStats()

    def publish_trade(self, event: TradeEvent) -> None:
        self._publish(
            topic=self.trade_topic,
            key=event.kafka_key,
            value=event.model_dump_json(),
        )
        self.stats.trades_queued += 1

    def publish_dead_letter(self, event: DeadLetterEvent) -> None:
        self._publish(
            topic=self.dead_letter_topic,
            key=f"{event.source}:{event.failure_stage}",
            value=event.model_dump_json(),
        )
        self.stats.dead_letters_queued += 1

    def _publish(self, topic: str, key: str, value: str) -> None:
        try:
            self.producer.produce(
                topic,
                key=key,
                value=value,
                on_delivery=self._delivery_report,
            )
        except BufferError:
            self.producer.poll(1.0)
            self.producer.produce(
                topic,
                key=key,
                value=value,
                on_delivery=self._delivery_report,
            )
        self.stats.queued += 1
        self.producer.poll(0)

    def close(self) -> None:
        remaining = self.producer.flush(float(self.flush_timeout_seconds))
        if remaining:
            raise RuntimeError(
                f"Kafka producer closed with {remaining} queued messages"
            )

    def _delivery_report(
        self,
        error: object | None,
        message: DeliveryMessage,
    ) -> None:
        if error is not None:
            self.stats.failed += 1
            LOGGER.error("Kafka delivery failed: %s", error)
            return

        self.stats.delivered += 1
        LOGGER.debug(
            "Kafka delivery confirmed: topic=%s partition=%d offset=%d",
            message.topic(),
            message.partition(),
            message.offset(),
        )
