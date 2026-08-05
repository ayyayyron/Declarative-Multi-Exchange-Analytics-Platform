from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, cast

from confluent_kafka import Producer

from common.logging_config import get_logger
from common.models import TradeEvent

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


@dataclass
class PublishStats:
    queued: int = 0
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


class KafkaTradePublisher:
    def __init__(
        self,
        topic: str,
        flush_timeout_seconds: int,
        producer: ProducerClient,
    ) -> None:
        self.topic = topic
        self.flush_timeout_seconds = flush_timeout_seconds
        self.producer = producer
        self.stats = PublishStats()

    def publish(self, event: TradeEvent) -> None:
        self.producer.produce(
            self.topic,
            key=event.kafka_key,
            value=event.model_dump_json(),
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
