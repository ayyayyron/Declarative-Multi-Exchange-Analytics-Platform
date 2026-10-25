"""Publish raw envelopes asynchronously and track Kafka delivery acknowledgements."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, cast

from confluent_kafka import Producer

from common.contracts import RawFrame
from common.logging_config import get_logger

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


class FramePublisher(Protocol):
    async def publish_frame(self, frame: RawFrame) -> None: ...


class KafkaPublishError(RuntimeError):
    """A publishing failure that must not be treated as a socket reconnect."""


@dataclass
class PublishStats:
    queued: int = 0
    frames_queued: int = 0
    delivered: int = 0
    failed: int = 0


def build_producer(
    bootstrap_servers: str,
    client_id: str,
) -> ProducerClient:
    """Create an idempotent Kafka client with acknowledgements from all in-sync
    replicas.
    """
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
        frame_topic: str,
        flush_timeout_seconds: int,
        producer: ProducerClient,
        *,
        enqueue_timeout_seconds: float = 10,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        if enqueue_timeout_seconds <= 0:
            raise ValueError("Enqueue timeout must be positive")
        self.frame_topic = frame_topic
        self.flush_timeout_seconds = flush_timeout_seconds
        self.producer = producer
        self.stats = PublishStats()
        self.enqueue_timeout_seconds = enqueue_timeout_seconds
        self.stop_event = stop_event

    async def publish_frame(self, frame: RawFrame) -> None:
        """Queue the envelope unchanged; broker delivery is confirmed by a
        later callback.
        """
        try:
            await self._publish(
                self.frame_topic, frame.kafka_key, frame.model_dump_json()
            )
        except asyncio.CancelledError:
            self.stats.failed += 1
            LOGGER.error("Kafka enqueue cancelled: capture_id=%s", frame.capture_id)
            raise
        except Exception as error:
            self.stats.failed += 1
            raise KafkaPublishError(
                f"Kafka enqueue failed: capture_id={frame.capture_id}: {error}"
            ) from error
        self.stats.frames_queued += 1

    async def _publish(self, topic: str, key: str, value: str) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.enqueue_timeout_seconds
        while True:
            try:
                self.producer.produce(
                    topic, key=key, value=value, on_delivery=self._delivery_report
                )
                break
            except BufferError:
                self.producer.poll(0)
                remaining = deadline - loop.time()
                if self.stop_event is not None and self.stop_event.is_set():
                    raise KafkaPublishError("Shutdown with an unqueued frame") from None
                if remaining <= 0:
                    raise KafkaPublishError("Kafka queue remained full") from None
                delay = min(0.05, remaining)
                if self.stop_event is None:
                    await asyncio.sleep(delay)
                else:
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), delay)
                    except TimeoutError:
                        pass
        self.stats.queued += 1
        self.producer.poll(0)

    def close(self) -> None:
        """Wait for queued deliveries and raise if records remain after the
        flush timeout.
        """
        remaining = self.producer.flush(float(self.flush_timeout_seconds))
        if remaining:
            raise RuntimeError(
                f"Kafka producer closed with {remaining} queued messages"
            )
        if self.stats.failed:
            raise KafkaPublishError(
                f"Kafka failed to publish {self.stats.failed} frames"
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
