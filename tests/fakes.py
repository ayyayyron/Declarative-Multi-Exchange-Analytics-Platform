from common.contracts import RawFrame
from ingestion.kafka_publisher import DeliveryCallback


class RecordingPublisher:
    def __init__(self) -> None:
        self.frames: list[RawFrame] = []

    def publish_frame(self, frame: RawFrame) -> None:
        self.frames.append(frame)


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
