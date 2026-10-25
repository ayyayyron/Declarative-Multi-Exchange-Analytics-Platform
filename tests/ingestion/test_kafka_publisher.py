import asyncio
from unittest.mock import MagicMock

import pytest

from common.contracts import RawFrame
from ingestion.kafka_publisher import KafkaPublisher, KafkaPublishError
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
    asyncio.run(publisher.publish_frame(frame))
    publisher.close()
    topic, key, value = producer.records[0]
    assert topic == "raw-market-frames-v1" and key == "binance:BTCUSDT"
    assert isinstance(value, (str, bytes))
    assert RawFrame.model_validate_json(value) == frame
    assert publisher.stats.frames_queued == publisher.stats.delivered == 1
    assert publisher.stats.failed == 0
    assert producer.flush_timeout == 10.0


def frame() -> RawFrame:
    return RawFrame.capture(
        "original payload",
        source="binance",
        symbol="BTCUSDT",
        channel="btc@trade",
        received_at_ms=1000,
    )


def test_repeated_backpressure_retries_identical_frame_without_blocking_peers() -> None:
    async def scenario() -> None:
        producer = MagicMock()
        producer.produce.side_effect = [
            BufferError(),
            BufferError(),
            BufferError(),
            None,
        ]
        publisher = KafkaPublisher("raw", 10, producer)
        original = frame()
        peer_ran = asyncio.Event()

        async def peer() -> None:
            peer_ran.set()

        await asyncio.wait_for(
            asyncio.gather(publisher.publish_frame(original), peer()), timeout=1
        )
        assert peer_ran.is_set()
        assert producer.produce.call_count == 4
        assert all(
            call.kwargs["value"] == original.model_dump_json()
            for call in producer.produce.call_args_list
        )
        assert publisher.stats.queued == publisher.stats.frames_queued == 1
        assert publisher.stats.failed == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("reason", ["timeout", "stop", "cancel"])
def test_full_queue_has_bounded_interruptible_failure(reason: str) -> None:
    async def scenario() -> None:
        producer = MagicMock()
        producer.produce.side_effect = BufferError("full")
        stop = asyncio.Event()
        publisher = KafkaPublisher(
            "raw", 10, producer, enqueue_timeout_seconds=0.02, stop_event=stop
        )
        task = asyncio.create_task(publisher.publish_frame(frame()))
        await asyncio.sleep(0)
        if reason == "stop":
            stop.set()
        if reason == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(KafkaPublishError):
                await asyncio.wait_for(task, timeout=1)
        assert publisher.stats.failed == 1
        assert publisher.stats.queued == publisher.stats.frames_queued == 0

    asyncio.run(scenario())


def test_terminal_enqueue_error_and_delivery_failure_are_reported() -> None:
    producer = MagicMock()
    producer.produce.side_effect = RuntimeError("invalid producer")
    publisher = KafkaPublisher("raw", 10, producer)
    with pytest.raises(KafkaPublishError, match="invalid producer"):
        asyncio.run(publisher.publish_frame(frame()))
    assert publisher.stats.failed == 1

    producer = MagicMock()
    producer.flush.return_value = 0
    publisher = KafkaPublisher("raw", 10, producer)
    asyncio.run(publisher.publish_frame(frame()))
    publisher._delivery_report("delivery timeout", MagicMock())
    with pytest.raises(KafkaPublishError, match="failed to publish"):
        publisher.close()


def test_shutdown_reports_unflushed_messages() -> None:
    producer = MagicMock()
    producer.flush.return_value = 2
    publisher = KafkaPublisher("raw", 10, producer)
    with pytest.raises(RuntimeError, match="2 queued messages"):
        publisher.close()
