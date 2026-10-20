import asyncio
import base64
import json
from pathlib import Path

import pytest

from ingestion.capture import capture_message
from ingestion.connectors.base import SubscriptionError
from ingestion.connectors.mexc.client import (
    MEXC_PING,
    build_mexc_subscription,
    consume_mexc_frames,
    send_mexc_heartbeats,
    validate_mexc_subscription,
)
from ingestion.runtime import SourceStats
from tests.fakes import RecordingPublisher

CHANNEL = "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class HeartbeatWebSocket:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event
        self.messages: list[str | bytes] = []

    async def send(self, message: str | bytes) -> None:
        self.messages.append(message)
        self.stop_event.set()


def test_mexc_subscription_uses_configured_channel() -> None:
    assert json.loads(build_mexc_subscription(CHANNEL)) == {
        "method": "SUBSCRIPTION",
        "params": [CHANNEL],
    }


def test_mexc_subscription_accepts_matching_acknowledgement() -> None:
    validate_mexc_subscription(
        json.dumps({"id": 0, "code": 0, "msg": CHANNEL}),
        CHANNEL,
    )


@pytest.mark.parametrize(
    "response",
    [
        "not-json",
        "[]",
        json.dumps({"id": 0, "code": 1, "msg": CHANNEL}),
        json.dumps({"id": 0, "code": 0, "msg": "other-channel"}),
    ],
)
def test_mexc_subscription_rejects_invalid_acknowledgement(response: str) -> None:
    with pytest.raises(SubscriptionError):
        validate_mexc_subscription(response, CHANNEL)


def test_mexc_binary_frame_is_captured_once_without_decoding() -> None:
    publisher = RecordingPublisher()
    stats = SourceStats("mexc", symbol="BTCUSDT")
    capture_message(b"not-protobuf", CHANNEL, 1000, publisher, stats)
    assert publisher.frames[0].payload_encoding == "base64"
    assert base64.b64decode(publisher.frames[0].payload) == b"not-protobuf"
    assert stats.frames_published == 1


def test_mexc_heartbeat_stops_with_pipeline() -> None:
    async def scenario() -> tuple[list[str | bytes], SourceStats]:
        stop_event = asyncio.Event()
        websocket = HeartbeatWebSocket(stop_event)
        stats = SourceStats("mexc")
        await send_mexc_heartbeats(websocket, stop_event, stats, 0.001)
        return websocket.messages, stats

    messages, stats = asyncio.run(scenario())

    assert messages == [MEXC_PING]
    assert stats.heartbeats_sent == 1


def test_known_control_messages_are_skipped_but_unexpected_text_is_captured() -> None:
    async def scenario() -> RecordingPublisher:
        messages = [json.dumps({"code": 0, "msg": "PONG"}), "unexpected-text"]

        class Socket:
            async def recv(self) -> str:
                return messages.pop(0)

        publisher = RecordingPublisher()
        stats = SourceStats("mexc", symbol="BTCUSDT")
        await consume_mexc_frames(
            Socket(),
            CHANNEL,
            publisher,
            stats,
            asyncio.Event(),
            1,
        )
        assert stats.frames_received == stats.frames_published == 1
        return publisher

    publisher = asyncio.run(scenario())
    assert publisher.frames[0].payload == "unexpected-text"
    assert publisher.frames[0].payload_encoding == "utf8"
