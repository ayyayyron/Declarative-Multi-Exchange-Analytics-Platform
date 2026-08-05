import asyncio
import base64
import json
from pathlib import Path

import pytest

from producer.adapters.base import SourcePayloadError
from producer.mexc_client import (
    MEXC_PING,
    build_mexc_subscription,
    process_mexc_frame,
    send_mexc_heartbeats,
    validate_mexc_subscription,
)
from producer.runtime import SourceStats
from tests.fakes import RecordingPublisher

CHANNEL = "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"
FIXTURES = Path(__file__).parent / "fixtures"


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
    with pytest.raises(SourcePayloadError):
        validate_mexc_subscription(response, CHANNEL)


def test_mexc_frame_publishes_every_contained_trade() -> None:
    encoded = (FIXTURES / "mexc" / "btcusdt_aggre_deals.pb.b64").read_text().strip()
    publisher = RecordingPublisher()
    stats = SourceStats("mexc")

    process_mexc_frame(
        base64.b64decode(encoded),
        source_channel=CHANNEL,
        received_at_ms=1_785_250_716_500,
        publisher=publisher,
        stats=stats,
    )

    assert len(publisher.trades) == 6
    assert not publisher.dead_letters
    assert stats.frames_received == 1
    assert stats.events_decoded == 6
    assert stats.events_published == 6


def test_malformed_mexc_frame_is_published_to_dlq() -> None:
    publisher = RecordingPublisher()
    stats = SourceStats("mexc")

    process_mexc_frame(
        b"not-protobuf",
        source_channel=CHANNEL,
        received_at_ms=1_735_689_600_150,
        publisher=publisher,
        stats=stats,
    )

    assert not publisher.trades
    assert publisher.dead_letters[0].failure_stage == "source_decode"
    assert publisher.dead_letters[0].original_payload_encoding == "base64-protobuf"
    assert stats.events_rejected == 1


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
