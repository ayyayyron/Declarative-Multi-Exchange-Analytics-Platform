import base64
import json

import pytest
from pydantic import ValidationError

from common.contracts import RawFrame


@pytest.mark.parametrize("message", ["not-json", "", b"\x00\xffbad", b""])
def test_raw_capture_round_trips_original_payload(message: str | bytes) -> None:
    frame = RawFrame.capture(
        message,
        source="binance",
        symbol="BTCUSDT",
        channel="fixture",
        received_at_ms=1000,
    )
    restored = RawFrame.model_validate_json(frame.model_dump_json())
    original = (
        restored.payload
        if restored.payload_encoding == "utf8"
        else base64.b64decode(restored.payload)
    )
    assert original == message
    assert restored.capture_id == frame.capture_id


@pytest.mark.parametrize("version", [True, 1.0, "1", 2])
def test_raw_version_is_explicit_and_not_coerced(version: object) -> None:
    frame = RawFrame.capture(
        "x",
        source="binance",
        symbol="BTCUSDT",
        channel="fixture",
        received_at_ms=1,
    )
    value = json.loads(frame.model_dump_json())
    value["schema_version"] = version
    with pytest.raises(ValidationError):
        RawFrame.model_validate(value)


def test_invalid_base64_is_an_invalid_envelope_not_a_market_decode() -> None:
    frame = RawFrame.capture(
        b"x",
        source="mexc",
        symbol="BTCUSDT",
        channel="fixture",
        received_at_ms=1,
    )
    value = json.loads(frame.model_dump_json())
    value["payload"] = "%%%"
    with pytest.raises(ValidationError):
        RawFrame.model_validate(value)
