import base64
import json
from pathlib import Path

import pytest

from common.contracts import RawFrame
from processing.decoders.binance import parse_binance_trade
from processing.decoders.mexc.adapter import parse_mexc_trade_frame
from processing.decoders.normalize import normalize_frame

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def captured(source: str) -> RawFrame:
    message = (
        (FIXTURES / "binance/btcusdt_trade.json").read_text().strip()
        if source == "binance"
        else base64.b64decode(
            (FIXTURES / "mexc/btcusdt_aggre_deals.pb.b64").read_text()
        )
    )
    return RawFrame.capture(
        message,
        source=source,
        symbol="BTCUSDT",
        channel="fixture",
        received_at_ms=1_785_250_729_250,
    )


@pytest.mark.parametrize("source", ["binance", "mexc"])
def test_raw_normalization_preserves_the_existing_canonical_events(source: str) -> None:
    frame = captured(source)
    expected = (
        (parse_binance_trade(frame.payload, frame.received_at_ms),)
        if source == "binance"
        else parse_mexc_trade_frame(
            base64.b64decode(frame.payload), frame.received_at_ms
        )
    )
    result = normalize_frame(frame.model_dump_json(), frame.kafka_key)
    assert result[0] == frame.capture_id
    assert result[3] == [event.model_dump_json() for event in expected]
    assert result[4] is None
    assert normalize_frame(frame.model_dump_json(), None) == result


@pytest.mark.parametrize("source,payload", [("binance", "not-json"), ("mexc", b"bad")])
def test_bad_market_data_survives_capture_and_is_rejected_during_processing(
    source: str,
    payload: str | bytes,
) -> None:
    frame = RawFrame.capture(
        payload,
        source=source,
        symbol="BTCUSDT",
        channel="fixture",
        received_at_ms=1,
    )
    result = normalize_frame(frame.model_dump_json(), frame.kafka_key)
    assert result[0] == frame.capture_id
    assert not result[3] and result[4].startswith("source_decode:")


@pytest.mark.parametrize("payload", [None, "bad", "{}", "[]", '{"schema_version":9}'])
def test_invalid_envelopes_return_diagnostic_rows(payload: str | None) -> None:
    result = normalize_frame(payload, None)
    assert not result[3] and result[4].startswith("invalid_envelope:")


def test_key_and_subscription_mismatch_do_not_leak_to_valid_events() -> None:
    frame = captured("mexc")
    assert normalize_frame(frame.model_dump_json(), "binance:BTCUSDT")[4]
    changed = json.loads(frame.model_dump_json())
    changed["subscription_symbol"] = "ETHUSDT"
    result = normalize_frame(json.dumps(changed), "mexc:ETHUSDT")
    assert not result[3] and "symbol" in result[4]
