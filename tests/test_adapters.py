import base64
from pathlib import Path

import pytest

from producer.adapters.base import SourcePayloadError
from producer.adapters.binance import parse_binance_trade
from producer.adapters.mexc import parse_mexc_trade_frame
from producer.generated.mexc_market_data_pb2 import PushDataV3ApiWrapper

FIXTURES = Path(__file__).parent / "fixtures"


def test_binance_fixture_maps_to_canonical_trade() -> None:
    raw_message = (FIXTURES / "binance" / "btcusdt_trade.json").read_text().strip()

    event = parse_binance_trade(raw_message, received_at_ms=1_785_250_729_250)

    assert event.event_id == "binance:BTCUSDT:6539522338"
    assert event.kafka_key == "binance:BTCUSDT"
    assert event.trade_side == "sell"
    assert event.source_payload == raw_message


def test_mexc_fixture_expands_to_canonical_trades() -> None:
    encoded = (FIXTURES / "mexc" / "btcusdt_aggre_deals.pb.b64").read_text().strip()
    raw_frame = base64.b64decode(encoded)

    events = parse_mexc_trade_frame(raw_frame, received_at_ms=1_785_250_716_500)

    assert len(events) == 6
    assert {event.source for event in events} == {"mexc"}
    assert {event.symbol for event in events} == {"BTCUSDT"}
    assert all(event.event_id.startswith("mexc:BTCUSDT:") for event in events)
    assert all(event.source_event_id for event in events)
    assert {event.trade_side for event in events} == {"buy", "sell"}
    assert all(event.source_payload == encoded for event in events)


def test_mexc_empty_trade_id_uses_stable_frame_identity() -> None:
    wrapper = PushDataV3ApiWrapper(
        channel="spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT",
        symbol="BTCUSDT",
        sendTime=1_735_689_600_123,
    )
    deal = wrapper.publicAggreDeals.deals.add()
    deal.price = "43250.50"
    deal.quantity = "0.015"
    deal.tradeType = 1
    deal.time = 1_735_689_600_120
    frame = wrapper.SerializeToString()

    first = parse_mexc_trade_frame(frame, received_at_ms=1_735_689_600_150)
    second = parse_mexc_trade_frame(frame, received_at_ms=1_735_689_600_151)

    assert first[0].source_event_id is None
    assert first[0].event_id == second[0].event_id


@pytest.mark.parametrize("payload", ["not-json", '{"e":"depth"}'])
def test_binance_rejects_non_trade_payload(payload: str) -> None:
    with pytest.raises(SourcePayloadError):
        parse_binance_trade(payload, received_at_ms=1_735_689_600_150)


def test_mexc_rejects_non_trade_frame() -> None:
    wrapper = PushDataV3ApiWrapper(channel="other", symbol="BTCUSDT")

    with pytest.raises(SourcePayloadError):
        parse_mexc_trade_frame(
            wrapper.SerializeToString(),
            received_at_ms=1_735_689_600_150,
        )
