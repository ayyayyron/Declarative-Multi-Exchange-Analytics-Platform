import base64
from decimal import Decimal

import pytest
from pydantic import ValidationError

from common.contracts import TradeEvent


def valid_trade() -> dict[str, object]:
    return {
        "event_id": "binance:BTCUSDT:123456789",
        "source": "binance",
        "event_time_ms": 1_735_689_600_123,
        "trade_time_ms": 1_735_689_600_120,
        "received_at_ms": 1_735_689_600_150,
        "symbol": "BTCUSDT",
        "source_event_id": "123456789",
        "price": "43250.50",
        "quantity": "0.015",
        "trade_side": "buy",
        "is_buyer_maker": False,
        "source_payload_encoding": "utf8-json",
        "source_payload": '{"e":"trade"}',
    }


def test_trade_event_preserves_decimal_wire_format() -> None:
    event = TradeEvent.model_validate(valid_trade())

    assert event.schema_version == 2
    assert event.price == Decimal("43250.50")
    assert event.kafka_key == "binance:BTCUSDT"
    assert '"price":"43250.50"' in event.model_dump_json()
    assert '"quantity":"0.015"' in event.model_dump_json()


@pytest.mark.parametrize("field", ["price", "quantity"])
@pytest.mark.parametrize("value", ["0.00000001", "0.0000000001", "1E-8"])
def test_trade_event_serializes_small_decimals_without_exponents(
    field: str, value: str
) -> None:
    payload = valid_trade()
    payload[field] = value
    event = TradeEvent.model_validate(payload)

    assert event.model_dump(mode="json")[field] == format(Decimal(value), "f")
    assert event.model_dump()[field] == Decimal(value)
    assert TradeEvent.model_validate_json(event.model_dump_json()) == event


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("symbol", "btcusdt"),
        ("price", "0"),
        ("quantity", "not-a-number"),
        ("event_id", "mexc:BTCUSDT:123"),
    ],
)
def test_trade_event_rejects_noncanonical_values(field: str, value: str) -> None:
    payload = valid_trade()
    payload[field] = value

    with pytest.raises(ValidationError):
        TradeEvent.model_validate(payload)


def test_trade_event_requires_source_payload_encoding() -> None:
    payload = valid_trade()
    payload.update(
        {
            "event_id": "mexc:BTCUSDT:987",
            "source": "mexc",
            "source_payload_encoding": "base64-protobuf",
            "source_payload": base64.b64encode(b"protobuf-frame").decode(),
            "is_buyer_maker": None,
        }
    )

    event = TradeEvent.model_validate(payload)

    assert event.kafka_key == "mexc:BTCUSDT"


def test_trade_event_rejects_unknown_contract_field() -> None:
    payload = valid_trade()
    payload["unexpected"] = True

    with pytest.raises(ValidationError):
        TradeEvent.model_validate(payload)
