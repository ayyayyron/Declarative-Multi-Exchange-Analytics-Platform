from streaming.schemas import TRADE_EVENT_WIRE_SCHEMA


def test_wire_schema_matches_canonical_contract_order() -> None:
    assert TRADE_EVENT_WIRE_SCHEMA.fieldNames() == [
        "schema_version",
        "event_id",
        "source",
        "event_type",
        "event_time_ms",
        "trade_time_ms",
        "received_at_ms",
        "symbol",
        "source_event_id",
        "price",
        "quantity",
        "trade_side",
        "is_buyer_maker",
        "source_payload_encoding",
        "source_payload",
    ]

    assert TRADE_EVENT_WIRE_SCHEMA["source_event_id"].nullable
    assert TRADE_EVENT_WIRE_SCHEMA["is_buyer_maker"].nullable
