"""Translate Binance trade JSON into the shared trade contract."""

from __future__ import annotations

import json
from typing import Any

from common.contracts import TradeEvent
from processing.decoders.base import SourcePayloadError


def parse_binance_trade(raw_message: str, received_at_ms: int) -> TradeEvent:
    """Decode one trade, preserving native identity, decimal values, and source
    payload.
    """
    try:
        payload: Any = json.loads(raw_message)
    except json.JSONDecodeError as error:
        raise SourcePayloadError("Binance payload is not valid JSON") from error

    if not isinstance(payload, dict) or payload.get("e") != "trade":
        raise SourcePayloadError("Binance payload is not a trade event")

    is_buyer_maker = payload.get("m")
    if not isinstance(is_buyer_maker, bool):
        raise SourcePayloadError("Binance maker flag must be boolean")

    try:
        source_event_id = str(payload["t"])
        return TradeEvent(
            event_id=f"binance:{payload['s']}:{source_event_id}",
            source="binance",
            event_time_ms=payload["E"],
            trade_time_ms=payload["T"],
            received_at_ms=received_at_ms,
            symbol=payload["s"],
            source_event_id=source_event_id,
            price=payload["p"],
            quantity=payload["q"],
            trade_side="sell" if is_buyer_maker else "buy",
            is_buyer_maker=is_buyer_maker,
            source_payload_encoding="utf8-json",
            source_payload=raw_message,
        )
    except KeyError as error:
        raise SourcePayloadError(
            f"Binance payload is missing field: {error.args[0]}"
        ) from error
