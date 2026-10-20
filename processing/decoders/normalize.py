"""Pure raw-frame decoding used by Spark's Python UDF and offline fixtures."""

import base64
from typing import TypeAlias

from pydantic import ValidationError

from common.contracts import RawFrame, TradeEvent
from processing.decoders.base import SourcePayloadError
from processing.decoders.binance import parse_binance_trade
from processing.decoders.mexc.adapter import parse_mexc_trade_frame

NormalizedFrame: TypeAlias = tuple[
    str | None, str | None, str | None, list[str], str | None
]


def normalize_frame(frame_json: str | None, source_key: str | None) -> NormalizedFrame:
    """Decode one envelope into canonical events or a frame-level diagnostic.

    Return capture ID, venue, symbol, event JSON list, and error.
    Preserve original receipt times; perform no I/O.
    """
    try:
        frame = RawFrame.model_validate_json(frame_json or "")
    except (ValidationError, ValueError) as error:
        return None, None, None, [], f"invalid_envelope:{str(error)[:500]}"
    try:
        if source_key is not None and source_key != frame.kafka_key:
            raise SourcePayloadError("Raw frame key does not match subscription")
        if frame.source == "binance":
            if frame.payload_encoding != "utf8":
                raise SourcePayloadError("Binance market frame must be text")
            events: tuple[TradeEvent, ...] = (
                parse_binance_trade(frame.payload, frame.received_at_ms),
            )
        else:
            if frame.payload_encoding != "base64":
                raise SourcePayloadError("MEXC market frame must be binary")
            events = parse_mexc_trade_frame(
                base64.b64decode(frame.payload, validate=True), frame.received_at_ms
            )
        if any(event.symbol != frame.subscription_symbol for event in events):
            raise SourcePayloadError("Decoded symbol does not match subscription")
        return (
            frame.capture_id,
            frame.source,
            frame.subscription_symbol,
            [event.model_dump_json() for event in events],
            None,
        )
    except (SourcePayloadError, ValidationError, ValueError) as error:
        return (
            frame.capture_id,
            frame.source,
            frame.subscription_symbol,
            [],
            f"source_decode:{type(error).__name__}:{str(error)[:500]}",
        )
