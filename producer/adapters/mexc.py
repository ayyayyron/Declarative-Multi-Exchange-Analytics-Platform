from __future__ import annotations

import base64
import hashlib

from google.protobuf.message import DecodeError

from common.models import TradeEvent, TradeSide
from producer.adapters.base import SourcePayloadError
from producer.generated.mexc_market_data_pb2 import PushDataV3ApiWrapper

TRADE_SIDE_BY_CODE: dict[int, TradeSide] = {1: "buy", 2: "sell"}


def parse_mexc_trade_frame(
    raw_frame: bytes,
    received_at_ms: int,
) -> tuple[TradeEvent, ...]:
    wrapper = PushDataV3ApiWrapper()
    try:
        wrapper.ParseFromString(raw_frame)
    except DecodeError as error:
        raise SourcePayloadError("MEXC payload is not valid Protobuf") from error

    if wrapper.WhichOneof("body") != "publicAggreDeals":
        raise SourcePayloadError("MEXC payload is not an aggregate-deals frame")
    if not wrapper.symbol:
        raise SourcePayloadError("MEXC payload has no symbol")

    encoded_frame = base64.b64encode(raw_frame).decode("ascii")
    frame_hash = hashlib.sha256(raw_frame).hexdigest()
    events: list[TradeEvent] = []

    for index, deal in enumerate(wrapper.publicAggreDeals.deals):
        source_event_id = deal.tradeId or None
        event_id = (
            f"mexc:{wrapper.symbol}:{source_event_id}"
            if source_event_id
            else f"mexc:{frame_hash}:{index}"
        )
        event_time_ms = wrapper.sendTime or wrapper.createTime or deal.time
        events.append(
            TradeEvent(
                event_id=event_id,
                source="mexc",
                event_time_ms=event_time_ms,
                trade_time_ms=deal.time,
                received_at_ms=received_at_ms,
                symbol=wrapper.symbol,
                source_event_id=source_event_id,
                price=deal.price,
                quantity=deal.quantity,
                trade_side=TRADE_SIDE_BY_CODE.get(deal.tradeType, "unknown"),
                source_payload_encoding="base64-protobuf",
                source_payload=encoded_frame,
            )
        )

    if not events:
        raise SourcePayloadError("MEXC aggregate-deals frame contains no trades")
    return tuple(events)
