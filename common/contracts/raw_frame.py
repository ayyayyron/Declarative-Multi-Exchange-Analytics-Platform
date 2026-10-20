"""Raw capture envelope: preserve a received market frame before business decoding."""

import base64
from typing import Literal, Self
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
    model_validator,
)

from common.contracts.types import Exchange


class RawFrame(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    capture_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    source: Exchange
    subscription_symbol: str = Field(pattern=r"^[A-Z0-9]{3,20}$")
    source_channel: str = Field(min_length=1, max_length=1000)
    feed_type: Literal["trade"] = "trade"
    transport: Literal["websocket"] = "websocket"
    received_at_ms: PositiveInt
    payload_encoding: Literal["utf8", "base64"]
    payload: str

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_version_one(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("Only raw frame version 1 is supported")
        return value

    @model_validator(mode="after")
    def valid_binary_payload(self) -> Self:
        if self.payload_encoding == "base64":
            base64.b64decode(self.payload, validate=True)
        return self

    @property
    def kafka_key(self) -> str:
        """Group this subscription's frames for routing, rather than identify
        one frame.
        """
        return f"{self.source}:{self.subscription_symbol}"

    @classmethod
    def capture(
        cls,
        message: str | bytes,
        *,
        source: Exchange,
        symbol: str,
        channel: str,
        received_at_ms: int,
    ) -> "RawFrame":
        """Assign a capture ID and preserve text or binary payload without
        decoding trades.
        """
        return cls(
            capture_id=uuid4().hex,
            source=source,
            subscription_symbol=symbol,
            source_channel=channel,
            received_at_ms=received_at_ms,
            payload_encoding="utf8" if isinstance(message, str) else "base64",
            payload=(
                message
                if isinstance(message, str)
                else base64.b64encode(message).decode("ascii")
            ),
        )
