import base64
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)

Decimal28x10 = Annotated[
    Decimal,
    Field(gt=Decimal("0"), max_digits=28, decimal_places=10),
]
Exchange = Literal["binance", "mexc"]
TradeSide = Literal["buy", "sell", "unknown"]
PayloadEncoding = Literal["utf8-json", "base64-protobuf"]
FailureStage = Literal[
    "source_decode",
    "contract_validation",
    "kafka_delivery",
    "spark_decode",
]


class TradeEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[2] = 2
    event_id: str = Field(
        min_length=5,
        max_length=200,
        pattern=r"^[A-Za-z0-9:_-]+$",
    )
    source: Exchange
    event_type: Literal["trade"] = "trade"
    event_time_ms: PositiveInt
    trade_time_ms: PositiveInt
    received_at_ms: PositiveInt
    symbol: str = Field(min_length=3, max_length=20, pattern=r"^[A-Z0-9]+$")
    source_event_id: str | None = Field(default=None, min_length=1, max_length=100)
    price: Decimal28x10
    quantity: Decimal28x10
    trade_side: TradeSide
    is_buyer_maker: bool | None = None
    source_payload_encoding: PayloadEncoding
    source_payload: str = Field(min_length=2)

    @field_validator("symbol")
    @classmethod
    def require_canonical_symbol(cls, value: str) -> str:
        if value != value.upper():
            raise ValueError("symbol must be uppercase")
        return value

    @model_validator(mode="after")
    def require_source_consistency(self) -> "TradeEvent":
        if not self.event_id.startswith(f"{self.source}:"):
            raise ValueError("event_id must start with the source name")

        expected_encoding: PayloadEncoding = (
            "utf8-json" if self.source == "binance" else "base64-protobuf"
        )
        if self.source_payload_encoding != expected_encoding:
            raise ValueError("source_payload_encoding does not match source")
        if expected_encoding == "base64-protobuf":
            try:
                base64.b64decode(self.source_payload, validate=True)
            except ValueError as error:
                raise ValueError("source_payload must be valid Base64") from error
        return self

    @property
    def kafka_key(self) -> str:
        return f"{self.source}:{self.symbol}"


class DeadLetterEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[2] = 2
    failed_at_ms: PositiveInt
    failure_stage: FailureStage
    failure_type: str = Field(min_length=1)
    failure_message: str = Field(min_length=1)
    source: Exchange | Literal["unknown"]
    source_channel: str | None = None
    original_payload_encoding: PayloadEncoding | Literal["unknown"]
    original_payload: str = Field(min_length=1)
    kafka_topic: str | None = None
    kafka_partition: NonNegativeInt | None = None
    kafka_offset: NonNegativeInt | None = None
    replay_status: Literal["pending", "skipped", "replayed"] = "pending"
