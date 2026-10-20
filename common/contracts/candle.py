"""Versioned replacement snapshot emitted by candle processing."""

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from common.contracts.types import Decimal28x10, Exchange

PositiveInteger = Annotated[int, Field(strict=True, gt=0)]
CandleInterval = Literal["1m", "5m"]
CANDLE_MILLISECONDS: dict[CandleInterval, int] = {"1m": 60_000, "5m": 300_000}


class CandleSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: Exchange
    symbol: str = Field(pattern=r"^[A-Z0-9]{3,20}$")
    interval: CandleInterval
    window_start_ms: PositiveInteger
    window_end_ms: PositiveInteger
    open: Decimal28x10
    high: Decimal28x10
    low: Decimal28x10
    close: Decimal28x10
    volume: Annotated[Decimal, Field(gt=0, max_digits=38, decimal_places=10)]
    trade_count: PositiveInteger
    revision: PositiveInteger
    is_final: Annotated[bool, Field(strict=True)]

    @model_validator(mode="after")
    def require_consistent_candle(self) -> Self:
        duration = CANDLE_MILLISECONDS[self.interval]
        if self.window_start_ms % duration or (
            self.window_end_ms != self.window_start_ms + duration
        ):
            raise ValueError("Candle boundaries must match the aligned interval")
        if (
            not self.low
            <= min(self.open, self.close)
            <= max(self.open, self.close)
            <= self.high
        ):
            raise ValueError("Candle OHLC values are inconsistent")
        return self
