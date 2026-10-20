from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field

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
