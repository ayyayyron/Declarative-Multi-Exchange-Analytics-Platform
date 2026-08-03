from typing import Final

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Decimal values remain strings until streaming validation can reject a malformed
# number instead of silently turning it into a null.
TRADE_EVENT_WIRE_SCHEMA: Final = StructType(
    [
        StructField("schema_version", LongType(), nullable=False),
        StructField("event_id", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_time_ms", LongType(), nullable=False),
        StructField("trade_time_ms", LongType(), nullable=False),
        StructField("received_at_ms", LongType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("source_event_id", StringType(), nullable=True),
        StructField("price", StringType(), nullable=False),
        StructField("quantity", StringType(), nullable=False),
        StructField("trade_side", StringType(), nullable=False),
        StructField("is_buyer_maker", BooleanType(), nullable=True),
        StructField("source_payload_encoding", StringType(), nullable=False),
        StructField("source_payload", StringType(), nullable=False),
    ]
)
