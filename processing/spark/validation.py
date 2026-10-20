"""Validate canonical event columns with Spark SQL, then expose typed valid rows."""

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from processing.spark.schemas import TRADE_EVENT_PARSE_SCHEMA

PRICE_TYPE = DecimalType(28, 10)
DECIMAL_PATTERN = r"^(?:0|[1-9][0-9]{0,17})(?:\.[0-9]{1,10})?$"
EVENT_ID_PATTERN = r"^[A-Za-z0-9:_-]{5,200}$"
SYMBOL_PATTERN = r"^[A-Z0-9]{3,20}$"
BASE64_PATTERN = r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"


def decode_trade_records(
    kafka_records: DataFrame, extra_columns: Sequence[str] = ()
) -> DataFrame:
    """Parse canonical JSON and attach the first validation error while
    retaining provenance.
    """
    parsed = (
        kafka_records.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            *extra_columns,
        )
        .withColumn(
            "trade",
            F.from_json(
                "raw_value",
                TRADE_EVENT_PARSE_SCHEMA,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )
        .select(
            "kafka_key",
            "raw_value",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "trade.*",
            *extra_columns,
        )
    )
    return parsed.withColumn("validation_error", _first_validation_error())


def valid_trade_records(decoded_records: DataFrame) -> DataFrame:
    """Keep valid events, cast decimal fields, and derive the UTC trade timestamp."""
    return (
        decoded_records.filter(F.col("validation_error").isNull())
        .drop("_corrupt_record", "validation_error", "raw_value")
        .withColumn("price", F.col("price").cast(PRICE_TYPE))
        .withColumn("quantity", F.col("quantity").cast(PRICE_TYPE))
        .withColumn("trade_time", F.timestamp_millis("trade_time_ms"))
    )


def invalid_trade_records(decoded_records: DataFrame) -> DataFrame:
    return decoded_records.filter(F.col("validation_error").isNotNull()).select(
        "kafka_key",
        "raw_value",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "validation_error",
    )


def _first_validation_error() -> Column:
    rules: Sequence[tuple[Column, str]] = (
        (F.col("raw_value").isNull(), "null_kafka_value"),
        (F.col("_corrupt_record").isNotNull(), "malformed_json"),
        (
            F.col("schema_version").isNull() | (F.col("schema_version") != 2),
            "unsupported_schema_version",
        ),
        (
            F.col("event_type").isNull() | (F.col("event_type") != "trade"),
            "invalid_event_type",
        ),
        (
            F.col("source").isNull() | ~F.col("source").isin("binance", "mexc"),
            "invalid_source",
        ),
        (
            F.col("event_id").isNull() | ~F.col("event_id").rlike(EVENT_ID_PATTERN),
            "invalid_event_id",
        ),
        (
            ~F.expr("event_id LIKE concat(source, ':%')"),
            "event_id_source_mismatch",
        ),
        (
            F.col("symbol").isNull() | ~F.col("symbol").rlike(SYMBOL_PATTERN),
            "invalid_symbol",
        ),
        (
            F.col("source_event_id").isNotNull() & (F.length("source_event_id") == 0),
            "invalid_source_event_id",
        ),
        (
            F.col("event_time_ms").isNull() | (F.col("event_time_ms") <= 0),
            "invalid_event_time",
        ),
        (
            F.col("trade_time_ms").isNull() | (F.col("trade_time_ms") <= 0),
            "invalid_trade_time",
        ),
        (
            F.col("received_at_ms").isNull() | (F.col("received_at_ms") <= 0),
            "invalid_received_at",
        ),
        (_invalid_decimal("price"), "invalid_price"),
        (_invalid_decimal("quantity"), "invalid_quantity"),
        (
            F.col("trade_side").isNull()
            | ~F.col("trade_side").isin("buy", "sell", "unknown"),
            "invalid_trade_side",
        ),
        (
            F.col("source_payload_encoding").isNull()
            | ~F.col("source_payload_encoding").isin("utf8-json", "base64-protobuf"),
            "invalid_payload_encoding",
        ),
        (
            (F.col("source") == "binance")
            & (F.col("source_payload_encoding") != "utf8-json"),
            "source_payload_encoding_mismatch",
        ),
        (
            (F.col("source") == "mexc")
            & (F.col("source_payload_encoding") != "base64-protobuf"),
            "source_payload_encoding_mismatch",
        ),
        (
            F.col("source_payload").isNull() | (F.length("source_payload") < 2),
            "invalid_source_payload",
        ),
        (
            (F.col("source") == "mexc")
            & ~F.col("source_payload").rlike(BASE64_PATTERN),
            "invalid_source_payload",
        ),
        (
            F.col("kafka_key").isNull()
            | (
                F.col("kafka_key") != F.concat_ws(":", F.col("source"), F.col("symbol"))
            ),
            "kafka_key_mismatch",
        ),
    )

    result = F.lit(None).cast("string")
    for condition, error_code in reversed(rules):
        result = F.when(condition, F.lit(error_code)).otherwise(result)
    return result


def _invalid_decimal(column_name: str) -> Column:
    value = F.col(column_name)
    parsed = value.cast(PRICE_TYPE)
    return (
        value.isNull() | ~value.rlike(DECIMAL_PATTERN) | parsed.isNull() | (parsed <= 0)
    )
