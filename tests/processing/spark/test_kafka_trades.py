import base64
import json
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.contracts import TradeEvent
from processing.spark.postgres_sink import spark_row_to_live_trade
from processing.spark.schemas import KAFKA_RECORD_SCHEMA
from processing.spark.validation import (
    decode_trade_records,
    invalid_trade_records,
    valid_trade_records,
)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("kafka-trade-decoder-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def kafka_frame(
    spark: SparkSession,
    key: str,
    value: str,
    offset: int = 0,
) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                key.encode(),
                value.encode(),
                "raw-trades",
                0,
                offset,
                datetime(2026, 8, 9, tzinfo=UTC),
            )
        ],
        schema=KAFKA_RECORD_SCHEMA,
    )


def binance_trade() -> TradeEvent:
    return TradeEvent(
        event_id="binance:BTCUSDT:123",
        source="binance",
        event_time_ms=1_786_000_000_000,
        trade_time_ms=1_786_000_000_001,
        received_at_ms=1_786_000_000_002,
        symbol="BTCUSDT",
        source_event_id="123",
        price=Decimal("114000.1200000000"),
        quantity=Decimal("0.0010000000"),
        trade_side="buy",
        is_buyer_maker=False,
        source_payload_encoding="utf8-json",
        source_payload="{}",
    )


def test_valid_trade_is_typed_and_keeps_kafka_position(
    spark: SparkSession,
) -> None:
    raw = kafka_frame(
        spark,
        "binance:BTCUSDT",
        binance_trade().model_dump_json(),
        offset=42,
    )

    valid = valid_trade_records(decode_trade_records(raw))
    rows = valid.collect()

    assert len(rows) == 1
    assert rows[0].price == Decimal("114000.1200000000")
    assert rows[0].quantity == Decimal("0.0010000000")
    assert rows[0].partition == 0
    assert rows[0].offset == 42
    trade_time_ms = valid.select(F.unix_millis("trade_time")).first()[0]
    assert trade_time_ms == 1_786_000_000_001


def test_multiple_exchange_symbols_keep_identity_through_serving_rows(
    spark: SparkSession,
) -> None:
    records = []
    for source in ("binance", "mexc"):
        for symbol in ("BTCUSDT", "ETHUSDT"):
            payload = binance_trade().model_dump()
            payload.update(
                source=source, symbol=symbol, event_id=f"{source}:{symbol}:123"
            )
            if source == "mexc":
                payload.update(
                    source_payload_encoding="base64-protobuf",
                    source_payload=base64.b64encode(b"fixture").decode(),
                )
            event = TradeEvent.model_validate(payload)
            records.append(
                (
                    event.kafka_key.encode(),
                    event.model_dump_json().encode(),
                    "raw-trades",
                    0,
                    len(records),
                    datetime(2026, 10, 15, tzinfo=UTC),
                )
            )

    raw = spark.createDataFrame(records, schema=KAFKA_RECORD_SCHEMA)
    rows = valid_trade_records(decode_trade_records(raw)).collect()
    serving = [spark_row_to_live_trade(row) for row in rows]
    assert {(row[1], row[2]) for row in serving} == {
        ("binance", "BTCUSDT"),
        ("binance", "ETHUSDT"),
        ("mexc", "BTCUSDT"),
        ("mexc", "ETHUSDT"),
    }
    assert {row[-1] for row in serving} == {0, 1, 2, 3}
    assert all(row[5] == Decimal("114000.1200000000") for row in serving)


@pytest.mark.parametrize(
    ("key", "value", "expected_error"),
    [
        ("binance:BTCUSDT", "not-json", "malformed_json"),
        (
            "binance:BTCUSDT",
            json.dumps(
                {**json.loads(binance_trade().model_dump_json()), "price": "NaN"}
            ),
            "invalid_price",
        ),
        (
            "mexc:BTCUSDT",
            binance_trade().model_dump_json(),
            "kafka_key_mismatch",
        ),
        (
            "binance:BTCUSDT",
            json.dumps(
                {
                    key: value
                    for key, value in json.loads(
                        binance_trade().model_dump_json()
                    ).items()
                    if key != "schema_version"
                }
            ),
            "unsupported_schema_version",
        ),
    ],
)
def test_invalid_trade_retains_diagnostic_position(
    spark: SparkSession,
    key: str,
    value: str,
    expected_error: str,
) -> None:
    raw = kafka_frame(spark, key, value, offset=9)

    rows = invalid_trade_records(decode_trade_records(raw)).collect()

    assert len(rows) == 1
    assert rows[0].validation_error == expected_error
    assert rows[0].topic == "raw-trades"
    assert rows[0].partition == 0
    assert rows[0].offset == 9
