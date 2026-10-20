import sys
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from processing.spark.normalization import normalize_raw_frames
from processing.spark.raw_source import RAW_INPUT_SCHEMA
from processing.spark.validation import valid_trade_records
from tests.processing.decoders.test_normalize import captured


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("raw-normalization-test")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_worker_decoding_expands_frames_and_preserves_error_provenance(
    spark: SparkSession,
) -> None:
    frames = [captured("binance"), captured("mexc")]
    inputs = [
        (
            frame.model_dump_json(),
            "kafka",
            f"raw:0:{i}",
            frame.kafka_key,
            "raw",
            0,
            i,
            None,
        )
        for i, frame in enumerate(frames)
    ]
    inputs.append(("not-json", "kafka", "raw:0:2", None, "raw", 0, 2, None))
    decoded = normalize_raw_frames(spark.createDataFrame(inputs, RAW_INPUT_SCHEMA))
    valid = valid_trade_records(decoded).collect()
    assert len(valid) == 7
    mexc = [row for row in valid if row.source == "mexc"]
    assert {row.frame_event_index for row in mexc} == set(range(6))
    assert {row.offset for row in mexc} == {1}
    assert {row.capture_id for row in mexc} == {frames[1].capture_id}
    invalid = decoded.filter("validation_error IS NOT NULL").collect()
    assert len(invalid) == 1
    assert invalid[0].raw_frame_json == "not-json"
    assert invalid[0].source_location == "raw:0:2"


def test_fixture_reader_boundary_does_not_require_kafka_metadata(
    spark: SparkSession,
) -> None:
    frame = captured("mexc")
    raw = spark.createDataFrame(
        [
            (
                frame.model_dump_json(),
                "fixture",
                "fixture:mexc:1",
                None,
                None,
                None,
                None,
                None,
            )
        ],
        RAW_INPUT_SCHEMA,
    )
    rows = valid_trade_records(normalize_raw_frames(raw)).collect()
    assert len(rows) == 6
    assert all(row.topic is None and row.offset is None for row in rows)
    assert all(row.source_location == "fixture:mexc:1" for row in rows)

