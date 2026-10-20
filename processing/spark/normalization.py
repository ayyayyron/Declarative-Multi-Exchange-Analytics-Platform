"""Spark worker decoding, frame expansion, and canonical validation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from processing.decoders.normalize import normalize_frame
from processing.spark.validation import decode_trade_records

NORMALIZED_FRAME_SCHEMA = StructType(
    [
        StructField("capture_id", StringType()),
        StructField("envelope_source", StringType()),
        StructField("subscription_symbol", StringType()),
        StructField("events", ArrayType(StringType(), containsNull=False), False),
        StructField("decode_error", StringType()),
    ]
)


def normalize_raw_frames(raw_frames: DataFrame) -> DataFrame:
    """Build a lazy decoding, expansion, and canonical-validation plan.

    Keep provenance and errors; cast decimals later on the valid-row path.
    """
    decode = F.udf(normalize_frame, NORMALIZED_FRAME_SCHEMA)
    expanded = (
        raw_frames.withColumn("decoded", decode("frame_json", "source_key"))
        .select("*", "decoded.*")
        .select("*", F.posexplode_outer("events").alias("event_index", "event_json"))
        .select(
            F.concat_ws(":", "envelope_source", "subscription_symbol").alias("key"),
            F.col("event_json").alias("value"),
            "topic",
            "partition",
            "offset",
            "timestamp",
            "source_kind",
            "source_location",
            "capture_id",
            "envelope_source",
            F.coalesce("event_index", F.lit(0)).alias("frame_event_index"),
            F.col("frame_json").alias("raw_frame_json"),
            "decode_error",
        )
    )
    extra = (
        "source_kind",
        "source_location",
        "capture_id",
        "envelope_source",
        "frame_event_index",
        "raw_frame_json",
        "decode_error",
    )
    return decode_trade_records(expanded, extra_columns=extra).withColumn(
        "validation_error", F.coalesce("decode_error", "validation_error")
    )
