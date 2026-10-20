from unittest.mock import MagicMock

import pytest

from common.config import Settings
from processing.spark import candle_pipeline


@pytest.mark.parametrize("available_now", [True, False])
def test_candles_use_raw_input_update_mode_and_separate_checkpoint(
    monkeypatch: pytest.MonkeyPatch, available_now: bool
) -> None:
    settings = Settings(_env_file=None, candle_watermark_delay_seconds=90)
    spark, raw, decoded, valid, candles = (MagicMock() for _ in range(5))
    writer = candles.writeStream
    for operation in ("queryName", "outputMode", "foreachBatch", "option", "trigger"):
        getattr(writer, operation).return_value = writer
    reader = MagicMock(return_value=raw)
    normalizer = MagicMock(return_value=decoded)
    validator = MagicMock(return_value=valid)
    aggregator = MagicMock(return_value=candles)
    monkeypatch.setattr(candle_pipeline, "get_settings", lambda: settings)
    monkeypatch.setattr(candle_pipeline, "build_spark", lambda *a, **kw: spark)
    monkeypatch.setattr(candle_pipeline, "read_raw_frames", reader)
    monkeypatch.setattr(candle_pipeline, "normalize_raw_frames", normalizer)
    monkeypatch.setattr(candle_pipeline, "valid_trade_records", validator)
    monkeypatch.setattr(candle_pipeline, "prototype_candles", aggregator)
    monkeypatch.setattr(candle_pipeline, "await_query_termination", lambda _: None)

    candle_pipeline.run(available_now)

    reader.assert_called_once_with(spark, settings)
    normalizer.assert_called_once_with(raw)
    validator.assert_called_once_with(decoded)
    aggregator.assert_called_once_with(valid, watermark_delay="90 seconds")
    writer.queryName.assert_called_once_with("candles-1m-postgres-v1")
    writer.outputMode.assert_called_once_with("update")
    writer.option.assert_called_once_with(
        "checkpointLocation", "checkpoints/candles-1m-postgres-v1"
    )
    callback = writer.foreachBatch.call_args.args[0]
    assert callback.func is candle_pipeline.write_candle_batch
    assert callback.keywords == {"settings": settings, "interval": "1m"}
    if available_now:
        writer.trigger.assert_called_once_with(availableNow=True)
    else:
        writer.trigger.assert_called_once_with(processingTime="5 seconds")
    spark.stop.assert_called_once()


def test_spark_stops_when_plan_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = MagicMock()
    monkeypatch.setattr(
        candle_pipeline, "get_settings", lambda: Settings(_env_file=None)
    )
    monkeypatch.setattr(candle_pipeline, "build_spark", lambda *a, **kw: spark)
    monkeypatch.setattr(
        candle_pipeline,
        "read_raw_frames",
        MagicMock(side_effect=RuntimeError("source failure")),
    )
    with pytest.raises(RuntimeError, match="source failure"):
        candle_pipeline.run(True)
    spark.stop.assert_called_once()
