from unittest.mock import MagicMock

import pytest

from common.config import Settings
from processing.spark import main


@pytest.mark.parametrize("mode", ["raw", "canonical"])
def test_pipeline_selection_uses_separate_source_and_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    settings = Settings(_env_file=None, serving_mode=mode)
    spark, raw_input, canonical_input = MagicMock(), MagicMock(), MagicMock()
    decoded = MagicMock()
    writer = decoded.writeStream
    for operation in ("queryName", "foreachBatch", "option", "trigger"):
        getattr(writer, operation).return_value = writer
    raw_reader, canonical_reader = MagicMock(return_value=raw_input), MagicMock(
        return_value=canonical_input
    )
    raw_normalize = MagicMock(return_value=decoded)
    canonical_decode = MagicMock(return_value=decoded)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "build_spark", lambda *args, **kwargs: spark)
    monkeypatch.setattr(main, "read_raw_frames", raw_reader)
    monkeypatch.setattr(main, "read_trade_stream", canonical_reader)
    monkeypatch.setattr(main, "normalize_raw_frames", raw_normalize)
    monkeypatch.setattr(main, "decode_trade_records", canonical_decode)
    monkeypatch.setattr(main, "await_query_termination", lambda _: None)
    main.run(True)
    name = "raw-market-postgres-v1" if mode == "raw" else "live-market-postgres"
    writer.queryName.assert_called_once_with(name)
    writer.option.assert_called_once_with(
        "checkpointLocation", str(settings.checkpoint_base_dir / name)
    )
    assert raw_reader.call_count == raw_normalize.call_count == (mode == "raw")
    assert (
        canonical_reader.call_count
        == canonical_decode.call_count
        == (mode == "canonical")
    )
    writer.trigger.assert_called_once_with(availableNow=True)
    spark.stop.assert_called_once()
