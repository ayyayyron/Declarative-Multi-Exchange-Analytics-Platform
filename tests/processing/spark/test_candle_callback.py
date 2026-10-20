import json
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

from pyspark.sql import DataFrame, Row

from common.config import Settings
from processing.spark import candle_sink


def test_committed_retry_still_consumes_spark_output(
    tmp_path: Path, monkeypatch
) -> None:
    settings = Settings(checkpoint_base_dir=tmp_path)
    folder = tmp_path / candle_sink.candle_query_name("1m")
    folder.mkdir()
    history = str(uuid4())
    (folder / "metadata").write_text(json.dumps({"id": history}))
    consumed = []

    def output():
        for index in range(3):
            consumed.append(index)
            yield Row(revision=index + 1)

    batch = Mock(spec=DataFrame)
    batch.toLocalIterator.return_value = output()
    persist = Mock(return_value=None)
    monkeypatch.setattr(candle_sink, "persist_candle_batch", persist)
    candle_sink.write_candle_batch(batch, 7, settings)
    assert consumed == [0, 1, 2]
    assert persist.call_args.args[1:3] == (history, 7)
    batch.toLocalIterator.assert_called_once()
