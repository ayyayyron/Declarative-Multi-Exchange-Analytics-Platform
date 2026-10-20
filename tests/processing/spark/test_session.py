import sys
from unittest.mock import MagicMock, call

import pytest
from pyspark.sql import SparkSession

from common.config import Settings
from processing.spark.session import KAFKA_CONNECTOR, build_spark


def test_session_builder_preserves_runtime_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(_env_file=None)
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    monkeypatch.setattr(SparkSession, "builder", builder)

    session = build_spark(settings, app_name="live-market-postgres")

    assert session is builder.getOrCreate.return_value
    builder.appName.assert_called_once_with("live-market-postgres")
    builder.master.assert_called_once_with(settings.spark_master)
    assert builder.config.call_args_list == [
        call("spark.driver.memory", settings.spark_driver_memory),
        call("spark.pyspark.python", sys.executable),
        call("spark.pyspark.driver.python", sys.executable),
        call("spark.sql.session.timeZone", "UTC"),
        call("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions)),
        call("spark.jars.packages", KAFKA_CONNECTOR),
    ]
