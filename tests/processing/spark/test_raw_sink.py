import os
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pyspark.sql import Row

from common.config import Settings
from processing.spark.raw_sink import persist_raw_batch

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 for an isolated local PostgreSQL database",
)


@pytest.fixture(scope="module")
def dsn() -> Iterator[str]:
    original = str(Settings().postgres_dsn)
    name = "raw_refactor_test_" + uuid4().hex
    with psycopg.connect(original, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    params = conninfo_to_dict(original)
    params["dbname"] = name
    isolated = make_conninfo(**params)
    try:
        with psycopg.connect(isolated) as connection:
            for path in sorted(Path("migrations").glob("*.sql")):
                connection.execute(path.read_text())
        yield isolated
    finally:
        with psycopg.connect(original, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


def rows(location: str = "raw:0:1") -> list[Row]:
    return [
        Row(
            event_id=f"mexc:BTCUSDT:{index}",
            source="mexc",
            symbol="BTCUSDT",
            trade_time_ms=1_785_250_729_250 + index,
            received_at_ms=1_785_250_729_300,
            price=Decimal("60000.25"),
            quantity=Decimal("0.01"),
            trade_side="buy",
            capture_id="a" * 32,
            frame_event_index=index,
            source_kind="kafka",
            source_location=location,
            topic="raw",
            partition=0,
            offset=1,
        )
        for index in range(6)
    ]


def test_transactions_retries_one_to_many_and_history_guard(dsn: str) -> None:
    history = str(uuid4())

    def interrupted() -> Iterator[Row]:
        yield rows()[0]
        raise RuntimeError("simulate interruption during staging")

    with pytest.raises(RuntimeError, match="interruption"):
        persist_raw_batch(dsn, history, 0, 1, interrupted(), [], 60)
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM raw_market.query_history"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM raw_market.live_trades"
        ).fetchone() == (0,)

    invalid = Row(
        source_kind="kafka",
        source_location="raw:0:2",
        frame_event_index=0,
        capture_id=None,
        raw_frame_json="not-json",
        validation_error="invalid_envelope",
    )
    assert persist_raw_batch(dsn, history, 0, 2, rows(), [invalid], 60) == (6, 6, 1)
    assert persist_raw_batch(dsn, history, 0, 2, rows(), [invalid], 60) is None
    assert persist_raw_batch(dsn, history, 1, 1, rows("raw:0:3"), [], 60) == (6, 0, 0)
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM raw_market.live_trades"
        ).fetchone() == (6,)
        assert connection.execute(
            "SELECT count(*) FROM raw_market.rejected_frames"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT events_processed,events_rejected FROM raw_market.pipeline_status "
            "WHERE source='all'"
        ).fetchone() == (12, 1)
        assert connection.execute(
            "SELECT event_id FROM raw_market.latest_trades"
        ).fetchone() == ("mexc:BTCUSDT:5",)
    with pytest.raises(RuntimeError, match="Checkpoint history differs"):
        persist_raw_batch(dsn, str(uuid4()), 0, 1, rows(), [], 60)
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM raw_market.pipeline_batch_runs"
        ).fetchone() == (2,)
