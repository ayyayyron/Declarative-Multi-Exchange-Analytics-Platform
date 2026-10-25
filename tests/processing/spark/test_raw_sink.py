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


@pytest.fixture
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


def serving_state(dsn: str) -> list[list[tuple[object, ...]]]:
    with psycopg.connect(dsn) as connection:
        return [
            connection.execute(
                sql.SQL(
                    "SELECT to_jsonb(t) FROM raw_market.{} t ORDER BY to_jsonb(t)::text"
                ).format(sql.Identifier(table))
            ).fetchall()
            for table in (
                "live_trades",
                "latest_trades",
                "pipeline_status",
                "pipeline_batch_runs",
                "query_history",
                "rejected_frames",
            )
        ]


@pytest.mark.parametrize("phase", ["same_batch", "later_batch", "after_retention"])
@pytest.mark.parametrize(
    "changes",
    [
        {"price": Decimal("70000")},
        {"quantity": Decimal("0.02")},
        {"trade_time_ms": 1_785_250_730_250},
    ],
)
def test_conflicting_identity_rolls_back_every_serving_change(
    dsn: str, phase: str, changes: dict[str, object]
) -> None:
    history = str(uuid4())
    original = rows()[0]
    changed = Row(**(original.asDict() | changes | {"source_location": "raw:0:2"}))
    if phase != "same_batch":
        persist_raw_batch(dsn, history, 0, 1, [original], [], 60)
        if phase == "after_retention":
            with psycopg.connect(dsn) as connection:
                connection.execute("DELETE FROM raw_market.live_trades")
    before = serving_state(dsn)
    batch_id = 0 if phase == "same_batch" else 1
    incoming = [original, changed] if phase == "same_batch" else [changed]
    with pytest.raises(ValueError, match="Conflicting duplicate trade"):
        persist_raw_batch(dsn, history, batch_id, 2, incoming, [], 60)
    assert serving_state(dsn) == before


def test_exact_duplicate_payload_with_new_capture_is_safe(dsn: str) -> None:
    history = str(uuid4())
    original = rows()[0]
    duplicate = Row(
        **(
            original.asDict()
            | {
                "source_location": "raw:0:2",
                "capture_id": "b" * 32,
                "received_at_ms": original.received_at_ms + 1000,
            }
        )
    )
    assert persist_raw_batch(dsn, history, 0, 2, [original, duplicate], [], 60) == (
        2,
        1,
        0,
    )
    assert persist_raw_batch(dsn, history, 1, 1, [duplicate], [], 60) == (1, 0, 0)
    before = serving_state(dsn)
    assert persist_raw_batch(dsn, history, 1, 1, [duplicate], [], 60) is None
    assert serving_state(dsn) == before
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT l.price=t.price FROM raw_market.latest_trades l "
            "JOIN raw_market.live_trades t USING(event_id)"
        ).fetchone() == (True,)


@pytest.mark.parametrize("same_batch", [False, True])
def test_source_location_cannot_be_reused_for_another_identity(
    dsn: str, same_batch: bool
) -> None:
    history = str(uuid4())
    original = rows()[0]
    other = Row(**(original.asDict() | {"event_id": "mexc:BTCUSDT:other"}))
    if not same_batch:
        persist_raw_batch(dsn, history, 0, 1, [original], [], 60)
    before = serving_state(dsn)
    with pytest.raises(ValueError, match="Conflicting source location"):
        persist_raw_batch(
            dsn,
            history,
            0 if same_batch else 1,
            1,
            [original, other] if same_batch else [other],
            [],
            60,
        )
    assert serving_state(dsn) == before
