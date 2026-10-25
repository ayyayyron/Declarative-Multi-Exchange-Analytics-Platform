import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from pyspark.sql import Row

from common.config import Settings
from dashboard.queries import StoredCandle, fetch_candles
from processing.spark.candle_sink import persist_candle_batch

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 for isolated PostgreSQL rollup tests",
)
MONDAY = datetime(2026, 10, 19, tzinfo=UTC)


@pytest.fixture(scope="module")
def rollup_database() -> Iterator[str]:
    original = str(Settings().postgres_dsn)
    name = "candle_rollup_test_" + uuid4().hex
    with psycopg.connect(original, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    isolated = urlunsplit(urlsplit(original)._replace(path=f"/{name}"))
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


@pytest.fixture
def dsn(rollup_database: str) -> str:
    with psycopg.connect(rollup_database) as connection:
        connection.execute("TRUNCATE raw_market.candle_query_history CASCADE")
    return rollup_database


def base(index: int, **changes: object) -> Row:
    start = int(MONDAY.timestamp() * 1000) + index * 300_000
    values = dict(
        source="binance",
        symbol="BTCUSDT",
        interval="5m",
        window_start_ms=start,
        window_end_ms=start + 300_000,
        open=Decimal(100 + index),
        high=Decimal(110 + index),
        low=Decimal(90 + index),
        close=Decimal(105 + index),
        volume=Decimal(index + 1) / 10,
        trade_count=index + 1,
        revision=1,
        is_final=True,
    )
    return Row(**(values | changes))


def read(dsn: str, interval: str, limit: int = 200) -> list[StoredCandle]:
    with psycopg.connect(dsn) as connection:
        connection.execute("SET TIME ZONE 'Asia/Singapore'")
        return fetch_candles(connection, "BTCUSDT", "binance", limit, interval=interval)


@pytest.mark.parametrize(
    "interval,slots", [("15m", 3), ("1h", 12), ("1d", 288), ("1w", 2016)]
)
def test_boundaries_and_open_close_are_utc_and_chronological(
    dsn: str, interval: str, slots: int
) -> None:
    persist_candle_batch(
        dsn, str(uuid4()), 0, [base(slots), base(slots - 1), base(0)], interval="5m"
    )
    rows = read(dsn, interval)
    assert len(rows) == 2
    assert rows[0].window_start == MONDAY
    assert rows[0].window_end == MONDAY + timedelta(minutes=slots * 5)
    assert rows[0].open == Decimal(100)
    assert rows[0].close == Decimal(104 + slots)
    assert rows[1].window_start == rows[0].window_end
    assert rows[0].revision is None
    assert rows[0].base_windows == 2
    assert rows[0].expected_windows == slots
    assert rows[0].status == "Partial"


@pytest.mark.parametrize(
    "interval,slots", [("15m", 3), ("1h", 12), ("1d", 288), ("1w", 2016)]
)
def test_full_base_coverage_is_final_only_when_every_child_is_final(
    dsn: str, interval: str, slots: int
) -> None:
    history = str(uuid4())
    rows = [base(i, is_final=i != slots - 1) for i in range(slots)]
    persist_candle_batch(dsn, history, 0, rows, interval="5m")
    developing = read(dsn, interval)[0]
    assert developing.base_windows == developing.expected_windows == slots
    assert not developing.is_final
    assert developing.status == "Developing"
    persist_candle_batch(dsn, history, 1, [base(slots - 1, revision=2)], interval="5m")
    final = read(dsn, interval)[0]
    assert final.is_final and final.all_observed_final
    assert final.trade_count == slots * (slots + 1) // 2
    assert final.volume == Decimal(slots * (slots + 1)) / 20


def test_revisions_replace_contributions_and_retries_do_not_double_count(
    dsn: str,
) -> None:
    history = str(uuid4())
    persist_candle_batch(
        dsn, history, 0, [base(i, is_final=False) for i in range(3)], interval="5m"
    )
    assert read(dsn, "15m")[0].volume == Decimal("0.6")
    changed = base(
        1,
        revision=2,
        is_final=False,
        volume=Decimal("0.7"),
        trade_count=7,
        high=Decimal("115"),
        low=Decimal("88"),
    )
    persist_candle_batch(dsn, history, 1, [changed], interval="5m")
    persist_candle_batch(dsn, history, 1, [changed], interval="5m")
    row = read(dsn, "15m")[0]
    assert (row.open, row.high, row.low, row.close) == tuple(
        map(Decimal, (100, 115, 88, 107))
    )
    assert (row.volume, row.trade_count) == (Decimal("1.1"), 11)


def test_missing_middle_slot_is_not_filled_or_marked_final(dsn: str) -> None:
    persist_candle_batch(dsn, str(uuid4()), 0, [base(0), base(2)], interval="5m")
    row = read(dsn, "15m")[0]
    assert (row.base_windows, row.expected_windows) == (2, 3)
    assert row.all_observed_final and not row.is_final
    assert row.status == "Partial"
    assert row.volume == Decimal("0.4")


def test_latest_limit_keeps_entire_selected_parent_and_market_isolation(
    dsn: str,
) -> None:
    persist_candle_batch(
        dsn,
        str(uuid4()),
        0,
        [
            *(base(i) for i in range(7)),
            base(0, source="mexc"),
            base(0, symbol="ETHUSDT"),
        ],
        interval="5m",
    )
    persist_candle_batch(
        dsn,
        str(uuid4()),
        0,
        [base(0, interval="1m", window_end_ms=int(MONDAY.timestamp() * 1000) + 60_000)],
        interval="1m",
    )
    latest = read(dsn, "15m", 1)
    assert len(latest) == 1 and latest[0].base_windows == 1
    assert latest[0].window_start == MONDAY + timedelta(minutes=30)
    two = read(dsn, "15m", 2)
    assert two[0].base_windows == 3
    assert two[0].trade_count == 4 + 5 + 6
    assert two[1] == latest[0]


def test_empty_history_returns_no_synthetic_windows(dsn: str) -> None:
    assert read(dsn, "1d") == []


@pytest.mark.parametrize(
    "interval,limit", [("5m", 1), ("bad", 1), ("15m", 0), ("1w", 2001)]
)
def test_sql_function_rejects_invalid_direct_calls(
    dsn: str, interval: str, limit: int
) -> None:
    with (
        psycopg.connect(dsn) as connection,
        pytest.raises(psycopg.errors.InvalidParameterValue),
    ):
        connection.execute(
            "SELECT * FROM raw_market.candle_rollups(%s,%s,%s,%s)",
            ("binance", "BTCUSDT", interval, limit),
        )
