"""Transactional replacement writes for the opt-in candle query."""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from pyspark.sql import DataFrame, Row

from common.config import Settings
from common.contracts.candle import CandleInterval, CandleSnapshot
from common.logging_config import get_logger

LOGGER = get_logger(__name__)
SNAPSHOT_COLUMNS = (
    "source,symbol,interval,window_start_ms,window_end_ms,open,high,low,close,"
    "volume,trade_count,revision,is_final"
)


def candle_query_name(interval: CandleInterval) -> str:
    return f"candles-{interval}-postgres-v1"


@dataclass(frozen=True)
class CandleBatchResult:
    snapshots: int
    applied: int
    unchanged: int
    stale: int


def _verify_history(
    connection: Connection[dict[str, Any]],
    interval: CandleInterval,
    checkpoint_id: UUID,
) -> None:
    connection.execute(
        "INSERT INTO raw_market.candle_query_history "
        "(interval,query_name,checkpoint_id) VALUES (%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        (interval, candle_query_name(interval), checkpoint_id),
    )
    history = connection.execute(
        "SELECT query_name,checkpoint_id FROM raw_market.candle_query_history "
        "WHERE interval=%s FOR UPDATE",
        (interval,),
    ).fetchone()
    if history is None or (
        history["checkpoint_id"] != checkpoint_id
        or history["query_name"] != candle_query_name(interval)
    ):
        raise RuntimeError(
            "Candle checkpoint history differs from database ownership. "
            "Restore matching state or replay into an isolated database."
        )


def _apply_snapshot(
    connection: Connection[dict[str, Any]],
    snapshot: CandleSnapshot,
    checkpoint_id: UUID,
    batch_id: int,
) -> str:
    previous = connection.execute(
        f"SELECT {SNAPSHOT_COLUMNS} FROM raw_market.candles "
        "WHERE source=%s AND symbol=%s AND interval=%s AND window_start_ms=%s",
        (snapshot.source, snapshot.symbol, snapshot.interval, snapshot.window_start_ms),
    ).fetchone()
    if previous is not None:
        old = CandleSnapshot.model_validate(previous)
        if snapshot.revision < old.revision:
            return "stale"
        if snapshot.revision == old.revision:
            if snapshot != old:
                raise ValueError("Conflicting candle snapshots at the same revision")
            return "unchanged"
        if old.is_final:
            raise ValueError("Finalized candle cannot be revised in this history")
        if (
            snapshot.trade_count < old.trade_count
            or snapshot.volume < old.volume
            or snapshot.high < old.high
            or snapshot.low > old.low
        ):
            raise ValueError("Candle revision regresses cumulative aggregates")

    connection.execute(
        f"INSERT INTO raw_market.candles ({SNAPSHOT_COLUMNS},"
        "checkpoint_id,last_batch_id) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (source,symbol,interval,window_start_ms) DO UPDATE SET "
        "window_end_ms=excluded.window_end_ms,open=excluded.open,"
        "high=excluded.high,low=excluded.low,close=excluded.close,"
        "volume=excluded.volume,trade_count=excluded.trade_count,"
        "revision=excluded.revision,is_final=excluded.is_final,"
        "last_batch_id=excluded.last_batch_id,updated_at=now() "
        "WHERE candles.revision < excluded.revision",
        (
            *(getattr(snapshot, name) for name in SNAPSHOT_COLUMNS.split(",")),
            checkpoint_id,
            batch_id,
        ),
    )
    return "applied"


def persist_candle_batch(
    dsn: str,
    checkpoint_id: str,
    batch_id: int,
    rows: Iterable[Row],
    interval: CandleInterval = "1m",
) -> CandleBatchResult | None:
    """Commit replacement snapshots and their retry marker in one transaction.

    Lock ownership per interval so concurrent retries cannot race. A committed
    batch returns None without consuming rows. Reject checkpoint-only resets.
    """
    checkpoint = UUID(checkpoint_id)
    if interval not in ("1m", "5m") or type(batch_id) is not int or batch_id < 0:
        raise ValueError("Unsupported interval or invalid batch ID")
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        _verify_history(connection, interval, checkpoint)
        if connection.execute(
            "SELECT 1 FROM raw_market.candle_batch_runs "
            "WHERE interval=%s AND checkpoint_id=%s AND batch_id=%s",
            (interval, checkpoint, batch_id),
        ).fetchone():
            return None
        counts = {"applied": 0, "unchanged": 0, "stale": 0}
        for row in rows:
            snapshot = CandleSnapshot.model_validate(row.asDict())
            if snapshot.interval != interval:
                raise ValueError("Candle interval differs from writer ownership")
            outcome = _apply_snapshot(connection, snapshot, checkpoint, batch_id)
            counts[outcome] += 1
        total = sum(counts.values())
        connection.execute(
            "INSERT INTO raw_market.candle_batch_runs "
            "(interval,checkpoint_id,batch_id,snapshot_rows,applied_rows,"
            "unchanged_rows,stale_rows) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                interval,
                checkpoint,
                batch_id,
                total,
                counts["applied"],
                counts["unchanged"],
                counts["stale"],
            ),
        )
    return CandleBatchResult(
        total, counts["applied"], counts["unchanged"], counts["stale"]
    )


def write_candle_batch(
    batch: DataFrame, batch_id: int, settings: Settings, interval: CandleInterval = "1m"
) -> None:
    """Persist output, draining committed retries so Spark completes state work."""
    metadata = settings.checkpoint_base_dir / candle_query_name(interval) / "metadata"
    checkpoint_id = str(json.loads(metadata.read_text())["id"])
    rows = batch.toLocalIterator()
    result = persist_candle_batch(
        str(settings.postgres_dsn),
        checkpoint_id,
        batch_id,
        rows,
        interval,
    )
    if result is None:
        for _ in rows:
            pass
    LOGGER.info(
        "candle_postgres_batch interval=%s batch_id=%d result=%s",
        interval,
        batch_id,
        result,
    )
