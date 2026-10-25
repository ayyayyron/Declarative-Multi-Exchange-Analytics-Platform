"""Commit raw-mode serving rows, quarantine, counters, and batch evidence atomically."""

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import Connection
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row

from common.config import Settings
from common.logging_config import get_logger
from processing.spark.validation import valid_trade_records

LOGGER = get_logger(__name__)
QUERY_NAME = "raw-market-postgres-v1"


def checkpoint_identity(settings: Settings) -> str:
    """Read Spark's persistent query UUID from its managed checkpoint metadata."""
    metadata = settings.checkpoint_base_dir / QUERY_NAME / "metadata"
    return str(json.loads(metadata.read_text())["id"])


def verify_history(connection: Connection[Any], checkpoint_id: str) -> None:
    """Bind and lock this query's checkpoint history within the caller's
    transaction.

    Reject a new checkpoint against existing output rather than reuse old
    batch IDs.
    """
    connection.execute(
        "INSERT INTO raw_market.query_history (query_name, checkpoint_id) "
        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (QUERY_NAME, checkpoint_id),
    )
    history = connection.execute(
        "SELECT checkpoint_id::text FROM raw_market.query_history "
        "WHERE query_name=%s FOR UPDATE",
        (QUERY_NAME,),
    ).fetchone()
    if history is None or history[0] != checkpoint_id:
        raise RuntimeError(
            "Checkpoint history differs from the serving database. "
            "Restore the matching checkpoint or replay into an isolated database; "
            "do not reset only one side."
        )


def stage_events(connection: Connection[Any], rows: Iterable[Row]) -> int:
    """COPY valid event rows and sub-frame provenance into a transaction-local
    staging table.
    """
    connection.execute(
        "CREATE TEMP TABLE raw_trade_batch "
        "(LIKE raw_market.live_trades INCLUDING DEFAULTS) ON COMMIT DROP"
    )
    count = 0
    with connection.cursor().copy(
        "COPY raw_trade_batch (event_id,source,symbol,trade_time,received_at,"
        "price,quantity,trade_side,capture_id,frame_event_index,source_kind,"
        "source_location,kafka_topic,kafka_partition,kafka_offset) FROM STDIN"
    ) as copy:
        for row in rows:
            copy.write_row(
                (
                    row.event_id,
                    row.source,
                    row.symbol,
                    datetime.fromtimestamp(row.trade_time_ms / 1000, tz=UTC),
                    datetime.fromtimestamp(row.received_at_ms / 1000, tz=UTC),
                    row.price,
                    row.quantity,
                    row.trade_side,
                    row.capture_id,
                    row.frame_event_index,
                    row.source_kind,
                    row.source_location,
                    row.topic,
                    row.partition,
                    row.offset,
                )
            )
            count += 1
    return count


def persist_raw_batch(
    dsn: str,
    checkpoint_id: str,
    batch_id: int,
    input_frames: int,
    rows: Iterable[Row],
    rejected: Iterable[Row],
    retention_minutes: int,
) -> tuple[int, int, int] | None:
    """Commit one batch's serving rows, diagnostics, and processing evidence.

    Return (staged, inserted, rejected), or None for a committed retry.
    Writes and counters roll back together on failure.
    """
    with psycopg.connect(dsn) as connection:
        verify_history(connection, checkpoint_id)
        if connection.execute(
            "SELECT 1 FROM raw_market.pipeline_batch_runs "
            "WHERE query_name=%s AND batch_id=%s",
            (QUERY_NAME, batch_id),
        ).fetchone():
            return None
        staged = stage_events(connection, rows)
        _validate_staged_events(connection)
        inserted = connection.execute(
            "INSERT INTO raw_market.live_trades SELECT * FROM raw_trade_batch "
            "ON CONFLICT DO NOTHING"
        ).rowcount
        connection.execute("""
            INSERT INTO raw_market.latest_trades
                (source,symbol,event_id,trade_time,received_at,price,quantity,trade_side)
            SELECT DISTINCT ON (source,symbol)
                source,symbol,event_id,trade_time,received_at,price,quantity,trade_side
            FROM raw_trade_batch
            ORDER BY source,symbol,trade_time DESC,event_id DESC
            ON CONFLICT (source,symbol) DO UPDATE SET
                event_id=excluded.event_id, trade_time=excluded.trade_time,
                received_at=excluded.received_at, price=excluded.price,
                quantity=excluded.quantity, trade_side=excluded.trade_side,
                updated_at=now()
            WHERE (excluded.trade_time,excluded.event_id)
                > (latest_trades.trade_time,latest_trades.event_id)
        """)
        invalid = 0
        for row in rejected:
            connection.execute(
                "INSERT INTO raw_market.rejected_frames "
                "(source_kind,source_location,frame_event_index,capture_id,"
                "raw_frame_json,reason,query_name,batch_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    row.source_kind,
                    row.source_location,
                    row.frame_event_index,
                    row.capture_id,
                    row.raw_frame_json,
                    row.validation_error,
                    QUERY_NAME,
                    batch_id,
                ),
            )
            invalid += 1
        summaries = connection.execute(
            "SELECT source,count(*),max(trade_time),max(received_at) "
            "FROM raw_trade_batch GROUP BY source"
        ).fetchall()
        all_times = connection.execute(
            "SELECT max(trade_time),max(received_at) FROM raw_trade_batch"
        ).fetchone()
        assert all_times is not None
        for source, count, event_time, received_at in [
            *summaries,
            ("all", staged, *all_times),
        ]:
            connection.execute(
                """
                INSERT INTO raw_market.pipeline_status
                    (component,source,status,last_event_time,last_ingest_time,
                     last_successful_batch_at,events_processed,events_rejected,detail)
                VALUES ('spark-live-market',%s,'healthy',%s,%s,now(),%s,%s,%s)
                ON CONFLICT (component,source) DO UPDATE SET
                    last_event_time=excluded.last_event_time,
                    last_ingest_time=excluded.last_ingest_time,
                    last_successful_batch_at=excluded.last_successful_batch_at,
                    events_processed=pipeline_status.events_processed
                        + excluded.events_processed,
                    events_rejected=pipeline_status.events_rejected
                        + excluded.events_rejected,
                    detail=excluded.detail,updated_at=now()
            """,
                (
                    source,
                    event_time,
                    received_at,
                    count,
                    invalid if source == "all" else 0,
                    f"batch_id={batch_id}",
                ),
            )
        connection.execute(
            "INSERT INTO raw_market.pipeline_batch_runs "
            "(query_name,batch_id,input_frames,valid_events,"
            "rejected_rows,inserted_events) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (QUERY_NAME, batch_id, input_frames, staged, invalid, inserted),
        )
        connection.execute(
            "DELETE FROM raw_market.live_trades "
            "WHERE observed_at < now() - make_interval(mins => %s)",
            (retention_minutes,),
        )
        connection.execute(
            "DELETE FROM raw_market.rejected_frames "
            "WHERE observed_at < now() - make_interval(mins => %s)",
            (retention_minutes,),
        )
    return staged, inserted, invalid


def _validate_staged_events(connection: Connection[Any]) -> None:
    """Reject conflicting identities before changing serving rows or counters."""
    conflict = connection.execute("""
        WITH identities AS (
            SELECT event_id,source,symbol,trade_time,price,quantity,trade_side
            FROM raw_trade_batch
            UNION
            SELECT t.event_id,t.source,t.symbol,t.trade_time,
                t.price,t.quantity,t.trade_side
            FROM raw_market.live_trades t
            JOIN raw_trade_batch b ON b.event_id=t.event_id
            UNION
            SELECT t.event_id,t.source,t.symbol,t.trade_time,
                t.price,t.quantity,t.trade_side
            FROM raw_market.latest_trades t
            JOIN raw_trade_batch b ON b.event_id=t.event_id
        )
        SELECT event_id FROM identities
        GROUP BY event_id HAVING count(*) > 1 LIMIT 1
        """).fetchone()
    if conflict is not None:
        raise ValueError(f"Conflicting duplicate trade: {conflict[0]}")
    conflict = connection.execute("""
        WITH locations AS (
            SELECT source_kind,source_location,frame_event_index,event_id
            FROM raw_trade_batch
            UNION
            SELECT t.source_kind,t.source_location,t.frame_event_index,t.event_id
            FROM raw_market.live_trades t
            JOIN raw_trade_batch b
                USING (source_kind,source_location,frame_event_index)
        )
        SELECT source_location FROM locations
        GROUP BY source_kind,source_location,frame_event_index
        HAVING count(*) > 1 LIMIT 1
        """).fetchone()
    if conflict is not None:
        raise ValueError(f"Conflicting source location: {conflict[0]}")


def write_raw_batch(batch: DataFrame, batch_id: int, settings: Settings) -> None:
    """Run on the Spark driver for each micro-batch and stream its rows to
    PostgreSQL.

    Cache reused transformations and release them even when the database
    write fails.
    """
    batch.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        result = persist_raw_batch(
            dsn=str(settings.postgres_dsn),
            checkpoint_id=checkpoint_identity(settings),
            batch_id=batch_id,
            input_frames=batch.select("source_kind", "source_location")
            .distinct()
            .count(),
            rows=valid_trade_records(batch).toLocalIterator(),
            rejected=batch.filter("validation_error IS NOT NULL").toLocalIterator(),
            retention_minutes=int(settings.live_trade_retention_minutes),
        )
        LOGGER.info("raw_postgres_batch batch_id=%d result=%s", batch_id, result)
    finally:
        batch.unpersist()
