from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import Connection
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row

from common.config import Settings
from common.logging_config import get_logger
from streaming.kafka_trades import invalid_trade_records, valid_trade_records

LOGGER = get_logger(__name__)
QUERY_NAME = "live-market-postgres"
LiveTradeRow = tuple[
    str,
    str,
    str,
    datetime,
    datetime,
    Decimal,
    Decimal,
    str,
    str,
    int,
    int,
]


@dataclass(frozen=True)
class SinkResult:
    staged_rows: int
    inserted_rows: int


def write_live_batch(
    decoded_batch: DataFrame,
    batch_id: int,
    settings: Settings,
) -> None:
    dsn = str(settings.postgres_dsn)
    if batch_is_committed(dsn, batch_id):
        LOGGER.info(
            "postgres_batch_skipped batch_id=%d reason=already_committed", batch_id
        )
        return

    decoded_batch.persist(StorageLevel.MEMORY_AND_DISK)
    valid = valid_trade_records(decoded_batch).select(
        "event_id",
        "source",
        "symbol",
        "trade_time_ms",
        "received_at_ms",
        "price",
        "quantity",
        "trade_side",
        "topic",
        "partition",
        "offset",
    )
    valid.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        input_rows = decoded_batch.count()
        invalid_rows = invalid_trade_records(decoded_batch).count()
        result = persist_live_rows(
            dsn=dsn,
            batch_id=batch_id,
            input_rows=input_rows,
            invalid_rows=invalid_rows,
            retention_minutes=int(settings.live_trade_retention_minutes),
            rows=(spark_row_to_live_trade(row) for row in valid.toLocalIterator()),
        )
        LOGGER.info(
            "postgres_batch_committed batch_id=%d input=%d valid=%d invalid=%d "
            "inserted=%d",
            batch_id,
            input_rows,
            result.staged_rows,
            invalid_rows,
            result.inserted_rows,
        )
    finally:
        valid.unpersist()
        decoded_batch.unpersist()


def batch_is_committed(dsn: str, batch_id: int) -> bool:
    with psycopg.connect(dsn, autocommit=True) as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM analytics.pipeline_batch_runs
            WHERE query_name = %s AND batch_id = %s AND status = 'committed'
            """,
            (QUERY_NAME, batch_id),
        ).fetchone()
    return row is not None


def persist_live_rows(
    dsn: str,
    batch_id: int,
    input_rows: int,
    invalid_rows: int,
    retention_minutes: int,
    rows: Iterable[LiveTradeRow],
) -> SinkResult:
    with psycopg.connect(dsn, autocommit=True) as connection:
        with connection.transaction():
            staged_rows = copy_staging_rows(connection, rows)
            inserted_rows = merge_live_rows(connection)
            update_pipeline_status(
                connection,
                batch_id=batch_id,
                valid_rows=staged_rows,
                invalid_rows=invalid_rows,
            )
            connection.execute(
                """
                INSERT INTO analytics.pipeline_batch_runs (
                    query_name,
                    batch_id,
                    started_at,
                    committed_at,
                    input_rows,
                    output_rows,
                    status
                )
                VALUES (%s, %s, now(), now(), %s, %s, 'committed')
                ON CONFLICT (query_name, batch_id) DO NOTHING
                """,
                (QUERY_NAME, batch_id, input_rows, staged_rows),
            )
            connection.execute(
                """
                DELETE FROM analytics.live_trades
                WHERE observed_at < now() - make_interval(mins => %s)
                """,
                (retention_minutes,),
            )
    return SinkResult(staged_rows=staged_rows, inserted_rows=inserted_rows)


def copy_staging_rows(
    connection: Connection[Any],
    rows: Iterable[LiveTradeRow],
) -> int:
    connection.execute("""
        CREATE TEMP TABLE live_trade_batch
        (LIKE analytics.live_trades INCLUDING DEFAULTS)
        ON COMMIT DROP
        """)
    row_count = 0
    with connection.cursor().copy("""
        COPY live_trade_batch (
            event_id,
            source,
            symbol,
            trade_time,
            received_at,
            price,
            quantity,
            trade_side,
            kafka_topic,
            kafka_partition,
            kafka_offset
        ) FROM STDIN
        """) as copy:
        for row in rows:
            copy.write_row(row)
            row_count += 1
    return row_count


def merge_live_rows(connection: Connection[Any]) -> int:
    cursor = connection.execute("""
        INSERT INTO analytics.live_trades (
            event_id,
            source,
            symbol,
            trade_time,
            received_at,
            price,
            quantity,
            trade_side,
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        SELECT
            event_id,
            source,
            symbol,
            trade_time,
            received_at,
            price,
            quantity,
            trade_side,
            kafka_topic,
            kafka_partition,
            kafka_offset
        FROM live_trade_batch
        ON CONFLICT DO NOTHING
        """)
    connection.execute("""
        INSERT INTO analytics.latest_trades (
            source,
            symbol,
            event_id,
            trade_time,
            received_at,
            price,
            quantity,
            trade_side
        )
        SELECT DISTINCT ON (source, symbol)
            source,
            symbol,
            event_id,
            trade_time,
            received_at,
            price,
            quantity,
            trade_side
        FROM live_trade_batch
        ORDER BY source, symbol, trade_time DESC, event_id DESC
        ON CONFLICT (source, symbol) DO UPDATE SET
            event_id = excluded.event_id,
            trade_time = excluded.trade_time,
            received_at = excluded.received_at,
            price = excluded.price,
            quantity = excluded.quantity,
            trade_side = excluded.trade_side,
            updated_at = now()
        WHERE (excluded.trade_time, excluded.event_id)
            > (latest_trades.trade_time, latest_trades.event_id)
        """)
    return cursor.rowcount


def update_pipeline_status(
    connection: Connection[Any],
    batch_id: int,
    valid_rows: int,
    invalid_rows: int,
) -> None:
    connection.execute(
        """
        INSERT INTO analytics.pipeline_status (
            component,
            source,
            status,
            last_event_time,
            last_ingest_time,
            last_successful_batch_at,
            events_processed,
            detail
        )
        SELECT
            'spark-live-market',
            source,
            'healthy',
            max(trade_time),
            max(received_at),
            now(),
            count(*),
            %s
        FROM live_trade_batch
        GROUP BY source
        ON CONFLICT (component, source) DO UPDATE SET
            status = excluded.status,
            last_event_time = excluded.last_event_time,
            last_ingest_time = excluded.last_ingest_time,
            last_successful_batch_at = excluded.last_successful_batch_at,
            events_processed = pipeline_status.events_processed
                + excluded.events_processed,
            detail = excluded.detail,
            updated_at = now()
        """,
        (f"batch_id={batch_id}",),
    )
    connection.execute(
        """
        INSERT INTO analytics.pipeline_status (
            component,
            source,
            status,
            last_event_time,
            last_ingest_time,
            last_successful_batch_at,
            events_processed,
            events_rejected,
            detail
        )
        SELECT
            'spark-live-market',
            'all',
            'healthy',
            max(trade_time),
            max(received_at),
            now(),
            %s,
            %s,
            %s
        FROM live_trade_batch
        ON CONFLICT (component, source) DO UPDATE SET
            status = excluded.status,
            last_event_time = excluded.last_event_time,
            last_ingest_time = excluded.last_ingest_time,
            last_successful_batch_at = excluded.last_successful_batch_at,
            events_processed = pipeline_status.events_processed
                + excluded.events_processed,
            events_rejected = pipeline_status.events_rejected
                + excluded.events_rejected,
            detail = excluded.detail,
            updated_at = now()
        """,
        (valid_rows, invalid_rows, f"batch_id={batch_id}"),
    )


def spark_row_to_live_trade(row: Row) -> LiveTradeRow:
    return (
        str(row.event_id),
        str(row.source),
        str(row.symbol),
        datetime.fromtimestamp(int(row.trade_time_ms) / 1000, tz=UTC),
        datetime.fromtimestamp(int(row.received_at_ms) / 1000, tz=UTC),
        Decimal(row.price),
        Decimal(row.quantity),
        str(row.trade_side),
        str(row.topic),
        int(row.partition),
        int(row.offset),
    )
