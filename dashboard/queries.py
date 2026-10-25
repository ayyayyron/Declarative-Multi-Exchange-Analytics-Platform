"""Bounded serving-store reads and display arithmetic shared by the dashboard."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from psycopg import Connection, sql

from common.contracts.candle import (
    CANDLE_MILLISECONDS,
    DISPLAY_CANDLE_INTERVALS,
    DisplayCandleInterval,
)


def serving_sql(query: str, schema: str) -> sql.Composed:
    if schema not in ("analytics", "raw_market"):
        raise ValueError("Unsupported serving schema")
    return sql.SQL(query).format(schema=sql.Identifier(schema))


@dataclass(frozen=True)
class LatestTrade:
    source: str
    symbol: str
    event_id: str
    trade_time: datetime
    received_at: datetime
    price: Decimal
    quantity: Decimal
    trade_side: str


@dataclass(frozen=True)
class PipelineStatus:
    last_successful_batch_at: datetime | None
    events_processed: int
    events_rejected: int
    detail: str | None


@dataclass(frozen=True)
class StoredCandle:
    window_start: datetime
    window_end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int
    revision: int | None
    is_final: bool
    updated_at: datetime
    base_windows: int | None = None
    expected_windows: int | None = None
    all_observed_final: bool | None = None

    @property
    def status(self) -> str:
        if self.is_final:
            return "Final"
        if self.all_observed_final:
            return "Partial"
        return "Developing"


def fetch_candles(
    connection: Connection[Any],
    symbol: str,
    source: str,
    limit: int = 200,
    *,
    interval: DisplayCandleInterval = "1m",
) -> list[StoredCandle]:
    """Read bounded, chronological snapshots for one venue, symbol and interval."""
    if interval not in DISPLAY_CANDLE_INTERVALS:
        raise ValueError("Unsupported candle interval")
    if source not in ("binance", "mexc"):
        raise ValueError("Unsupported candle exchange")
    if type(limit) is not int or not 1 <= limit <= 2_000:
        raise ValueError("Candle limit must be between 1 and 2000")
    if interval not in CANDLE_MILLISECONDS:
        rows = connection.execute(
            "SELECT * FROM raw_market.candle_rollups(%s,%s,%s,%s)",
            (source, symbol, interval, limit),
        ).fetchall()
        return [
            StoredCandle(
                window_start=datetime.fromtimestamp(row[0] / 1000, tz=UTC),
                window_end=datetime.fromtimestamp(row[1] / 1000, tz=UTC),
                open=Decimal(row[2]),
                high=Decimal(row[3]),
                low=Decimal(row[4]),
                close=Decimal(row[5]),
                volume=Decimal(row[6]),
                trade_count=int(row[7]),
                revision=None,
                is_final=bool(row[8]),
                updated_at=row[9],
                base_windows=int(row[10]),
                expected_windows=int(row[11]),
                all_observed_final=bool(row[12]),
            )
            for row in rows
        ]
    rows = connection.execute(
        "SELECT window_start_ms,window_end_ms,open,high,low,close,volume,"
        "trade_count,revision,is_final,updated_at FROM ("
        "SELECT * FROM raw_market.candles "
        "WHERE symbol=%s AND source=%s AND interval=%s "
        "ORDER BY window_start_ms DESC LIMIT %s"
        ") AS recent ORDER BY window_start_ms",
        (symbol, source, interval, limit),
    ).fetchall()
    return [
        StoredCandle(
            window_start=datetime.fromtimestamp(row[0] / 1000, tz=UTC),
            window_end=datetime.fromtimestamp(row[1] / 1000, tz=UTC),
            open=Decimal(row[2]),
            high=Decimal(row[3]),
            low=Decimal(row[4]),
            close=Decimal(row[5]),
            volume=Decimal(row[6]),
            trade_count=int(row[7]),
            revision=int(row[8]),
            is_final=bool(row[9]),
            updated_at=row[10],
        )
        for row in rows
    ]


def fetch_latest_trades(
    connection: Connection[Any],
    symbol: str,
    schema: str = "analytics",
) -> dict[str, LatestTrade]:
    """Read the newest stored observation per venue for one selected symbol."""
    rows = connection.execute(
        serving_sql(
            """
        SELECT
            source,
            symbol,
            event_id,
            trade_time,
            received_at,
            price,
            quantity,
            trade_side
        FROM {schema}.latest_trades
        WHERE symbol = %s
        ORDER BY source
        """,
            schema,
        ),
        (symbol,),
    ).fetchall()
    return {
        str(row[0]): LatestTrade(
            source=str(row[0]),
            symbol=str(row[1]),
            event_id=str(row[2]),
            trade_time=row[3],
            received_at=row[4],
            price=Decimal(row[5]),
            quantity=Decimal(row[6]),
            trade_side=str(row[7]),
        )
        for row in rows
    }


def fetch_recent_prices(
    connection: Connection[Any],
    symbol: str,
    limit: int,
    schema: str = "analytics",
) -> list[dict[str, object]]:
    rows = connection.execute(
        serving_sql(
            """
        SELECT trade_time, source, price
        FROM (
            SELECT trade_time, source, price, observed_at
            FROM {schema}.live_trades
            WHERE symbol = %s
            ORDER BY observed_at DESC
            LIMIT %s
        ) AS recent
        ORDER BY trade_time
        """,
            schema,
        ),
        (symbol, limit),
    ).fetchall()
    return [
        {
            "trade_time": row[0],
            "source": str(row[1]).upper(),
            "price": float(row[2]),
        }
        for row in rows
    ]


def fetch_recent_trades(
    connection: Connection[Any],
    symbol: str,
    limit: int = 25,
    schema: str = "analytics",
) -> list[dict[str, object]]:
    rows = connection.execute(
        serving_sql(
            """
        SELECT trade_time, source, price, quantity, trade_side
        FROM {schema}.live_trades
        WHERE symbol = %s
        ORDER BY observed_at DESC
        LIMIT %s
        """,
            schema,
        ),
        (symbol, limit),
    ).fetchall()
    return [
        {
            "Time (UTC)": row[0],
            "Exchange": str(row[1]).upper(),
            "Price (USDT)": float(row[2]),
            "Quantity (BTC)": float(row[3]),
            "Side": str(row[4]).upper(),
        }
        for row in rows
    ]


def fetch_pipeline_status(
    connection: Connection[Any], schema: str = "analytics"
) -> PipelineStatus | None:
    """Read all-symbol processing activity, not selected-symbol feed freshness."""
    row = connection.execute(
        serving_sql(
            """
        SELECT
            last_successful_batch_at,
            events_processed,
            events_rejected,
            detail
        FROM {schema}.pipeline_status
        WHERE component = 'spark-live-market' AND source = 'all'
        """,
            schema,
        )
    ).fetchone()
    if row is None:
        return None
    return PipelineStatus(
        last_successful_batch_at=row[0],
        events_processed=int(row[1]),
        events_rejected=int(row[2]),
        detail=row[3],
    )


def calculate_spread(
    binance_price: Decimal,
    mexc_price: Decimal,
) -> tuple[Decimal, Decimal]:
    """Return the signed MEXC-minus-Binance difference and midpoint-based
    spread in bps.
    """
    difference = mexc_price - binance_price
    midpoint = (mexc_price + binance_price) / 2
    spread_bps = Decimal(10_000) * difference / midpoint
    return difference, spread_bps


def age_seconds(timestamp: datetime, now: datetime | None = None) -> float:
    observed_at = now or datetime.now(UTC)
    return max(0.0, (observed_at - timestamp).total_seconds())
