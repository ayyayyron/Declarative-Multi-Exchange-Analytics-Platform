from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from psycopg import Connection


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


def fetch_latest_trades(
    connection: Connection[Any],
    symbol: str,
) -> dict[str, LatestTrade]:
    rows = connection.execute(
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
        FROM analytics.latest_trades
        WHERE symbol = %s
        ORDER BY source
        """,
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
) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT trade_time, source, price
        FROM (
            SELECT trade_time, source, price, observed_at
            FROM analytics.live_trades
            WHERE symbol = %s
            ORDER BY observed_at DESC
            LIMIT %s
        ) AS recent
        ORDER BY trade_time
        """,
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
) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT trade_time, source, price, quantity, trade_side
        FROM analytics.live_trades
        WHERE symbol = %s
        ORDER BY observed_at DESC
        LIMIT %s
        """,
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


def fetch_pipeline_status(connection: Connection[Any]) -> PipelineStatus | None:
    row = connection.execute("""
        SELECT
            last_successful_batch_at,
            events_processed,
            events_rejected,
            detail
        FROM analytics.pipeline_status
        WHERE component = 'spark-live-market' AND source = 'all'
        """).fetchone()
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
    difference = mexc_price - binance_price
    midpoint = (mexc_price + binance_price) / 2
    spread_bps = Decimal(10_000) * difference / midpoint
    return difference, spread_bps


def age_seconds(timestamp: datetime, now: datetime | None = None) -> float:
    observed_at = now or datetime.now(UTC)
    return max(0.0, (observed_at - timestamp).total_seconds())
