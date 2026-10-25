"""Deduplication and OHLCV in one interval-specific stateful operator.

Retains accepted event IDs until each candle expires, so state grows with
trade volume. Runs separately from the trade-serving query.
"""

import json
from collections.abc import Iterable, Iterator
from decimal import Decimal, localcontext
from functools import partial
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from common.contracts.candle import CANDLE_MILLISECONDS, CandleInterval

OUTPUT_SCHEMA = (
    "source string, symbol string, interval string, window_start_ms long, "
    "window_end_ms long, open decimal(28,10), high decimal(28,10), "
    "low decimal(28,10), close decimal(28,10), volume decimal(38,10), "
    "trade_count long, revision long, is_final boolean"
)


def _snapshot(
    key: tuple[Any, ...],
    candle: dict[str, Any],
    final: bool,
    interval: CandleInterval = "1m",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source": str(key[0]),
                "symbol": str(key[1]),
                "interval": interval,
                "window_start_ms": int(key[2]),
                "window_end_ms": int(key[2]) + CANDLE_MILLISECONDS[interval],
                **{
                    name: Decimal(candle[name])
                    for name in ("open", "high", "low", "close", "volume")
                },
                "trade_count": len(candle["seen"]),
                "revision": candle["revision"],
                "is_final": final,
            }
        ]
    )


def _update_candle(
    key: tuple[Any, ...],
    batches: Iterable[pd.DataFrame],
    state: GroupState,
    *,
    interval: CandleInterval = "1m",
) -> Iterator[pd.DataFrame]:
    if state.hasTimedOut:
        candle = json.loads(state.get[0])
        candle["revision"] += 1
        yield _snapshot(key, candle, final=True, interval=interval)
        state.remove()
        return

    end_ms = int(key[2]) + CANDLE_MILLISECONDS[interval]
    if end_ms <= state.getCurrentWatermarkMs():
        return

    candle = json.loads(state.get[0]) if state.exists else {"seen": {}, "revision": 0}
    changed = False
    with localcontext() as context:
        context.prec = 50
        for batch in batches:
            for row in batch.itertuples(index=False):
                event_id = str(row.event_id)
                time_ms = int(str(row.trade_time_ms))
                price, quantity = Decimal(str(row.price)), Decimal(str(row.quantity))
                signature = [time_ms, str(price), str(quantity)]
                if event_id in candle["seen"]:
                    if candle["seen"][event_id] != signature:
                        raise ValueError(f"Conflicting duplicate trade: {event_id}")
                    continue

                order = [time_ms, event_id]
                if not candle["seen"]:
                    candle.update(
                        open=str(price),
                        high=str(price),
                        low=str(price),
                        close=str(price),
                        volume="0",
                        first=order,
                        last=order,
                    )
                if order < candle["first"]:
                    candle["first"], candle["open"] = order, str(price)
                if order > candle["last"]:
                    candle["last"], candle["close"] = order, str(price)
                candle["high"] = str(max(Decimal(candle["high"]), price))
                candle["low"] = str(min(Decimal(candle["low"]), price))
                candle["volume"] = str(Decimal(candle["volume"]) + quantity)
                candle["seen"][event_id] = signature
                changed = True

    if changed:
        candle["revision"] += 1
        state.update((json.dumps(candle),))
        yield _snapshot(key, candle, final=False, interval=interval)
    if state.exists:
        state.setTimeoutTimestamp(end_ms)


def prototype_candles(
    trades: DataFrame,
    watermark_delay: str = "2 minutes",
    *,
    interval: CandleInterval = "1m",
) -> DataFrame:
    """Build an experimental streaming plan over valid typed canonical trades.

    Each interval needs its own query and checkpoint. Identity is scoped to
    venue, symbol and window; repeated IDs must keep time, price and quantity.
    Cross-window conflicts and permanent deduplication are outside this contract.
    """
    if interval not in CANDLE_MILLISECONDS:
        raise ValueError("Supported candle intervals are 1m and 5m")
    if not trades.isStreaming:
        raise ValueError("The prototype requires a streaming DataFrame")
    duration = CANDLE_MILLISECONDS[interval]
    prepared = (
        trades.withWatermark("trade_time", watermark_delay)
        .withColumn("trade_time_ms", F.unix_millis("trade_time"))
        .withColumn(
            "window_start_ms", F.expr(f"(trade_time_ms DIV {duration}) * {duration}")
        )
        .select(
            "source",
            "symbol",
            "event_id",
            "trade_time",
            "trade_time_ms",
            "window_start_ms",
            "price",
            "quantity",
        )
    )
    return prepared.groupBy(
        "source", "symbol", "window_start_ms"
    ).applyInPandasWithState(
        (
            _update_candle
            if interval == "1m"
            else partial(_update_candle, interval=interval)
        ),
        outputStructType=OUTPUT_SCHEMA,
        stateStructType="candle_json string",
        outputMode="Update",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )
