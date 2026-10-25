"""Presentation-only candle charts; all aggregates come from PostgreSQL."""

from collections.abc import Sequence
from typing import Literal, cast

import altair as alt
import pandas as pd

from dashboard.queries import StoredCandle


def candle_chart(candles: Sequence[StoredCandle]) -> tuple[alt.LayerChart, alt.Chart]:
    """Draw responsive price/volume panels with the same explicit UTC domain."""
    duration = (candles[0].window_end - candles[0].window_start).total_seconds()
    tick_unit: Literal["minute", "hour", "day", "week"] = (
        "week"
        if duration >= 604800
        else "day" if duration >= 86400 else "hour" if duration >= 3600 else "minute"
    )
    frame = pd.DataFrame(
        [
            {
                "window_center": candle.window_start
                + (candle.window_end - candle.window_start) / 2,
                "start_utc": candle.window_start.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "end_utc": candle.window_end.strftime("%Y-%m-%d %H:%M:%S UTC"),
                **{
                    name: float(getattr(candle, name))
                    for name in ("open", "high", "low", "close", "volume")
                },
                "trade_count": candle.trade_count,
                "status": candle.status,
            }
            for candle in candles
        ]
    )
    base = alt.Chart(frame).encode(
        x=alt.X(
            "window_center:T",
            title="Candle interval (UTC)",
            scale=alt.Scale(
                type="utc",
                nice=False,
                domain=[candles[0].window_start, candles[-1].window_end],
            ),
            axis=alt.Axis(format="%b %d %H:%M", tickCount=tick_unit, labelOverlap=True),
        ),
        color=alt.condition(
            alt.datum.close >= alt.datum.open,
            alt.value("#16876D"),
            alt.value("#D64B4B"),
        ),
        tooltip=[
            alt.Tooltip("start_utc:N", title="Start (inclusive)"),
            alt.Tooltip("end_utc:N", title="End (exclusive)"),
            *[
                alt.Tooltip(f"{name}:Q", title=name.capitalize(), format=",.8f")
                for name in ("open", "high", "low", "close", "volume")
            ],
            alt.Tooltip("trade_count:Q", title="Trades", format=","),
            alt.Tooltip("status:N", title="Status"),
        ],
    )
    price = alt.Y("low:Q", title="Price (USDT)", scale=alt.Scale(zero=False))
    wick = base.mark_rule().encode(y=price, y2="high:Q")
    body = base.mark_bar(size=12).encode(
        y=alt.Y("open:Q", title="Price (USDT)", scale=alt.Scale(zero=False)),
        y2="close:Q",
    )
    doji = (
        base.transform_filter(alt.datum.open == alt.datum.close)
        .mark_tick(orient="horizontal", size=12)
        .encode(y=alt.Y("close:Q", scale=alt.Scale(zero=False)))
    )
    volume = (
        base.mark_bar(size=12)
        .encode(y=alt.Y("volume:Q", title="Volume (base asset)"))
        .properties(height=200)
        .configure_axisY(minExtent=80)
    )
    return (
        cast(
            alt.LayerChart,
            (wick + body + doji).properties(height=300).configure_axisY(minExtent=80),
        ),
        volume,
    )


def candle_table(candles: Sequence[StoredCandle]) -> pd.DataFrame:
    """Keep exact stored decimals and explicit UTC strings in the inspection table."""
    return pd.DataFrame(
        [
            {
                "Start (UTC)": c.window_start.strftime("%Y-%m-%d %H:%M:%S"),
                "End (UTC, exclusive)": c.window_end.strftime("%Y-%m-%d %H:%M:%S"),
                "Open": str(c.open),
                "High": str(c.high),
                "Low": str(c.low),
                "Close": str(c.close),
                "Volume (base asset)": str(c.volume),
                "Trades": c.trade_count,
                "Revision": c.revision if c.revision is not None else "Computed",
                "Status": c.status,
                **(
                    {"Observed 5m windows": f"{c.base_windows}/{c.expected_windows}"}
                    if c.base_windows is not None
                    else {}
                ),
            }
            for c in reversed(candles)
        ]
    )
