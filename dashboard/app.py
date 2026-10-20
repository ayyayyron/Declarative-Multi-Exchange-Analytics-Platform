"""Render PostgreSQL serving results; this page does not consume Kafka directly."""

from datetime import UTC
from typing import Any

import altair as alt
import pandas as pd
import psycopg
import streamlit as st
from psycopg_pool import ConnectionPool, PoolTimeout

from common.config import get_settings
from dashboard.charts import candle_chart, candle_table
from dashboard.queries import (
    LatestTrade,
    age_seconds,
    calculate_spread,
    fetch_candles,
    fetch_latest_trades,
    fetch_pipeline_status,
    fetch_recent_prices,
    fetch_recent_trades,
)

settings = get_settings()
st.set_page_config(
    page_title="Multi-Exchange Market Monitor",
    page_icon="↗",
    layout="wide",
)


@st.cache_resource
def database_pool(dsn: str) -> ConnectionPool[Any]:
    return ConnectionPool(conninfo=dsn, min_size=1, max_size=4, open=True)


def health_label(trade: LatestTrade | None) -> str:
    if trade is None:
        return "WAITING"
    if age_seconds(trade.trade_time) > settings.dashboard_stale_after_seconds:
        return "STALE"
    return "LIVE"


def price_label(trade: LatestTrade | None) -> str:
    if trade is None:
        return "—"
    return f"${trade.price:,.2f}"


@st.fragment(run_every=f"{settings.dashboard_refresh_seconds}s")
def live_market() -> None:
    """Refresh the selected symbol's observations and pipeline-wide processing
    status.
    """
    schema = "raw_market" if settings.serving_mode == "raw" else "analytics"
    try:
        with database_pool(str(settings.postgres_dsn)).connection() as connection:
            latest = fetch_latest_trades(connection, settings.dashboard_symbol, schema)
            prices = fetch_recent_prices(
                connection,
                settings.dashboard_symbol,
                int(settings.dashboard_chart_points),
                schema,
            )
            trades = fetch_recent_trades(
                connection, settings.dashboard_symbol, schema=schema
            )
            pipeline = fetch_pipeline_status(connection, schema)
    except (psycopg.Error, PoolTimeout) as error:
        st.error(f"PostgreSQL is unavailable: {type(error).__name__}")
        return

    binance = latest.get("binance")
    mexc = latest.get("mexc")
    binance_state = health_label(binance)
    mexc_state = health_label(mexc)
    pipeline_age = (
        age_seconds(pipeline.last_successful_batch_at)
        if pipeline and pipeline.last_successful_batch_at
        else None
    )
    pipeline_state = (
        "LIVE"
        if pipeline_age is not None
        and pipeline_age <= settings.dashboard_stale_after_seconds
        else "STALE"
    )

    status_columns = st.columns(3)
    status_columns[0].metric("Binance feed", binance_state)
    status_columns[1].metric("MEXC feed", mexc_state)
    status_columns[2].metric("Kafka → Spark → PostgreSQL", pipeline_state)

    if "STALE" in {binance_state, mexc_state, pipeline_state}:
        st.warning(
            "At least one source is stale. Displayed prices are observations, "
            "not executable quotes."
        )

    metric_columns = st.columns(4)
    metric_columns[0].metric("Binance", price_label(binance))
    metric_columns[1].metric("MEXC", price_label(mexc))
    if binance and mexc:
        difference, spread_bps = calculate_spread(binance.price, mexc.price)
        metric_columns[2].metric("MEXC − Binance", f"${difference:,.2f}")
        metric_columns[3].metric("Symmetric spread", f"{spread_bps:,.2f} bps")
    else:
        metric_columns[2].metric("MEXC − Binance", "—")
        metric_columns[3].metric("Symmetric spread", "—")

    st.subheader("Live trade prices")
    if prices:
        price_frame = pd.DataFrame(prices)
        chart = (
            alt.Chart(price_frame)
            .mark_line()
            .encode(
                x=alt.X(
                    "trade_time:T",
                    title="Trade time (UTC)",
                    scale=alt.Scale(type="utc"),
                    axis=alt.Axis(format="%H:%M:%S"),
                ),
                y=alt.Y(
                    "price:Q",
                    title="Price (USDT)",
                    scale=alt.Scale(zero=False),
                ),
                color=alt.Color(
                    "source:N",
                    title="Exchange",
                    scale=alt.Scale(
                        domain=["BINANCE", "MEXC"],
                        range=["#5B8FF9", "#E8684A"],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("source:N", title="Exchange"),
                    alt.Tooltip(
                        "trade_time:T",
                        title="Trade time",
                        format="%Y-%m-%d %H:%M:%S.%L UTC",
                    ),
                    alt.Tooltip("price:Q", title="Price", format=",.2f"),
                ],
            )
            .properties(height=390)
            .interactive()
        )
        st.altair_chart(chart, width="stretch")
    else:
        st.info("No trades have reached PostgreSQL yet.")

    left, right = st.columns((2, 1))
    with left:
        st.subheader("Recent trades")
        st.dataframe(
            pd.DataFrame(trades),
            hide_index=True,
            width="stretch",
        )
    with right:
        st.subheader("Pipeline")
        if pipeline:
            st.metric("Trades processed", f"{pipeline.events_processed:,}")
            st.metric("Records rejected", f"{pipeline.events_rejected:,}")
            st.caption(f"Last committed {pipeline.detail or 'batch'}")
        else:
            st.info("Waiting for the first Spark batch.")


@st.fragment(run_every=f"{settings.dashboard_refresh_seconds}s")
def candle_market() -> None:
    """Refresh persisted candles without coupling them to trade-pipeline health."""
    st.divider()
    st.subheader("1-minute candles")
    exchange = st.selectbox(
        "Candle exchange", ["Binance", "MEXC"], key="candle_exchange"
    )
    try:
        with database_pool(str(settings.postgres_dsn)).connection() as connection:
            candles = fetch_candles(
                connection, settings.dashboard_symbol, exchange.lower()
            )
    except psycopg.errors.UndefinedTable:
        st.info(
            "Candle tables are not installed. Run make db-migrate, then make candles."
        )
        return
    except (psycopg.Error, PoolTimeout) as error:
        st.error(f"Candle data is unavailable: {type(error).__name__}")
        return
    if not candles:
        st.info(
            f"No 1-minute candles for {exchange} {settings.dashboard_symbol}. "
            "Run ingestion and make candles to populate this view."
        )
        return

    latest = candles[-1]
    metrics = st.columns(4)
    metrics[0].metric("Candle close (USDT)", f"{latest.close:,.2f}")
    metrics[1].metric("Candle volume (base asset)", f"{latest.volume:,.10f}")
    metrics[2].metric("Candle trades", f"{latest.trade_count:,}")
    metrics[3].metric("Candle status", "FINAL" if latest.is_final else "DEVELOPING")
    last_write = latest.updated_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    st.caption(
        f"Latest interval: [{latest.window_start:%Y-%m-%d %H:%M:%S}, "
        f"{latest.window_end:%H:%M:%S}) UTC · revision {latest.revision} · "
        f"last write {last_write} UTC"
    )
    if age_seconds(latest.updated_at) > settings.dashboard_stale_after_seconds:
        st.warning(
            "Candle updates are stale. A developing candle can remain open "
            "when ingestion or processing stops."
        )
    st.caption(
        "Green: close ≥ open; red: close < open. Final means the watermark "
        "ended acceptance, not that capture history is complete. Gaps are not filled."
    )
    price_chart, volume_chart = candle_chart(candles)
    st.altair_chart(price_chart, width="stretch")
    st.altair_chart(volume_chart, width="stretch")
    st.dataframe(candle_table(candles), hide_index=True, width="stretch")


st.title(f"{settings.dashboard_symbol} · Multi-Exchange Market Monitor")
st.caption(
    "Public Binance and MEXC trades · UTC timestamps · refreshes every "
    f"{settings.dashboard_refresh_seconds} seconds"
)
live_market()
candle_market()
