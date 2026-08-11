from typing import Any

import altair as alt
import pandas as pd
import psycopg
import streamlit as st
from psycopg_pool import ConnectionPool, PoolTimeout

from common.config import get_settings
from dashboard.queries import (
    LatestTrade,
    age_seconds,
    calculate_spread,
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
    try:
        with database_pool(str(settings.postgres_dsn)).connection() as connection:
            latest = fetch_latest_trades(connection, settings.dashboard_symbol)
            prices = fetch_recent_prices(
                connection,
                settings.dashboard_symbol,
                int(settings.dashboard_chart_points),
            )
            trades = fetch_recent_trades(connection, settings.dashboard_symbol)
            pipeline = fetch_pipeline_status(connection)
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


st.title(f"{settings.dashboard_symbol} · Multi-Exchange Market Monitor")
st.caption(
    "Public Binance and MEXC trades · UTC timestamps · refreshes every "
    f"{settings.dashboard_refresh_seconds} seconds"
)
live_market()
