"""Generate the Grafana dashboard from SQL queries and panel definitions."""

import json
from pathlib import Path
from typing import Any

DATASOURCE = {"type": "postgres", "uid": "market-postgres"}
SYMBOL = "${symbol:sqlstring}"
EXCHANGE = "${exchange:sqlstring}"
INTERVAL = "${interval:sqlstring}"

CANDLES = f"""
WITH selected AS (
    SELECT window_start_ms,window_end_ms,open,high,low,close,volume,trade_count,
        is_final,updated_at,revision::text AS revision,
        NULL::bigint AS base_windows,NULL::bigint AS expected_windows,
        NULL::boolean AS all_observed_final
    FROM (SELECT * FROM raw_market.candles
        WHERE source={EXCHANGE} AND symbol={SYMBOL} AND interval={INTERVAL}
        ORDER BY window_start_ms DESC LIMIT 200) b
    WHERE {INTERVAL} IN ('1m','5m')
    UNION ALL
    SELECT window_start_ms,window_end_ms,open,high,low,close,volume,trade_count,
        is_final,updated_at,'Computed',base_windows,expected_windows,all_observed_final
    FROM raw_market.candle_rollups({EXCHANGE},{SYMBOL},
        CASE WHEN {INTERVAL} IN ('15m','1h','1d','1w')
            THEN {INTERVAL} ELSE '15m' END,200)
    WHERE {INTERVAL} NOT IN ('1m','5m')
), visible AS (
    SELECT *, CASE WHEN is_final THEN 'Final'
        WHEN all_observed_final THEN 'Partial' ELSE 'Developing' END AS status,
        CASE WHEN base_windows IS NULL THEN 'Base candle'
        ELSE base_windows::text || '/' || expected_windows::text END AS coverage
    FROM selected
    WHERE window_end_ms > $__unixEpochFrom()::bigint * 1000
        AND window_start_ms < $__unixEpochTo()::bigint * 1000
)
"""


def target(query: str, format: str = "table") -> dict[str, Any]:
    return {
        "refId": "A",
        "datasource": DATASOURCE,
        "rawSql": query,
        "format": format,
        "rawQuery": True,
    }


def panel(
    identifier: int,
    title: str,
    kind: str,
    query: str,
    x: int,
    y: int,
    w: int,
    h: int,
    **options: Any,
) -> dict[str, Any]:
    return {
        "id": identifier,
        "title": title,
        "type": kind,
        "datasource": DATASOURCE,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(query)],
        "fieldConfig": {"defaults": {"decimals": 2}, "overrides": []},
        "options": options,
    }


def variable(name: str, query: str, current: str) -> dict[str, Any]:
    return {
        "name": name,
        "label": name.capitalize(),
        "type": "query",
        "datasource": DATASOURCE,
        "query": query,
        "refresh": 1,
        "multi": False,
        "includeAll": False,
        "current": {"text": current, "value": current},
        "options": [],
    }


def build_dashboard() -> dict[str, Any]:
    """Build the file-provisioned market dashboard."""
    fields = "to_timestamp(window_start_ms/1000.0) AS time,open,high,low,close,volume"
    candle_query = CANDLES + f"SELECT {fields} FROM visible ORDER BY window_start_ms"
    panels = [
        {
            "id": 1,
            "title": "Read this first",
            "type": "text",
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": 3},
            "options": {
                "mode": "markdown",
                "content": (
                    "**Local read-only dashboard · UTC.** Stored samples may be stale; "
                    "run ingestion and Spark for updates.\n\n"
                    "Last-trade differences are **not executable bid/ask spreads**. "
                    "Larger candles use 5m snapshots; missing slots are never filled."
                ),
            },
        },
        panel(
            2,
            "Latest trades · $symbol",
            "table",
            f"""
            SELECT CASE WHEN trade_time >= now()-interval '15 seconds'
                    THEN 'LIVE' ELSE 'STALE' END AS freshness,
                source,price,
                round(greatest(0,extract(epoch FROM now()-trade_time)))
                    AS \"Age (seconds)\",trade_time AS \"Trade time\"
            FROM raw_market.latest_trades WHERE symbol={SYMBOL} ORDER BY source
        """,
            0,
            3,
            10,
            5,
            showHeader=True,
        ),
        panel(
            3,
            "MEXC − Binance · last-trade difference",
            "stat",
            f"""
            SELECT m.price-b.price AS \"Difference (USDT)\",
                10000*(m.price-b.price)/((m.price+b.price)/2)
                    AS \"Symmetric spread (bps)\"
            FROM raw_market.latest_trades b
            JOIN raw_market.latest_trades m USING(symbol)
            WHERE b.source='binance' AND m.source='mexc' AND b.symbol={SYMBOL}
        """,
            10,
            3,
            7,
            5,
            showHeader=True,
        ),
        panel(
            4,
            "Trade pipeline · all symbols",
            "table",
            """
            SELECT CASE WHEN last_successful_batch_at >= now()-interval '15 seconds'
                    THEN 'LIVE' ELSE 'STALE' END AS status,
                events_processed,events_rejected,
                last_successful_batch_at AS "Last commit"
            FROM raw_market.pipeline_status
            WHERE component='spark-live-market' AND source='all'
        """,
            17,
            3,
            7,
            5,
            showHeader=True,
        ),
        panel(
            5,
            "$symbol · $exchange · $interval candles",
            "candlestick",
            candle_query,
            0,
            8,
            16,
            10,
            mode="candles",
            candleStyle="candles",
            colorStrategy="open-close",
        ),
        panel(
            6,
            "Latest visible candle · status and coverage",
            "table",
            CANDLES + """
            SELECT status,
                CASE WHEN updated_at >= now()-interval '15 seconds'
                    THEN 'FRESH' ELSE 'STALE' END AS freshness,
                coverage,close::text,volume::text,trade_count,revision,
                to_timestamp(window_start_ms/1000.0) AS "Start (UTC)",
                to_timestamp(window_end_ms/1000.0) AS "End (exclusive)",
                updated_at AS "Last base update"
            FROM visible ORDER BY window_start_ms DESC LIMIT 1
        """,
            16,
            8,
            8,
            10,
            showHeader=True,
        ),
        panel(
            7,
            "Base-asset volume · $interval",
            "barchart",
            CANDLES + """
            SELECT to_timestamp(window_start_ms/1000.0) AS time,volume
            FROM visible ORDER BY window_start_ms
        """,
            0,
            18,
            24,
            7,
            xField="time",
            showValue="never",
        ),
        panel(
            8,
            "Candle inspection · exact decimal strings",
            "table",
            CANDLES + """
            SELECT to_timestamp(window_start_ms/1000.0) AS "Start (UTC)",
                to_timestamp(window_end_ms/1000.0) AS "End (exclusive)",
                open::text,high::text,low::text,close::text,volume::text,
                trade_count,status,coverage,revision
            FROM visible ORDER BY window_start_ms DESC
        """,
            0,
            25,
            24,
            8,
            showHeader=True,
        ),
        panel(
            9,
            "Recent trades · $symbol",
            "table",
            f"""
            SELECT trade_time,source,price::text,quantity::text,trade_side
            FROM raw_market.live_trades WHERE symbol={SYMBOL}
                AND $__timeFilter(trade_time)
            ORDER BY trade_time DESC,event_id DESC LIMIT 50
        """,
            0,
            33,
            24,
            8,
            showHeader=True,
        ),
    ]
    return {
        "uid": "market-platform",
        "title": "Multi-Exchange Market Analytics",
        "description": "Market analytics on PostgreSQL; Streamlit is also available.",
        "tags": ["market-data", "streaming"],
        "schemaVersion": 41,
        "version": 1,
        "timezone": "utc",
        "editable": False,
        "refresh": "5s",
        "time": {"from": "now-7d", "to": "now"},
        "panels": panels,
        "templating": {
            "list": [
                variable(
                    "symbol",
                    "SELECT symbol FROM raw_market.latest_trades "
                    "UNION SELECT symbol FROM raw_market.candles ORDER BY 1",
                    "BTCUSDT",
                ),
                variable(
                    "exchange",
                    "SELECT DISTINCT source FROM raw_market.candles ORDER BY 1",
                    "binance",
                ),
                {
                    "name": "interval",
                    "label": "Interval",
                    "type": "custom",
                    "query": "1m,5m,15m,1h,1d,1w",
                    "current": {"text": "5m", "value": "5m"},
                    "options": [
                        {"text": v, "value": v, "selected": v == "5m"}
                        for v in ("1m", "5m", "15m", "1h", "1d", "1w")
                    ],
                    "multi": False,
                    "includeAll": False,
                },
            ]
        },
    }


if __name__ == "__main__":
    destination = (
        Path(__file__).resolve().parents[1] / "infra/grafana/dashboards/market.json"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(build_dashboard(), indent=2) + "\n")
    print("Generated infra/grafana/dashboards/market.json")
