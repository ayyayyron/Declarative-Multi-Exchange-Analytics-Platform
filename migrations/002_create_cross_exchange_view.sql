CREATE OR REPLACE VIEW analytics.cross_exchange_comparison_1m AS
SELECT
    binance.symbol,
    binance.window_start,
    binance.window_end,
    binance.close_price AS binance_close,
    mexc.close_price AS mexc_close,
    mexc.close_price - binance.close_price AS price_difference,
    10000 * (mexc.close_price - binance.close_price)
        / NULLIF((mexc.close_price + binance.close_price) / 2, 0) AS spread_bps,
    binance.vwap AS binance_vwap,
    mexc.vwap AS mexc_vwap,
    binance.base_volume AS binance_volume,
    mexc.base_volume AS mexc_volume,
    binance.last_event_time AS binance_last_event_time,
    mexc.last_event_time AS mexc_last_event_time
FROM analytics.venue_candles_1m AS binance
JOIN analytics.venue_candles_1m AS mexc
    ON mexc.symbol = binance.symbol
    AND mexc.window_start = binance.window_start
WHERE binance.source = 'binance'
    AND mexc.source = 'mexc';
