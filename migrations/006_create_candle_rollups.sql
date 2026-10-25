CREATE FUNCTION raw_market.candle_rollups(
    p_source text,
    p_symbol text,
    p_interval text,
    p_limit integer DEFAULT 200
)
RETURNS TABLE (
    window_start_ms bigint,
    window_end_ms bigint,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    trade_count bigint,
    is_final boolean,
    updated_at timestamptz,
    base_windows bigint,
    expected_windows bigint,
    all_observed_final boolean
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    duration_ms bigint;
    slots bigint;
    monday_origin_ms constant bigint := 345600000;
BEGIN
    duration_ms := CASE p_interval
        WHEN '15m' THEN 900000
        WHEN '1h' THEN 3600000
        WHEN '1d' THEN 86400000
        WHEN '1w' THEN 604800000
        ELSE NULL
    END;
    IF duration_ms IS NULL THEN
        RAISE EXCEPTION 'Unsupported rollup interval' USING ERRCODE = '22023';
    END IF;
    IF p_source IS NULL OR p_source NOT IN ('binance', 'mexc')
        OR p_symbol IS NULL OR p_symbol !~ '^[A-Z0-9]{3,20}$'
        OR p_limit IS NULL OR p_limit < 1 OR p_limit > 2000 THEN
        RAISE EXCEPTION 'Invalid rollup selection or limit' USING ERRCODE = '22023';
    END IF;
    slots := duration_ms / 300000;

    RETURN QUERY
    WITH recent_base AS (
        SELECT c.*
        FROM raw_market.candles c
        WHERE c.source = p_source AND c.symbol = p_symbol AND c.interval = '5m'
        ORDER BY c.window_start_ms DESC
        LIMIT p_limit * slots
    ), bucketed AS (
        SELECT c.*,
            floor((c.window_start_ms - monday_origin_ms)::numeric / duration_ms)::bigint
                * duration_ms + monday_origin_ms AS bucket_start
        FROM recent_base c
    ), latest_rollups AS (
        SELECT b.bucket_start,
            (array_agg(b.open ORDER BY b.window_start_ms))[1] AS first_open,
            max(b.high) AS highest,
            min(b.low) AS lowest,
            (array_agg(b.close ORDER BY b.window_start_ms DESC))[1] AS last_close,
            sum(b.volume) AS total_volume,
            sum(b.trade_count)::bigint AS total_trades,
            bool_and(b.is_final) AS observed_final,
            max(b.updated_at) AS last_update,
            count(*) AS observed_slots
        FROM bucketed b
        GROUP BY b.bucket_start
        ORDER BY b.bucket_start DESC
        LIMIT p_limit
    )
    SELECT r.bucket_start, r.bucket_start + duration_ms,
        r.first_open, r.highest, r.lowest, r.last_close,
        r.total_volume, r.total_trades,
        r.observed_slots = slots AND r.observed_final,
        r.last_update, r.observed_slots, slots, r.observed_final
    FROM latest_rollups r
    ORDER BY r.bucket_start;
END;
$$;

COMMENT ON FUNCTION raw_market.candle_rollups(text, text, text, integer) IS
'Read-time UTC rollups of current 5m rows. Weeks start Monday. Full base-window coverage does not prove complete exchange capture.';
