CREATE TABLE raw_market.candle_query_history (
    interval text PRIMARY KEY CHECK (interval IN ('1m', '5m')),
    query_name text NOT NULL UNIQUE,
    checkpoint_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (interval, checkpoint_id)
);

CREATE TABLE raw_market.candle_batch_runs (
    interval text NOT NULL,
    checkpoint_id uuid NOT NULL,
    batch_id bigint NOT NULL CHECK (batch_id >= 0),
    snapshot_rows bigint NOT NULL CHECK (snapshot_rows >= 0),
    applied_rows bigint NOT NULL CHECK (applied_rows >= 0),
    unchanged_rows bigint NOT NULL CHECK (unchanged_rows >= 0),
    stale_rows bigint NOT NULL CHECK (stale_rows >= 0),
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (interval, checkpoint_id, batch_id),
    FOREIGN KEY (interval, checkpoint_id)
        REFERENCES raw_market.candle_query_history (interval, checkpoint_id),
    CHECK (snapshot_rows = applied_rows + unchanged_rows + stale_rows)
);

CREATE TABLE raw_market.candles (
    source text NOT NULL CHECK (source IN ('binance', 'mexc')),
    symbol text NOT NULL CHECK (symbol ~ '^[A-Z0-9]{3,20}$'),
    interval text NOT NULL CHECK (interval IN ('1m', '5m')),
    window_start_ms bigint NOT NULL CHECK (window_start_ms > 0),
    window_end_ms bigint NOT NULL,
    open numeric(28, 10) NOT NULL CHECK (open > 0 AND open <> 'NaN'::numeric),
    high numeric(28, 10) NOT NULL CHECK (high > 0 AND high <> 'NaN'::numeric),
    low numeric(28, 10) NOT NULL CHECK (low > 0 AND low <> 'NaN'::numeric),
    close numeric(28, 10) NOT NULL CHECK (close > 0 AND close <> 'NaN'::numeric),
    volume numeric(38, 10) NOT NULL CHECK (volume > 0 AND volume <> 'NaN'::numeric),
    trade_count bigint NOT NULL CHECK (trade_count > 0),
    revision bigint NOT NULL CHECK (revision > 0),
    is_final boolean NOT NULL,
    checkpoint_id uuid NOT NULL,
    last_batch_id bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, symbol, interval, window_start_ms),
    FOREIGN KEY (interval, checkpoint_id, last_batch_id)
        REFERENCES raw_market.candle_batch_runs (interval, checkpoint_id, batch_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (low <= open AND open <= high AND low <= close AND close <= high),
    CHECK (
        window_start_ms % CASE interval WHEN '1m' THEN 60000 ELSE 300000 END = 0
        AND window_end_ms = window_start_ms
            + CASE interval WHEN '1m' THEN 60000 ELSE 300000 END
    )
);

CREATE INDEX candles_symbol_interval_time_idx
    ON raw_market.candles (symbol, interval, window_start_ms DESC);
