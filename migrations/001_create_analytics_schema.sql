CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.venue_candles_1m (
    source text NOT NULL CHECK (source IN ('binance', 'mexc')),
    symbol text NOT NULL CHECK (symbol = upper(symbol)),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    open_price numeric(28, 10) NOT NULL CHECK (open_price > 0),
    high_price numeric(28, 10) NOT NULL CHECK (high_price > 0),
    low_price numeric(28, 10) NOT NULL CHECK (low_price > 0),
    close_price numeric(28, 10) NOT NULL CHECK (close_price > 0),
    vwap numeric(28, 10) NOT NULL CHECK (vwap > 0),
    base_volume numeric(28, 10) NOT NULL CHECK (base_volume > 0),
    trade_count bigint NOT NULL CHECK (trade_count > 0),
    price_volatility numeric(28, 10),
    last_event_time timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, symbol, window_start),
    CHECK (window_end = window_start + interval '1 minute'),
    CHECK (high_price >= low_price),
    CHECK (open_price BETWEEN low_price AND high_price),
    CHECK (close_price BETWEEN low_price AND high_price),
    CHECK (vwap BETWEEN low_price AND high_price)
);

CREATE INDEX IF NOT EXISTS venue_candles_symbol_window_idx
    ON analytics.venue_candles_1m (symbol, window_start DESC);

CREATE TABLE IF NOT EXISTS analytics.pipeline_status (
    component text NOT NULL,
    source text NOT NULL CHECK (source IN ('binance', 'mexc', 'all')),
    status text NOT NULL CHECK (status IN ('starting', 'healthy', 'stale', 'failed')),
    last_event_time timestamptz,
    last_ingest_time timestamptz,
    last_successful_batch_at timestamptz,
    events_processed bigint NOT NULL DEFAULT 0 CHECK (events_processed >= 0),
    events_rejected bigint NOT NULL DEFAULT 0 CHECK (events_rejected >= 0),
    reconnect_count bigint NOT NULL DEFAULT 0 CHECK (reconnect_count >= 0),
    detail text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (component, source)
);

CREATE TABLE IF NOT EXISTS analytics.pipeline_batch_runs (
    query_name text NOT NULL,
    batch_id bigint NOT NULL CHECK (batch_id >= 0),
    started_at timestamptz NOT NULL,
    committed_at timestamptz,
    input_rows bigint NOT NULL CHECK (input_rows >= 0),
    output_rows bigint CHECK (output_rows >= 0),
    status text NOT NULL CHECK (status IN ('started', 'committed', 'failed')),
    PRIMARY KEY (query_name, batch_id)
);
