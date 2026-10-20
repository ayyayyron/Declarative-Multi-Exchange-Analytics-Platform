CREATE SCHEMA IF NOT EXISTS raw_market;

CREATE TABLE IF NOT EXISTS raw_market.query_history (
    query_name text PRIMARY KEY,
    checkpoint_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_market.pipeline_batch_runs (
    query_name text NOT NULL REFERENCES raw_market.query_history(query_name),
    batch_id bigint NOT NULL,
    input_frames bigint NOT NULL,
    valid_events bigint NOT NULL,
    rejected_rows bigint NOT NULL,
    inserted_events bigint NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);

CREATE TABLE IF NOT EXISTS raw_market.live_trades (
    event_id text PRIMARY KEY,
    source text NOT NULL CHECK (source IN ('binance', 'mexc')),
    symbol text NOT NULL CHECK (symbol = upper(symbol)),
    trade_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    price numeric(28, 10) NOT NULL CHECK (price > 0),
    quantity numeric(28, 10) NOT NULL CHECK (quantity > 0),
    trade_side text NOT NULL CHECK (trade_side IN ('buy', 'sell', 'unknown')),
    capture_id text NOT NULL,
    frame_event_index integer NOT NULL CHECK (frame_event_index >= 0),
    source_kind text NOT NULL,
    source_location text NOT NULL,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    observed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_kind, source_location, frame_event_index)
);

CREATE INDEX IF NOT EXISTS raw_live_trades_symbol_time_idx
    ON raw_market.live_trades (symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS raw_live_trades_observed_idx
    ON raw_market.live_trades (observed_at);

CREATE TABLE IF NOT EXISTS raw_market.latest_trades
    (LIKE analytics.latest_trades INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw_market.pipeline_status
    (LIKE analytics.pipeline_status INCLUDING ALL);

CREATE TABLE IF NOT EXISTS raw_market.rejected_frames (
    source_kind text NOT NULL,
    source_location text NOT NULL,
    frame_event_index integer NOT NULL,
    capture_id text,
    raw_frame_json text,
    reason text NOT NULL,
    query_name text NOT NULL,
    batch_id bigint NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_kind, source_location, frame_event_index)
);
