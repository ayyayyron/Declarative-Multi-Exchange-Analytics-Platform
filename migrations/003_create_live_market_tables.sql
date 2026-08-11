CREATE TABLE IF NOT EXISTS analytics.live_trades (
    event_id text PRIMARY KEY,
    source text NOT NULL CHECK (source IN ('binance', 'mexc')),
    symbol text NOT NULL CHECK (symbol = upper(symbol)),
    trade_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    price numeric(28, 10) NOT NULL CHECK (price > 0),
    quantity numeric(28, 10) NOT NULL CHECK (quantity > 0),
    trade_side text NOT NULL CHECK (trade_side IN ('buy', 'sell', 'unknown')),
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset bigint NOT NULL CHECK (kafka_offset >= 0),
    observed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS live_trades_symbol_time_idx
    ON analytics.live_trades (symbol, trade_time DESC);

CREATE INDEX IF NOT EXISTS live_trades_observed_at_idx
    ON analytics.live_trades (observed_at);

CREATE TABLE IF NOT EXISTS analytics.latest_trades (
    source text NOT NULL CHECK (source IN ('binance', 'mexc')),
    symbol text NOT NULL CHECK (symbol = upper(symbol)),
    event_id text NOT NULL,
    trade_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    price numeric(28, 10) NOT NULL CHECK (price > 0),
    quantity numeric(28, 10) NOT NULL CHECK (quantity > 0),
    trade_side text NOT NULL CHECK (trade_side IN ('buy', 'sell', 'unknown')),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, symbol)
);
