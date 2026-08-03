# Data contracts

## Kafka key

```text
<source>:<symbol>
```

The key defines the ordering boundary. Examples are `binance:BTCUSDT` and
`mexc:BTCUSDT`.

## Canonical trade version 2

| Field | Purpose |
|---|---|
| `schema_version` | Contract compatibility; fixed to `2` |
| `event_id` | Stable deduplication identity |
| `source` | `binance` or `mexc` |
| `event_time_ms` | Exchange message time |
| `trade_time_ms` | Economic trade time used for windows |
| `received_at_ms` | Local ingestion time |
| `symbol` | Uppercase shared market symbol |
| `source_event_id` | Native exchange trade ID when available |
| `price`, `quantity` | Positive decimal strings |
| `trade_side` | Source-reported or derived buy/sell classification |
| `is_buyer_maker` | Binance flag; null when unavailable |
| `source_payload_encoding` | `utf8-json` or `base64-protobuf` |
| `source_payload` | Exact retained source message |

Binance event IDs use `binance:<symbol>:<trade-id>`.

Current MEXC aggregate deals expose `tradeId`, so their preferred identity is
`mexc:<symbol>:<trade-id>`. If an older or empty-ID frame is received, the adapter
uses `mexc:<sha256-of-frame>:<deal-index>`. That fallback deduplicates identical
frame replay but cannot prove economic uniqueness across differently encoded
frames.

Prices and quantities remain strings on Kafka so JSON serialization cannot
introduce binary floating-point changes. Spark casts them to explicit decimals
after validation.
