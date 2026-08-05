# Binance-to-Kafka evidence

The first live producer milestone was validated locally on 2026-08-03.

| Check | Result |
|---|---|
| Public source | Connected to `btcusdt@trade` without credentials |
| Bounded run | Five messages received, queued, and acknowledged |
| Kafka key | All records used `binance:BTCUSDT` |
| Partition and offsets | Partition 1, offsets 0 through 4 |
| Contract | All values were canonical schema-version 2 JSON |
| Shutdown | The producer queue flushed with zero failed deliveries |

This milestone does not claim reconnect handling, DLQ publication, MEXC live
ingestion, or completion of the dual-source producer.
