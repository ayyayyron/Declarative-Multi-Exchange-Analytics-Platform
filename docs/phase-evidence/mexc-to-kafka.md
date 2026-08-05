# MEXC-to-Kafka evidence

The first live MEXC producer milestone was validated locally on 2026-08-05.

| Check | Result |
|---|---|
| Public source | Subscribed without credentials |
| Subscription | Aggregate-deals channel acknowledged with code 0 |
| Bounded run | Five canonical trades queued and acknowledged |
| Kafka key | All records used `mexc:BTCUSDT` |
| Partition and offsets | Partition 1, offsets 10 through 14 |
| Protobuf expansion | One source frame produced three distinct trade records |
| Contract | All values were canonical schema-version 2 JSON |
| Shutdown | The producer queue flushed with zero failed deliveries |

This milestone does not claim reconnect handling, application-level heartbeats,
DLQ publication, or concurrent Binance and MEXC supervision.
