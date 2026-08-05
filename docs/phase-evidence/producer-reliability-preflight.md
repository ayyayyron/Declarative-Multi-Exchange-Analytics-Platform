# Producer reliability preflight

The producer was brought to the measured-run boundary on 2026-08-05.

## Deterministic checks

| Check | Result |
|---|---|
| Quality gate | Black, Ruff, strict mypy, and 39 tests passed |
| Reconnect | Simulated disconnect retried with the expected jittered delay |
| Source isolation | Simulated Binance failure did not cancel the MEXC worker |
| Heartbeat | MEXC application-level PING stopped with the shared stop event |
| DLQ | Malformed Binance JSON and MEXC Protobuf produced dead-letter events |
| Multi-deal frame | One captured MEXC frame produced six canonical trades |
| Blocked socket | The shared stop event interrupted a pending receive |

## Short live proofs

The bounded dual-source run produced five canonical events from each exchange.
Binance used five JSON frames; MEXC produced five trades from one Protobuf frame.
Kafka acknowledged all ten records with zero delivery failures.

The DLQ smoke published `binance:source_decode` to `raw-trades-dlq` partition 0,
offset 0. The record retained the malformed public payload and no environment or
credential context.

A continuous dual-source run was interrupted with Ctrl+C. The process exited
with code 0 after reporting 304 Binance events, 18 MEXC events, 322 Kafka delivery
acknowledgements, and zero delivery failures.

The application-level heartbeat is proven offline but was not emitted during the
short live proofs because they ended before its 20-second interval.

## Remaining acceptance

Run both feeds concurrently for at least 15 minutes, retain the start and final
metrics, and confirm valid version-2 records from both source keys. This preflight
does not claim that measured run has occurred.
