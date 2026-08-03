# Preflight evidence

Do not record user identities, tokens, account IDs, or private workspace URLs in
this file.

## Public exchange access

| Check | Status | Evidence |
|---|---|---|
| Binance WebSocket | Passed 2026-07-27 | Anonymous BTCUSDT JSON trade received |
| MEXC WebSocket | Passed 2026-07-27 | Anonymous subscription accepted and binary frame received |
| Offline fixtures | Captured 2026-07-28 | Binance JSON trade and MEXC aggregate-deals Protobuf frame |

## Local runtime

| Check | Status | Evidence |
|---|---|---|
| Hardware | Passed | Apple M3 ARM64 MacBook Air with 16 GB memory |
| Docker | Passed previously | Docker 28.1.1 and Compose 2.35.1 |
| Java | Passed | Homebrew OpenJDK 17 selected by the Makefile |
| Python | Passed | Homebrew Python 3.11 in repository `.venv` |
| Kafka persistence | Passed | Test record survived a normal Compose stop/start |
| Spark checkpoint reuse | Passed | Restart processed only newly added input |

## Phase 1A gates

| Check | Status |
|---|---|
| PostgreSQL image starts on ARM64 | Passed with PostgreSQL 17.10 |
| PostgreSQL volume survives stop/start | Passed; two migration records retained |
| Migrations are repeatable | Passed; second run skipped both verified checksums |
| Cross-exchange SQL view | Passed; rolled-back fixture returned 99.5025 bps |
| Version-2 fixtures map to one contract | Passed for Binance JSON and six MEXC deals |
| MEXC generated decoder | Passed; regeneration produced identical SHA-256 hashes |
| Spark Kafka connector and checkpoint reuse | Passed |
| Formatting, linting, typing, and tests | Passed; 19 tests |
