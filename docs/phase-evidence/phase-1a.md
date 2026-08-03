# Phase 1A evidence

Phase 1A was validated locally on the ARM64 development machine.

| Check | Result |
|---|---|
| Compose configuration | Kafka, topic initializer, Kafka UI, and PostgreSQL resolved |
| Service health | Kafka, Kafka UI, and PostgreSQL healthy |
| PostgreSQL version | `postgres:17.10-bookworm` started successfully |
| Migration first run | Migrations 001 and 002 applied |
| Migration second run | Both migrations recognized by checksum and skipped |
| PostgreSQL restart | Two migration records remained after stop/start |
| Cross-exchange view | A rolled-back fixture returned a 1.00 price difference and 99.5025 bps |
| Kafka topics | Three `raw-trades` partitions and one DLQ partition retained |
| Spark compatibility | Kafka connector read succeeded; checkpoint restart produced IDs 1, 2, and 3 once |
| Protobuf generation | Repeated generation produced identical Python and type-stub hashes |
| Python quality gate | Black, Ruff, strict mypy, and 19 pytest tests passed |

The exchange fixtures are offline contract evidence. Phase 1A does not claim that
the live dual-source producer or Spark aggregation pipeline exists.
