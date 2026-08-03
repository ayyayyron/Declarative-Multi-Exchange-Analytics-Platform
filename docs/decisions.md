# Architecture Decision Log

## ADR-001 — Dockerize services; run Python and Spark on the host

**Status:** Accepted

Kafka and PostgreSQL benefit from reproducible service configuration and named
volumes. Host-based Python and PySpark are easier to debug on the learning
machine. This is a local development topology, not a production deployment.

## ADR-002 — Use one KRaft broker with explicit topics

**Status:** Accepted

Auto-topic creation is disabled. `raw-trades` has three partitions and a
24-hour retention; `raw-trades-dlq` has one partition and a seven-day retention.
Replication factor one cannot survive loss of the broker volume.

## ADR-003 — Key canonical trades by source and symbol

**Status:** Accepted

Keys such as `binance:BTCUSDT` and `mexc:BTCUSDT` preserve ordering within one
venue-market stream. Independent exchanges do not share a trustworthy total
order, and Spark will shuffle by source, symbol, and event-time window when it
aggregates.

## ADR-004 — Normalize before Kafka

**Status:** Accepted

Source adapters convert Binance JSON and MEXC Protobuf to one versioned JSON
contract. Spark and the later Flink implementation therefore depend on a stable
business event instead of exchange-specific wire fields. Exact source data is
retained for diagnosis.

## ADR-005 — Use PostgreSQL only as a serving store

**Status:** Accepted

Kafka retains recent canonical events for replay. Spark will write compact
one-minute aggregates to PostgreSQL using deterministic keys and transactional
upserts. PostgreSQL is not an unbounded raw-event archive.

## ADR-006 — Keep Databricks in Project 2

**Status:** Superseded in this repository

The earlier Delta and Databricks path was moved to the separate equities
lakehouse project. This project now compares Spark/PostgreSQL with
Flink/ClickHouse while keeping the same Kafka contract.

## ADR-007 — Generate a minimal MEXC market-data decoder

**Status:** Accepted

MEXC's official wrapper imports every public and private channel schema. This
project pins the relevant upstream files and generates a local decoder containing
only aggregate public deals with identical Protobuf field numbers. Unknown
wrapper fields are ignored by Protobuf, avoiding unused account models.

## ADR-008 — Keep local credentials outside tracked source

**Status:** Accepted

`.env` is ignored. Its default PostgreSQL password is explicitly local-only, and
all service ports bind to `127.0.0.1`. Exchange market-data connections use no
account or API credentials.
