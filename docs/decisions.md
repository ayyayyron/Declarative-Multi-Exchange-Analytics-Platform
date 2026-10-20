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

**Status:** Superseded by ADR-012 for new ingestion

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

## ADR-009 — Supervise exchange sessions independently

**Status:** Accepted

Binance and MEXC run under separate reconnect supervisors and share only the
Kafka publisher and coordinated stop event. A recoverable source exception is
contained by its supervisor, while capped exponential backoff with jitter avoids
synchronized reconnect pressure. `SIGINT` and `SIGTERM` set the stop event so
sockets, heartbeat tasks, metrics, and Kafka flushing follow one shutdown path.

## ADR-010 — Keep source failures out of the canonical trade topic

**Status:** Legacy canonical path; superseded by ADR-012 for raw capture

Decode and contract-validation failures are serialized as `DeadLetterEvent`
records in `raw-trades-dlq`. The normal topic therefore retains only valid
version-2 trades. Failure context is bounded and excludes configuration or
tracebacks; the original public payload is retained for diagnosis and replay.

## ADR-011 — Expand watchlists into bounded asynchronous feeds

**Status:** Accepted

`config/feeds.yaml` selects supported spot trade pairs and venues. Strict startup
validation expands watchlists into at most 20 unique exchange/symbol feeds before
opening network connections. The tracked example is the template; the local
file is ignored. Operational settings remain in `.env`. Explicit `--source`
retains environment-based market selection for older commands.

Each logical feed owns one WebSocket session, reconnect supervisor, and metrics
identity. All feeds share one Kafka client in one Python process. This keeps
failures isolated without generating code, containers, or brokers per symbol.
The first implementation favors simple connections over subscription multiplexing.

Syntax validation does not prove exchange listing availability. Instrument
discovery, provider-wide rate limiting, quote feeds, and dynamic dashboard
selection are separate work. The existing canonical trade contract, Spark query,
and PostgreSQL keys already support multiple symbols and are unchanged.

## ADR-012 — Capture raw frames before interpretation

**Status:** Accepted

Ingestion owns sockets, subscriptions, control messages, receipt times, and
publication. It writes a versioned raw envelope to `raw-market-frames-v1` without
interpreting trade fields. Spark-worker decoders produce the existing canonical
business contract, followed by shared Spark validation and serving writes.
This moves the retained replay boundary before provider-specific interpretation.
It does not recover data never received/published or outlive Kafka retention.

There is no intermediate canonical topic or staging store. A source-reader
boundary supplies a common raw-frame DataFrame shape; a local fixture proves
normalization does not require Kafka metadata. File/S3 readers and archiving
remain future work. Python-worker decoders reuse the verified generated Protobuf
and model behavior; native Spark decoding is a possible measured optimization,
not assumed equivalent without tests.

Cutover uses a new checkpoint and `raw_market` schema; legacy state is preserved.
Sub-event indexes allow one frame to contain many trades. Transactions include
serving changes, bounded quarantine, counters, and batch evidence. A locked query
history binds the serving namespace to the checkpoint UUID and fails on mismatch.
The UUID is read from Spark-managed metadata, not edited by the application.
Intentional replay currently requires isolated output/database and checkpoint
state. The legacy sink's earlier reset hazard is not retroactively repaired.
