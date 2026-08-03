# Phase 0 — Validated foundation

Phase 0 established:

- Python 3.11 and project-local Java 17 selection
- Kafka 4.3.1 in KRaft mode
- Separate host and Docker-network listeners
- Explicit topics and named-volume persistence
- PySpark 4.0.3 and the matching Scala 2.13 Kafka connector
- Structured logging, centralized settings, strict typing, and offline tests
- Spark checkpoint reuse behavior

The original Binance-only contract and Delta smoke path were superseded by the
multi-exchange Phase 1 plan. The Databricks results were preserved in Project 2.

## Kafka concepts retained by the new design

- A topic is an append-only log split into partitions.
- Ordering exists within a partition, not across a complete topic.
- A record key chooses a partition consistently.
- Consumer groups maintain independent progress through offsets.
- Spark checkpoints contain source progress and state beyond a plain offset.
- KRaft stores Kafka cluster metadata without ZooKeeper.

Host clients use `localhost:9092`. Containers use `kafka:29092`; the two advertised
listeners represent different network namespaces.

Normal Compose stop/start operations preserve the Kafka and PostgreSQL named
volumes. Volume deletion is not a recovery drill.
