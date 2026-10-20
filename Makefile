SHELL := /bin/bash

PYTHON := .venv/bin/python
FEEDS_CONFIG ?= config/feeds.yaml
.PHONY: raw-quarantine-smoke
.PHONY: candles candles-once
JAVA_17_HOME := $(shell brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home
export JAVA_HOME := $(JAVA_17_HOME)
export PYSPARK_PYTHON := $(abspath $(PYTHON))
export PYSPARK_DRIVER_PYTHON := $(abspath $(PYTHON))

.PHONY: help venv install format format-check lint test check proto db-migrate db-status infra-up infra-down infra-stop infra-status infra-logs topics spark-smoke spark-consumer spark-consumer-once live-pipeline live-pipeline-once dashboard ingestion ingestion-plan ingestion-binance ingestion-mexc ingestion-dlq-smoke producer producer-binance producer-mexc producer-dlq-smoke

help:
	@echo "make install       Create .venv and install pinned dependencies"
	@echo "make check         Run formatting, linting, typing, and tests"
	@echo "make infra-up      Start Kafka, Kafka UI, and PostgreSQL"
	@echo "make infra-down    Stop containers while preserving Kafka data"
	@echo "make infra-status  Show container health"
	@echo "make topics        Describe the explicit topic contracts"
	@echo "make proto         Regenerate the pinned MEXC Protobuf module"
	@echo "make db-migrate    Apply pending PostgreSQL migrations"
	@echo "make db-status     Check PostgreSQL readiness"
	@echo "make spark-smoke   Verify the selected Java/Spark/Kafka combination"
	@echo "make spark-consumer       Consume and inspect new Kafka trades"
	@echo "make spark-consumer-once  Consume available trades and then stop"
	@echo "make live-pipeline        Stream validated trades into PostgreSQL"
	@echo "make live-pipeline-once   Sink available trades and then stop"
	@echo "make candles              Run separate one-minute candle aggregation"
	@echo "make candles-once         Aggregate available input and then stop"
	@echo "make dashboard            Start Streamlit on localhost:8501"
	@echo "make ingestion          Stream the configured YAML watchlists"
	@echo "make ingestion-plan     Validate and print the YAML worker plan"
	@echo "make ingestion-binance  Stream Binance trades into Kafka until stopped"
	@echo "make ingestion-mexc     Stream MEXC trades into Kafka until stopped"
	@echo "make ingestion-dlq-smoke  Compatibility alias for raw-quarantine-smoke"
	@echo "make raw-quarantine-smoke Capture a malformed frame for Spark quarantine"

venv:
	@test -x $(PYTHON) || python3.11 -m venv .venv

install: venv
	$(PYTHON) -m pip install -r requirements.txt

proto:
	$(PYTHON) -m grpc_tools.protoc \
		--proto_path=processing/decoders/mexc/proto \
		--python_out=processing/decoders/mexc/generated \
		--pyi_out=processing/decoders/mexc/generated \
		processing/decoders/mexc/proto/mexc_market_data.proto

db-migrate:
	$(PYTHON) -m scripts.migrate

db-status:
	docker compose exec postgres sh -c \
		'pg_isready --username="$${POSTGRES_USER}" --dbname="$${POSTGRES_DB}"'

format:
	$(PYTHON) -m black common ingestion processing dashboard scripts tests
	$(PYTHON) -m ruff check --fix common ingestion processing dashboard scripts tests

format-check:
	$(PYTHON) -m black --check common ingestion processing dashboard scripts tests

lint:
	$(PYTHON) -m ruff check common ingestion processing dashboard scripts tests
	$(PYTHON) -m mypy common ingestion processing dashboard scripts

test:
	$(PYTHON) -m pytest

check: format-check lint test

infra-up:
	docker volume create streaming-kafka-data
	docker volume create streaming-postgres-data
	docker compose up --detach

infra-down:
	docker compose down

infra-stop:
	docker compose stop

infra-status:
	docker compose ps

infra-logs:
	docker compose logs --tail=100

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe

spark-smoke:
	$(PYTHON) -m scripts.spark_kafka_smoke

spark-consumer:
	$(PYTHON) -m processing.spark.inspect

spark-consumer-once:
	$(PYTHON) -m processing.spark.inspect --available-now

live-pipeline:
	$(PYTHON) -m processing.spark.main

live-pipeline-once:
	$(PYTHON) -m processing.spark.main --available-now

candles:
	$(PYTHON) -m processing.spark.candle_pipeline

candles-once:
	$(PYTHON) -m processing.spark.candle_pipeline --available-now

dashboard:
	$(PYTHON) -m streamlit run dashboard/app.py --server.address 127.0.0.1

ingestion:
	$(PYTHON) -m ingestion.main --config "$(FEEDS_CONFIG)"

ingestion-plan:
	$(PYTHON) -m ingestion.main --config "$(FEEDS_CONFIG)" --dry-run

ingestion-binance:
	$(PYTHON) -m ingestion.main --source binance

ingestion-mexc:
	$(PYTHON) -m ingestion.main --source mexc

raw-quarantine-smoke:
	$(PYTHON) -m scripts.raw_quarantine_smoke

ingestion-dlq-smoke: raw-quarantine-smoke

producer:
	$(PYTHON) -m ingestion.main --source both
producer-binance: ingestion-binance
producer-mexc: ingestion-mexc
producer-dlq-smoke: ingestion-dlq-smoke
