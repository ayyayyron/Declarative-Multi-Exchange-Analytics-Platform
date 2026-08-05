SHELL := /bin/bash

PYTHON := .venv/bin/python
JAVA_17_HOME := $(shell brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home
export JAVA_HOME := $(JAVA_17_HOME)

.PHONY: help venv install format format-check lint test check proto db-migrate db-status infra-up infra-down infra-stop infra-status infra-logs topics spark-smoke producer producer-binance producer-mexc producer-dlq-smoke

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
	@echo "make producer          Stream both exchanges until stopped"
	@echo "make producer-binance  Stream Binance trades into Kafka until stopped"
	@echo "make producer-mexc     Stream MEXC trades into Kafka until stopped"
	@echo "make producer-dlq-smoke  Publish one malformed fixture to the DLQ"

venv:
	@test -x $(PYTHON) || python3.11 -m venv .venv

install: venv
	$(PYTHON) -m pip install -r requirements.txt

proto:
	$(PYTHON) -m grpc_tools.protoc \
		--proto_path=producer/proto \
		--python_out=producer/generated \
		--pyi_out=producer/generated \
		producer/proto/mexc_market_data.proto

db-migrate:
	$(PYTHON) -m scripts.migrate

db-status:
	docker compose exec postgres sh -c \
		'pg_isready --username="$${POSTGRES_USER}" --dbname="$${POSTGRES_DB}"'

format:
	$(PYTHON) -m black common producer streaming dashboard scripts tests
	$(PYTHON) -m ruff check --fix common producer streaming dashboard scripts tests

format-check:
	$(PYTHON) -m black --check common producer streaming dashboard scripts tests

lint:
	$(PYTHON) -m ruff check common producer streaming dashboard scripts tests
	$(PYTHON) -m mypy common producer streaming dashboard scripts

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

producer:
	$(PYTHON) -m producer.main --source both

producer-binance:
	$(PYTHON) -m producer.main --source binance

producer-mexc:
	$(PYTHON) -m producer.main --source mexc

producer-dlq-smoke:
	$(PYTHON) -m scripts.producer_dlq_smoke
