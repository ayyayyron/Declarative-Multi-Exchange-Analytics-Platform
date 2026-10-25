import asyncio
import base64
import json
from pathlib import Path
from typing import Any

import pytest

from common.config import Settings
from ingestion import main
from ingestion.configuration import load_configuration
from ingestion.connectors.binance import client as binance_client
from ingestion.connectors.mexc import client as mexc_client
from ingestion.feed_plan import FeedSpec, build_feed_plan
from ingestion.kafka_publisher import KafkaPublishError
from processing.decoders.mexc.generated.mexc_market_data_pb2 import (
    PushDataV3ApiWrapper,
)
from tests.fakes import FakeProducer

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "config/feeds.example.yaml"


@pytest.mark.parametrize("source", ["binance", "mexc"])
def test_queue_timeout_fails_run_without_reopening_socket(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    sockets = ExchangeSockets()
    producer, _ = install_fakes(monkeypatch, sockets)

    def full(*args: object, **kwargs: object) -> None:
        raise BufferError("queue full")

    monkeypatch.setattr(producer, "produce", full)

    async def scenario() -> None:
        async with asyncio.timeout(1):
            with pytest.raises(KafkaPublishError):
                await main.run(
                    (FeedSpec(source, "BTCUSDT"),),
                    1,
                    Settings(_env_file=None, kafka_enqueue_timeout_seconds=0.01),
                )
        assert asyncio.all_tasks() == {asyncio.current_task()}

    asyncio.run(scenario())
    assert sockets.opened == sockets.closed == 1
    assert producer.flush_timeout == 10


class ExchangeSockets:
    def __init__(self, fail_once: str | None = None) -> None:
        self.fail_once = fail_once
        self.opened = 0
        self.closed = 0
        self.attempts: dict[str, int] = {}
        self.all_open = asyncio.Event()

    def connect(self, url: str, **_: Any) -> "ExchangeSocket":
        return ExchangeSocket(self, url)


class ExchangeSocket:
    def __init__(self, owner: ExchangeSockets, url: str) -> None:
        self.owner = owner
        self.symbol = url.rsplit("/", 1)[-1].split("@")[0].upper()
        self.binance = "@trade" in url
        self.messages: list[str | bytes] = []
        self.failure_pending = False

    async def __aenter__(self) -> "ExchangeSocket":
        self.owner.opened += 1
        if self.owner.opened >= 4:
            self.owner.all_open.set()
        if self.binance:
            payload = json.loads(
                (ROOT / "tests/fixtures/binance/btcusdt_trade.json").read_text()
            )
            payload["s"] = self.symbol
            self.messages.append(json.dumps(payload))
            feed_id = f"binance:{self.symbol}:trade"
            attempt = self.owner.attempts.get(feed_id, 0) + 1
            self.owner.attempts[feed_id] = attempt
            self.failure_pending = feed_id == self.owner.fail_once and attempt == 1
        return self

    async def __aexit__(self, *_: object) -> None:
        self.owner.closed += 1

    async def send(self, message: str | bytes) -> None:
        request = json.loads(message)
        if request.get("method") != "SUBSCRIPTION":
            return
        channel = request["params"][0]
        self.symbol = channel.rsplit("@", 1)[-1]
        self.messages.append(json.dumps({"code": 0, "msg": channel}))
        encoded = (ROOT / "tests/fixtures/mexc/btcusdt_aggre_deals.pb.b64").read_text()
        wrapper = PushDataV3ApiWrapper.FromString(base64.b64decode(encoded))
        wrapper.symbol = self.symbol
        wrapper.channel = channel
        self.messages.append(wrapper.SerializeToString())

    async def recv(self) -> str | bytes:
        await asyncio.sleep(0)
        if self.failure_pending:
            self.failure_pending = False
            raise ConnectionError("simulated disconnect for one Binance symbol")
        if self.messages:
            return self.messages.pop(0)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def install_fakes(
    monkeypatch: pytest.MonkeyPatch, sockets: ExchangeSockets
) -> tuple[FakeProducer, list[tuple[str, str]]]:
    producer = FakeProducer()
    created: list[tuple[str, str]] = []

    def build(bootstrap: str, client_id: str) -> FakeProducer:
        created.append((bootstrap, client_id))
        return producer

    monkeypatch.setattr(main, "build_producer", build)
    monkeypatch.setattr(binance_client, "connect", sockets.connect)
    monkeypatch.setattr(mexc_client, "connect", sockets.connect)
    return producer, created


@pytest.mark.parametrize("fail_once", [None, "binance:ETHUSDT:trade"])
def test_four_feed_run_uses_one_publisher_and_independent_limits(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    fail_once: str | None,
) -> None:
    sockets = ExchangeSockets(fail_once)
    producer, created = install_fakes(monkeypatch, sockets)
    feeds = build_feed_plan(load_configuration(EXAMPLE))
    settings = Settings(
        _env_file=None,
        producer_reconnect_base_seconds=0.001,
        producer_reconnect_max_seconds=0.001,
    )

    async def scenario() -> None:
        async with asyncio.timeout(2):
            await main.run(feeds, 1, settings)

    with caplog.at_level("INFO"):
        asyncio.run(scenario())

    assert len(created) == 1
    assert sockets.opened == sockets.closed == (5 if fail_once else 4)
    assert producer.flush_timeout == settings.kafka_flush_timeout_seconds
    assert len(producer.records) == 4
    for source in ("binance", "mexc"):
        for symbol in ("BTCUSDT", "ETHUSDT"):
            events = [
                json.loads(value)
                for topic, key, value in producer.records
                if topic == "raw-market-frames-v1" and key == f"{source}:{symbol}"
            ]
            assert len(events) == 1
            assert all(
                event["source"] == source and event["subscription_symbol"] == symbol
                for event in events
            )
            assert f"feed_id={source}:{symbol}:trade" in caplog.text
    assert "failed=0" in caplog.text
    if fail_once:
        assert "source_reconnect source=binance symbol=ETHUSDT" in caplog.text


@pytest.mark.parametrize("cancel", [False, True])
def test_stop_and_cancellation_close_all_sessions_and_flush(
    monkeypatch: pytest.MonkeyPatch, cancel: bool
) -> None:
    sockets = ExchangeSockets()
    producer, _ = install_fakes(monkeypatch, sockets)
    captured: list[asyncio.Event] = []
    restored: list[bool] = []

    def install(stop: asyncio.Event):
        captured.append(stop)
        return lambda: restored.append(True)

    monkeypatch.setattr(main, "install_signal_handlers", install)

    async def scenario() -> None:
        task = asyncio.create_task(
            main.run(
                build_feed_plan(load_configuration(EXAMPLE)),
                None,
                Settings(_env_file=None),
            )
        )
        async with asyncio.timeout(2):
            await sockets.all_open.wait()
            if cancel:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                captured[0].set()
                await task
        assert asyncio.all_tasks() == {asyncio.current_task()}

    asyncio.run(scenario())
    assert sockets.opened == sockets.closed == 4
    assert producer.flush_timeout == 10
    assert restored == [True]


def test_invalid_plan_never_creates_a_kafka_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, created = install_fakes(monkeypatch, ExchangeSockets())
    with pytest.raises(ValueError):
        asyncio.run(main.run((), 1, Settings(_env_file=None)))
    assert not created


def test_session_preparation_error_still_flushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer, _ = install_fakes(monkeypatch, ExchangeSockets())

    def fail(*_: Any) -> None:
        raise RuntimeError("preparation failed")

    monkeypatch.setattr(main, "build_sessions", fail)
    with pytest.raises(RuntimeError, match="preparation failed"):
        asyncio.run(
            main.run((FeedSpec("binance", "BTCUSDT"),), 1, Settings(_env_file=None))
        )
    assert producer.flush_timeout == 10


def test_yaml_dry_run_overrides_environment_symbols_without_network(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _, created = install_fakes(monkeypatch, ExchangeSockets())
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: Settings(_env_file=None, market_symbols="DOGEUSDT"),
    )
    monkeypatch.setattr(
        "sys.argv", ["ingestion", "--config", str(EXAMPLE), "--dry-run"]
    )
    main.main()
    plan = json.loads(capsys.readouterr().out)
    assert {item["symbol"] for item in plan} == {"BTCUSDT", "ETHUSDT"}
    assert len(plan) == 4
    assert not created


def test_source_mode_ignores_yaml_and_uses_environment_symbols(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _, created = install_fakes(monkeypatch, ExchangeSockets())
    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: Settings(_env_file=None, market_symbols="SOLUSDT,ETHUSDT"),
    )
    monkeypatch.setattr("sys.argv", ["ingestion", "--source", "binance", "--dry-run"])
    main.main()
    plan = json.loads(capsys.readouterr().out)
    assert {item["symbol"] for item in plan} == {"SOLUSDT", "ETHUSDT"}
    assert {item["exchange"] for item in plan} == {"binance"}
    assert not created


@pytest.mark.parametrize(
    "arguments",
    [
        ["--config", str(EXAMPLE), "--source", "both"],
        ["--max-events", "0"],
        ["--max-events", "-1"],
        ["--source", "kraken"],
    ],
)
def test_invalid_cli_arguments_fail_before_connections(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    _, created = install_fakes(monkeypatch, ExchangeSockets())
    monkeypatch.setattr("sys.argv", ["ingestion", *arguments])
    with pytest.raises(SystemExit) as error:
        main.main()
    assert error.value.code == 2
    assert not created


def test_missing_default_yaml_fails_without_legacy_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, created = install_fakes(monkeypatch, ExchangeSockets())
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["ingestion", "--dry-run"])
    with pytest.raises(SystemExit) as error:
        main.main()
    assert error.value.code == 2
    assert not created
