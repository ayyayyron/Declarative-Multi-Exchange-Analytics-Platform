from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ingestion.configuration import (
    MAX_CONFIG_BYTES,
    ConfigurationError,
    IngestionConfiguration,
    load_configuration,
)
from ingestion.feed_plan import build_feed_plan


def document() -> dict[str, Any]:
    return {
        "version": 1,
        "exchanges": {"binance": {}, "mexc": {}},
        "watchlists": [
            {
                "name": "test",
                "exchanges": ["binance", "mexc"],
                "pairs": ["BTC/USDT", "ETH/USDT"],
                "feeds": ["trades"],
            }
        ],
    }


def test_pairs_are_normalized_before_planning() -> None:
    value = document()
    value["watchlists"][0]["pairs"] = [" btc/usdt "]
    config = IngestionConfiguration.model_validate(value)
    assert config.watchlists[0].pairs == ("BTC/USDT",)
    assert [feed.symbol for feed in build_feed_plan(config)] == ["BTCUSDT", "BTCUSDT"]


@pytest.mark.parametrize("version", [0, 2, True, "1", 1.0, None])
def test_unsupported_versions_are_rejected(version: object) -> None:
    value = document()
    value["version"] = version
    with pytest.raises(ValidationError, match="version 1"):
        IngestionConfiguration.model_validate(value)


@pytest.mark.parametrize(
    "pair",
    ["BTCUSDT", "BTC-USD", "BTC/BTC", "", "BTC/", "BTC/USDT/SOL", "A" * 20 + "/USDT"],
)
def test_invalid_pairs_are_rejected(pair: str) -> None:
    value = document()
    value["watchlists"][0]["pairs"] = [pair]
    with pytest.raises(ValidationError):
        IngestionConfiguration.model_validate(value)


@pytest.mark.parametrize("source", ["kraken", "okx", "unknown"])
def test_unimplemented_exchanges_are_rejected(source: str) -> None:
    value = document()
    value["exchanges"] = {source: {}}
    value["watchlists"][0]["exchanges"] = [source]
    with pytest.raises(ValidationError):
        IngestionConfiguration.model_validate(value)


@pytest.mark.parametrize(
    ("field", "unsupported"), [("transport", "rest"), ("market_type", "futures")]
)
def test_unsupported_connector_options_are_rejected(
    field: str, unsupported: str
) -> None:
    value = document()
    value["exchanges"]["binance"][field] = unsupported
    with pytest.raises(ValidationError):
        IngestionConfiguration.model_validate(value)


@pytest.mark.parametrize(
    "feeds", [["quotes"], ["trades", "quotes"], [], ["trades", "trades"]]
)
def test_only_trade_feeds_are_supported(feeds: list[str]) -> None:
    value = document()
    value["watchlists"][0]["feeds"] = feeds
    with pytest.raises(ValidationError):
        IngestionConfiguration.model_validate(value)


@pytest.mark.parametrize("level", ["root", "exchange", "watchlist"])
def test_unknown_fields_are_not_silently_ignored(level: str) -> None:
    value = document()
    target = (
        value
        if level == "root"
        else (
            value["exchanges"]["binance"]
            if level == "exchange"
            else value["watchlists"][0]
        )
    )
    target["typo"] = True
    with pytest.raises(ValidationError, match="Extra inputs"):
        IngestionConfiguration.model_validate(value)


@pytest.mark.parametrize(
    "kind", ["pairs", "exchanges", "names", "overlap", "undeclared", "empty"]
)
def test_inconsistent_selections_are_rejected(kind: str) -> None:
    value = document()
    if kind == "pairs":
        value["watchlists"][0]["pairs"] = ["BTC/USDT", "btc/usdt"]
    elif kind == "exchanges":
        value["watchlists"][0]["exchanges"] = ["binance", "binance"]
    elif kind in ("names", "overlap"):
        value["watchlists"].append(
            {**value["watchlists"][0], "name": "test" if kind == "names" else "other"}
        )
    elif kind == "undeclared":
        del value["exchanges"]["mexc"]
    else:
        value["watchlists"] = []
    with pytest.raises(ValidationError):
        IngestionConfiguration.model_validate(value)


def test_feed_limit_is_checked_before_connections() -> None:
    value = document()
    value["watchlists"][0]["pairs"] = [f"COIN{i}/USDT" for i in range(11)]
    with pytest.raises(ValidationError, match="between 1 and 20"):
        IngestionConfiguration.model_validate(value)


def test_twenty_feeds_are_allowed() -> None:
    value = document()
    value["watchlists"][0]["pairs"] = [f"COIN{i}/USDT" for i in range(10)]
    assert len(build_feed_plan(IngestionConfiguration.model_validate(value))) == 20


def test_loader_handles_yaml_from_disk(tmp_path: Path) -> None:
    path = tmp_path / "feeds.yaml"
    path.write_text(yaml.safe_dump(document()))
    assert len(build_feed_plan(load_configuration(path))) == 4


@pytest.mark.parametrize(
    "payload",
    [
        "",
        "[]",
        "watchlists: [",
        "version: 1\nversion: 1",
        "!!python/object/apply:builtins.str [test]",
        "? [a, b]\n: value",
        "version: &loop [*loop]",
    ],
)
def test_bad_yaml_is_reported_as_a_configuration_error(
    tmp_path: Path, payload: str
) -> None:
    path = tmp_path / "feeds.yaml"
    path.write_text(payload)
    with pytest.raises(ConfigurationError):
        load_configuration(path)


def test_missing_file_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="Cannot load"):
        load_configuration(tmp_path / "absent.yaml")


def test_oversized_file_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "feeds.yaml"
    path.write_bytes(b" " * (MAX_CONFIG_BYTES + 1))
    with pytest.raises(ConfigurationError, match="1 MiB"):
        load_configuration(path)
