"""Load and validate the supported watchlist schema before opening connections."""

import re
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from yaml.constructor import ConstructorError

from common.contracts import Exchange
from ingestion.feed_plan import MAX_FEEDS, build_feed_plan

MAX_CONFIG_BYTES = 1_048_576


class ConfigurationError(ValueError):
    pass


class _UniqueKeyLoader(yaml.SafeLoader):
    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[Any, Any]:
        keys: set[str] = set()
        for key_node, _ in node.value:
            if (
                not isinstance(key_node, yaml.ScalarNode)
                or key_node.tag != "tag:yaml.org,2002:str"
            ):
                raise ConstructorError(
                    None, None, "YAML mapping keys must be strings", key_node.start_mark
                )
            key = key_node.value
            if key in keys:
                raise ConstructorError(
                    None, None, f"Duplicate YAML key: {key!r}", key_node.start_mark
                )
            keys.add(key)
        return super().construct_mapping(node, deep=deep)


class ExchangeConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    transport: Literal["websocket"] = "websocket"
    market_type: Literal["spot"] = "spot"


class Watchlist(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    exchanges: tuple[Exchange, ...] = Field(min_length=1, max_length=2)
    pairs: tuple[str, ...] = Field(min_length=1, max_length=MAX_FEEDS)
    feeds: tuple[Literal["trades"], ...] = Field(
        default=("trades",), min_length=1, max_length=1
    )

    @field_validator("pairs")
    @classmethod
    def normalize_pairs(cls, pairs: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(pair.strip().upper() for pair in pairs)
        for pair in normalized:
            if not re.fullmatch(r"[A-Z0-9]{1,19}/[A-Z0-9]{1,19}", pair):
                raise ValueError(
                    "Pairs must use BASE/QUOTE notation, for example BTC/USDT"
                )
            base, quote = pair.split("/")
            if base == quote or not 3 <= len(base + quote) <= 20:
                raise ValueError(f"Invalid spot pair: {pair}")
        if len(set(normalized)) != len(normalized):
            raise ValueError("Duplicate pairs in watchlist")
        return normalized

    @field_validator("exchanges")
    @classmethod
    def unique_exchanges(cls, exchanges: tuple[Exchange, ...]) -> tuple[Exchange, ...]:
        if len(set(exchanges)) != len(exchanges):
            raise ValueError("Duplicate exchanges in watchlist")
        return exchanges


class IngestionConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal[1]
    exchanges: dict[Exchange, ExchangeConfiguration] = Field(min_length=1, max_length=2)
    watchlists: tuple[Watchlist, ...] = Field(min_length=1, max_length=MAX_FEEDS)

    @field_validator("version", mode="before")
    @classmethod
    def supported_version(cls, version: object) -> object:
        if type(version) is not int or version != 1:
            raise ValueError("Only configuration version 1 is supported")
        return version

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        names = [watchlist.name for watchlist in self.watchlists]
        if len(set(names)) != len(names):
            raise ValueError("Watchlist names must be unique")
        build_feed_plan(self)
        return self


def load_configuration(path: Path) -> IngestionConfiguration:
    """Read bounded, safe YAML and reject unsupported or overlapping feed
    definitions.
    """
    try:
        with path.open("rb") as stream:
            payload = stream.read(MAX_CONFIG_BYTES + 1)
        if len(payload) > MAX_CONFIG_BYTES:
            raise ConfigurationError("Configuration must be at most 1 MiB")
        document = yaml.load(payload, Loader=_UniqueKeyLoader)
        return IngestionConfiguration.model_validate(document)
    except ValidationError as error:
        details = "; ".join(
            f"{'.'.join(map(str, item['loc'])) or 'configuration'}: {item['msg']}"
            for item in error.errors(include_input=False, include_url=False)
        )
        raise ConfigurationError(
            f"Invalid ingestion configuration {path}: {details}"
        ) from error
    except (OSError, yaml.YAMLError, RecursionError) as error:
        raise ConfigurationError(
            f"Cannot load ingestion configuration {path}: {error}"
        ) from error
