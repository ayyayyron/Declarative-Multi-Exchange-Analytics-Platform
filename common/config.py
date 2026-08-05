from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import PositiveInt, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_raw_trades_topic: str = "raw-trades"
    kafka_dlq_topic: str = "raw-trades-dlq"
    kafka_producer_client_id: str = "streaming-market-producer"
    kafka_flush_timeout_seconds: PositiveInt = 10

    binance_ws_base_url: str = "wss://data-stream.binance.vision/ws"
    mexc_ws_url: str = "wss://wbs-api.mexc.com/ws"
    market_symbols: str = "BTCUSDT"
    mexc_trade_update_speed: Literal["100ms", "10ms"] = "100ms"

    postgres_dsn: PostgresDsn = PostgresDsn(
        "postgresql://streaming:streaming-local-only@localhost:5432/streaming"
    )

    local_data_dir: Path = Path("data")
    checkpoint_base_dir: Path = Path("checkpoints")

    spark_master: str = "local[2]"
    spark_driver_memory: str = "1g"
    spark_shuffle_partitions: PositiveInt = 4

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @field_validator("market_symbols")
    @classmethod
    def validate_symbol_csv(cls, value: str) -> str:
        symbols = [symbol.strip().upper() for symbol in value.split(",")]
        if not symbols or any(not symbol or not symbol.isalnum() for symbol in symbols):
            raise ValueError(
                "MARKET_SYMBOLS must contain non-empty alphanumeric symbols"
            )
        return ",".join(symbols)

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(self.market_symbols.split(","))

    @property
    def binance_streams(self) -> tuple[str, ...]:
        return tuple(f"{symbol.lower()}@trade" for symbol in self.symbols)

    @property
    def mexc_trade_channels(self) -> tuple[str, ...]:
        return tuple(
            "spot@public.aggre.deals.v3.api.pb@"
            f"{self.mexc_trade_update_speed}@{symbol}"
            for symbol in self.symbols
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
