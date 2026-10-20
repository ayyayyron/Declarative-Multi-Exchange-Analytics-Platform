"""Operational settings shared by the host applications; market watchlists live
in YAML.
"""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import PositiveFloat, PositiveInt, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_raw_trades_topic: str = "raw-trades"
    kafka_raw_frames_topic: str = "raw-market-frames-v1"
    kafka_dlq_topic: str = "raw-trades-dlq"
    kafka_producer_client_id: str = "streaming-market-producer"
    kafka_flush_timeout_seconds: PositiveInt = 10
    producer_reconnect_base_seconds: PositiveFloat = 1
    producer_reconnect_max_seconds: PositiveFloat = 30
    producer_metrics_interval_seconds: PositiveFloat = 10

    binance_ws_base_url: str = "wss://data-stream.binance.vision/ws"
    mexc_ws_url: str = "wss://wbs-api.mexc.com/ws"
    market_symbols: str = "BTCUSDT"
    mexc_trade_update_speed: Literal["100ms", "10ms"] = "100ms"
    mexc_heartbeat_interval_seconds: PositiveFloat = 20

    postgres_dsn: PostgresDsn = PostgresDsn(
        "postgresql://streaming:streaming-local-only@localhost:5432/streaming"
    )

    local_data_dir: Path = Path("data")
    checkpoint_base_dir: Path = Path("checkpoints")

    spark_master: str = "local[2]"
    spark_driver_memory: str = "1g"
    spark_shuffle_partitions: PositiveInt = 4
    spark_starting_offsets: Literal["earliest", "latest"] = "earliest"
    spark_max_offsets_per_trigger: PositiveInt = 20_000
    spark_trigger_interval_seconds: PositiveInt = 5
    candle_watermark_delay_seconds: PositiveInt = 120
    live_pipeline_fail_on_data_loss: bool = False
    serving_mode: Literal["raw", "canonical"] = "raw"

    live_trade_retention_minutes: PositiveInt = 60
    dashboard_symbol: str = "BTCUSDT"
    dashboard_refresh_seconds: PositiveInt = 3
    dashboard_stale_after_seconds: PositiveInt = 15
    dashboard_chart_points: PositiveInt = 2_000

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
    """Load and cache one settings instance per process."""
    return Settings()
