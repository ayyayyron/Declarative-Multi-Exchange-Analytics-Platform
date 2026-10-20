from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt

from common.contracts.types import Exchange, FailureStage, PayloadEncoding


class DeadLetterEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[2] = 2
    failed_at_ms: PositiveInt
    failure_stage: FailureStage
    failure_type: str = Field(min_length=1)
    failure_message: str = Field(min_length=1)
    source: Exchange | Literal["unknown"]
    source_channel: str | None = None
    original_payload_encoding: PayloadEncoding | Literal["unknown"]
    original_payload: str = Field(min_length=1)
    kafka_topic: str | None = None
    kafka_partition: NonNegativeInt | None = None
    kafka_offset: NonNegativeInt | None = None
    replay_status: Literal["pending", "skipped", "replayed"] = "pending"
