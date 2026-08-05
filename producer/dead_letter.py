import base64

from common.models import DeadLetterEvent, Exchange, FailureStage, PayloadEncoding

MAX_FAILURE_MESSAGE_LENGTH = 500


def build_dead_letter(
    source: Exchange,
    source_channel: str,
    payload: str | bytes,
    error: Exception,
    failure_stage: FailureStage,
    failed_at_ms: int,
) -> DeadLetterEvent:
    original_payload, encoding = encode_payload(source, payload)
    message = str(error)[:MAX_FAILURE_MESSAGE_LENGTH] or type(error).__name__
    return DeadLetterEvent(
        failed_at_ms=failed_at_ms,
        failure_stage=failure_stage,
        failure_type=type(error).__name__,
        failure_message=message,
        source=source,
        source_channel=source_channel,
        original_payload_encoding=encoding,
        original_payload=original_payload,
    )


def encode_payload(
    source: Exchange,
    payload: str | bytes,
) -> tuple[str, PayloadEncoding | str]:
    if isinstance(payload, str):
        return payload or "<empty>", "utf8-json"
    encoded = base64.b64encode(payload).decode("ascii") or "<empty>"
    if source == "mexc":
        return encoded, "base64-protobuf"
    return encoded, "unknown"
