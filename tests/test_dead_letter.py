from producer.dead_letter import MAX_FAILURE_MESSAGE_LENGTH, build_dead_letter


def test_dead_letter_limits_error_context_without_losing_payload() -> None:
    event = build_dead_letter(
        source="binance",
        source_channel="btcusdt@trade",
        payload='{"bad":true}',
        error=ValueError("x" * 1000),
        failure_stage="contract_validation",
        failed_at_ms=1_735_689_600_150,
    )

    assert len(event.failure_message) == MAX_FAILURE_MESSAGE_LENGTH
    assert event.original_payload == '{"bad":true}'
    assert event.original_payload_encoding == "utf8-json"
