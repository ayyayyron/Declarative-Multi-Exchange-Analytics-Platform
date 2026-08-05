import json

import pytest

from producer.adapters.base import SourcePayloadError
from producer.mexc_client import (
    build_mexc_subscription,
    validate_mexc_subscription,
)

CHANNEL = "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"


def test_mexc_subscription_uses_configured_channel() -> None:
    assert json.loads(build_mexc_subscription(CHANNEL)) == {
        "method": "SUBSCRIPTION",
        "params": [CHANNEL],
    }


def test_mexc_subscription_accepts_matching_acknowledgement() -> None:
    validate_mexc_subscription(
        json.dumps({"id": 0, "code": 0, "msg": CHANNEL}),
        CHANNEL,
    )


@pytest.mark.parametrize(
    "response",
    [
        "not-json",
        "[]",
        json.dumps({"id": 0, "code": 1, "msg": CHANNEL}),
        json.dumps({"id": 0, "code": 0, "msg": "other-channel"}),
    ],
)
def test_mexc_subscription_rejects_invalid_acknowledgement(response: str) -> None:
    with pytest.raises(SourcePayloadError):
        validate_mexc_subscription(response, CHANNEL)
