from producer.binance_client import build_binance_stream_url


def test_binance_stream_url_uses_raw_stream_path() -> None:
    assert (
        build_binance_stream_url(
            "wss://data-stream.binance.vision/ws",
            "btcusdt@trade",
        )
        == "wss://data-stream.binance.vision/ws/btcusdt@trade"
    )
