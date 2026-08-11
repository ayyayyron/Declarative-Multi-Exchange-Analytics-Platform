import signal
from threading import Event
from types import FrameType

from pyspark.sql.streaming.query import StreamingQuery

from common.logging_config import get_logger

LOGGER = get_logger(__name__)


def await_query_termination(query: StreamingQuery) -> None:
    stop_requested = Event()

    def request_stop(_: int, __: FrameType | None) -> None:
        stop_requested.set()

    previous_sigint = signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)
    try:
        while query.isActive and not stop_requested.is_set():
            query.awaitTermination(1)
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        if query.isActive:
            LOGGER.info("Stopping Spark query: %s", query.name)
            query.stop()
