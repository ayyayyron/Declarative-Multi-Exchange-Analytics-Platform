import logging
from typing import Final

LOG_FORMAT: Final = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def configure_logging(level: str = "INFO") -> None:
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Unknown log level: {level}")

    logging.basicConfig(level=numeric_level, format=LOG_FORMAT, force=True)
    logging.getLogger("py4j").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
