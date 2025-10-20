import logging
import threading
from typing import Optional

_LIB_LOGGER_NAME = "sDownload"
_LIB_LOGGER_SINGLETON: Optional[logging.Logger] = None
_LOGGER_LOCK = threading.Lock()


def get_logger() -> logging.Logger:
    global _LIB_LOGGER_SINGLETON
    if _LIB_LOGGER_SINGLETON is None:
        with _LOGGER_LOCK:
            if _LIB_LOGGER_SINGLETON is None:
                logger = logging.getLogger(_LIB_LOGGER_NAME)
                logger.setLevel(logging.INFO)
                if not logger.hasHandlers():
                    handler = logging.StreamHandler()
                    formatter = logging.Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    )
                    handler.setFormatter(formatter)
                    logger.addHandler(handler)

                _LIB_LOGGER_SINGLETON = logger
    return _LIB_LOGGER_SINGLETON


def set_logger(logger: logging.Logger) -> None:
    global _LIB_LOGGER_SINGLETON
    with _LOGGER_LOCK:
        _LIB_LOGGER_SINGLETON = logger
