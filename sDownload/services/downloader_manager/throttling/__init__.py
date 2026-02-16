from .base import ThrottlerProtocol
from .fixed_window import FixedWindowThrottler
from .token_bucket import TokenBucketThrottler

_DEFAULT_THROTTLER = FixedWindowThrottler()


def get_default_throttler() -> ThrottlerProtocol:
    return _DEFAULT_THROTTLER


def set_default_throttler(throttler: ThrottlerProtocol) -> None:
    global _DEFAULT_THROTTLER
    _DEFAULT_THROTTLER = throttler


__all__ = [
    "ThrottlerProtocol",
    "FixedWindowThrottler",
    "TokenBucketThrottler",
    "get_default_throttler",
    "set_default_throttler",
]
