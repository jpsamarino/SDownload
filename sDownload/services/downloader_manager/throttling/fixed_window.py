import asyncio
import logging
import time
from collections.abc import AsyncGenerator

from sDownload.interfaces.models import ChunkDownloadStats
from sDownload.interfaces.protocols import ThrottlerProtocol

logger = logging.getLogger(__name__)


class FixedWindowThrottler(ThrottlerProtocol):
    """
    Lightweight fixed window throttler.
    Zero clock calls on unlimited downloads.
    """

    def __init__(
        self,
        interval_ms: int = 200,
        min_chunk_size: int = 32768,
        max_sleep_seconds: float = 1.0,
    ):
        self.interval_s = interval_ms / 1000.0
        self.min_chunk_size = min_chunk_size
        self.max_sleep_seconds = max_sleep_seconds

    async def wrap(
        self, it: AsyncGenerator[bytes, None], stats: ChunkDownloadStats
    ) -> AsyncGenerator[bytes, None]:
        try:
            last_check = time.monotonic()
            accumulated = 0
            last_target = None

            def calc_threshold(target_speed):
                if not target_speed:
                    return 0
                return max(self.min_chunk_size, target_speed * self.interval_s)

            byte_threshold = calc_threshold(stats.target_speed_bps)

            async for data in it:
                size = len(data)
                stats.add_qt_bytes_downloaded(size)
                yield data

                target = stats.target_speed_bps

                if target is None:
                    continue

                if target != last_target:
                    byte_threshold = calc_threshold(target)
                    last_check = time.monotonic()
                    accumulated = 0
                    last_target = target

                accumulated += size
                if accumulated >= byte_threshold:
                    now = time.monotonic()
                    elapsed = now - last_check
                    expected = accumulated / target

                    if elapsed < expected:
                        await asyncio.sleep(min(expected - elapsed, self.max_sleep_seconds))
                        last_check = time.monotonic()
                    else:
                        last_check = now

                    accumulated = 0
                    byte_threshold = calc_threshold(target)
        finally:
            logger.info("FixedWindowThrottler finished - %s", stats.chunk_file_name)
