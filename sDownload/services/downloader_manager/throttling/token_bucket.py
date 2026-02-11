import asyncio
import time
import logging
from collections.abc import AsyncGenerator
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
)
from .base import ThrottlerProtocol

logger = logging.getLogger(__name__)


class TokenBucketThrottler(ThrottlerProtocol):
    """
    Smoother implementation using the Token Bucket algorithm.
    Allows for small bursts while maintaining a consistent average speed.
    Zero clock calls on unlimited downloads.
    """

    def __init__(
        self,
        burst_seconds: float = 1.0,
        min_chunk_size: int = 32768,
        max_sleep_seconds: float = 1.0,
    ):
        self.burst_seconds = burst_seconds
        self.min_chunk_size = min_chunk_size
        self.max_sleep_seconds = max_sleep_seconds

    async def wrap(
        self, it: AsyncGenerator[bytes, None], stats: ChunkDownloadStats
    ) -> AsyncGenerator[bytes, None]:
        try:
            last_check = time.monotonic()
            tokens = 0.0
            accumulated_since_check = 0
            last_target = None

            async for data in it:
                size = len(data)
                stats.add_qt_bytes_downloaded(size)
                yield data

                target = stats.target_speed_bps

                if target is None:
                    continue

                if target != last_target:
                    tokens = target * self.burst_seconds
                    last_check = time.monotonic()
                    accumulated_since_check = 0
                    last_target = target

                accumulated_since_check += size

                threshold = max(self.min_chunk_size, target * 0.05)
                if accumulated_since_check < threshold:
                    continue

                now = time.monotonic()
                elapsed = now - last_check
                capacity = target * self.burst_seconds
                tokens += elapsed * target
                if tokens > capacity:
                    tokens = capacity
                last_check = now

                tokens -= accumulated_since_check
                accumulated_since_check = 0

                if tokens < 0:
                    wait_time = abs(tokens) / target
                    await asyncio.sleep(min(wait_time, self.max_sleep_seconds))
                    last_check = time.monotonic()
                    tokens = 0.0
        finally:
            logger.info("TokenBucketThrottler finished - %s", stats.chunk_file_name)
