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
    """

    def __init__(self, burst_seconds: float = 1.0):
        self.burst_seconds = burst_seconds

    async def wrap(
        self, it: AsyncGenerator[bytes, None], stats: ChunkDownloadStats
    ) -> AsyncGenerator[bytes, None]:
        if stats.target_speed_bps == float("inf"):
            async for data in it:
                stats.add_qt_bytes_downloaded(len(data))
                yield data
            return

        last_check = time.monotonic()
        tokens = stats.target_speed_bps
        capacity = stats.target_speed_bps * self.burst_seconds

        try:
            async for data in it:
                size = len(data)
                stats.add_qt_bytes_downloaded(size)

                now = time.monotonic()
                tokens += (now - last_check) * stats.target_speed_bps
                last_check = now

                if tokens > capacity:
                    tokens = capacity

                tokens -= size

                if tokens < 0:
                    wait_time = abs(tokens) / stats.target_speed_bps
                    await asyncio.sleep(min(5, wait_time))
                    tokens = 0
                    last_check = time.monotonic()

                yield data
        finally:
            logger.info("TokenBucketThrottler finished - %s", stats.chunk_file_name)
