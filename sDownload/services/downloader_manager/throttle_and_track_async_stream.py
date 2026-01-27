from collections.abc import AsyncGenerator
import time
import asyncio
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
)
from sDownload.telemetry.logger import get_logger

_logger = get_logger()


async def throttle_and_track_async_stream(
    it: AsyncGenerator[bytes, None], stats: ChunkDownloadStats
):
    try:
        start_time = time.monotonic()
        accumulated_bytes = 0
        async for data in it:
            qt_bytes = len(data)
            stats.add_qt_bytes_downloaded(qt_bytes)
            accumulated_bytes += qt_bytes
            yield data
            if accumulated_bytes > stats.target_speed_bps:
                time_elapsed = time.monotonic() - start_time
                time_expected = accumulated_bytes / stats.target_speed_bps
                if time_elapsed < time_expected:
                    await asyncio.sleep(min(1, time_expected - time_elapsed))
                start_time = time.monotonic()
                accumulated_bytes = 0
    finally:
        _logger.info("AsyncIterator finished - %s", stats.chunk_file_name)
