import asyncio
import logging
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    EDownloadStatus,
)

logger = logging.getLogger(__name__)


async def monitor_download_progress(
    chunks_stats: dict[ChunkRange, ChunkDownloadStats],
    file_name: str,
    interval: float = 0.5,
) -> None:
    """
    Periodically updates the download progress stats.
    """
    try:
        while True:
            total_speed = 0.0
            active_count = 0

            active_stats = [
                s
                for s in chunks_stats.values()
                if s.status == EDownloadStatus.DOWNLOADING
            ]
            active_count = len(active_stats)

            if active_count > 0:

                for stats in active_stats:
                    stats.update()
                    total_speed += stats.speed_bps

                logger.info(
                    "(%s) SPEED: %.2f MB/s | Active Chunks: %d",
                    file_name,
                    total_speed / (1024 * 1024),
                    active_count,
                )

                if logger.isEnabledFor(logging.DEBUG):
                    for stats in active_stats:
                        logger.debug(
                            " └──▶ Chunk [%d-%d] %.1f%% @ %.2f MB/s",
                            stats.range.start,
                            stats.range.end,
                            stats.progress,
                            stats.speed_bps / (1024 * 1024),
                        )
            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        pass
