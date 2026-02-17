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
    is_active = True
    try:
        while is_active:
            total_speed = 0.0

            active_stats = [
                s
                for s in chunks_stats.values()
                if s.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.PENDING)
            ]

            downloading_stats = [
                s for s in active_stats if s.status == EDownloadStatus.DOWNLOADING
            ]

            if downloading_stats:
                for stats in downloading_stats:
                    stats.update()
                    total_speed += stats.speed_bps

                logger.info(
                    "(%s) SPEED: %.2f MB/s | Active Chunks: %d",
                    file_name,
                    total_speed / (1024 * 1024),
                    len(active_stats),
                )

                if logger.isEnabledFor(logging.DEBUG):
                    for stats in downloading_stats:
                        logger.debug(
                            " └──▶ Chunk [%d-%d] %.1f%% @ %.2f MB/s",
                            stats.range.start,
                            stats.range.end,
                            stats.progress,
                            stats.speed_bps / (1024 * 1024),
                        )
            elif not active_stats:
                logger.debug("(%s) No active chunks. Stopping monitor.", file_name)
                is_active = False

            if is_active:
                await asyncio.sleep(interval)

    except asyncio.CancelledError:
        logger.debug("(%s) Monitor cancelled.", file_name)
