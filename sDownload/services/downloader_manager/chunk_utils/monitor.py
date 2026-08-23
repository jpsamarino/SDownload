import asyncio
import logging

from sDownload.interfaces.models import ChunkDownloadStats, ChunkRange, EDownloadStatus

logger = logging.getLogger(__name__)


async def monitor_download_progress(
    chunks_stats: dict[ChunkRange, ChunkDownloadStats],
    file_name: str,
    interval: float = 0.5,
) -> None:
    """
    Periodically updates the download progress stats with a high-performance single-pass loop.
    """
    is_active = True
    try:
        while is_active:
            total_speed = 0.0
            pending_count = 0
            downloading_stats = []

            for stats in chunks_stats.values():
                status = stats.status
                if status == EDownloadStatus.DOWNLOADING:
                    stats.update()
                    total_speed += stats.speed_bps
                    downloading_stats.append(stats)
                elif status == EDownloadStatus.PENDING:
                    pending_count += 1

            downloading_count = len(downloading_stats)
            is_active = (downloading_count + pending_count) > 0

            if downloading_count > 0:
                logger.info(
                    "(%s) SPEED: %.2f MB/s | Active: %d | Pending: %d",
                    file_name,
                    total_speed / (1024 * 1024),
                    downloading_count,
                    pending_count,
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
            elif not is_active:
                logger.debug("(%s) No active or pending chunks. Stopping monitor.", file_name)

            if is_active:
                await asyncio.sleep(interval)

    except asyncio.CancelledError:
        logger.debug("(%s) Monitor cancelled.", file_name)
