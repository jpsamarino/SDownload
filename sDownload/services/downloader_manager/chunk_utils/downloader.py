import asyncio
import logging
from sDownload.interfaces.models import ChunkRange, ChunkDownloadStats, EDownloadStatus
from sDownload.interfaces.protocols import (
    DownloaderProtocol,
    FileStorageProtocol,
    ThrottlerProtocol,
)
from sDownload.exceptions import IntegrityError

logger = logging.getLogger(__name__)


async def download_chunk_supervised(
    downloader: DownloaderProtocol,
    storage: FileStorageProtocol,
    stats: ChunkDownloadStats,
    download_url: str,
    throttler: ThrottlerProtocol,
    init_signal: asyncio.Event | None = None,
) -> ChunkRange | None:
    """
    Executes a supervised download of a single chunk.
    """
    stats.set_status(EDownloadStatus.DOWNLOADING)
    if init_signal:
        init_signal.set()

    try:
        logger.info(
            "byte range [%s]-[%s] starting download",
            stats.range.start,
            stats.range.end or "EOF",
        )

        raw_it = downloader.download_chunk(
            download_url, stats.range.start, stats.range.end
        )

        tracked = throttler.wrap(raw_it, stats)

        await storage.save_binary_data(stats.chunk_file_name, tracked)

        if stats.file_size is not None and stats.bytes_downloaded != stats.file_size:
            raise IntegrityError(
                f"Chunk size error: expected {stats.file_size} bytes, "
                f"got {stats.bytes_downloaded} bytes"
            )

        stats.set_status(EDownloadStatus.COMPLETED)
        logger.info("[%s] download completed", stats.chunk_file_name)
        return stats.range

    except asyncio.CancelledError:
        if stats.limit_qt_bytes and stats.bytes_downloaded >= stats.limit_qt_bytes:
            if stats.status != EDownloadStatus.DEPRECATED:
                stats.set_status(EDownloadStatus.DEPRECATED)
            logger.info(
                "[%s] goal reached, marked as DEPRECATED.", stats.chunk_file_name
            )
        else:
            stats.set_status(EDownloadStatus.CANCELLED)
            logger.warning("[%s] download cancelled", stats.chunk_file_name)
        raise

    except Exception as e:
        stats.set_error(e)
        logger.warning("[%s] download failed: %s", stats.chunk_file_name, e)
        raise
    finally:
        stats.update()
