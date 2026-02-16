import asyncio
import logging
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    EDownloadStatus,
)
from sDownload.interfaces.protocols.downloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.services.downloader_manager.throttle_and_track_async_stream import (
    throttle_and_track_async_stream,
)

logger = logging.getLogger(__name__)


async def download_chunk_supervised(
    downloader: DownloaderProtocol,
    storage: FileStorageProtocol,
    stats: ChunkDownloadStats,
    download_url: str,
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

        tracked = throttle_and_track_async_stream(raw_it, stats)
        await storage.save_binary_data(stats.chunk_file_name, tracked)

        if stats.file_size is not None and stats.bytes_downloaded != stats.file_size:
            raise IOError(
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
        stats.set_status(EDownloadStatus.ERROR)
        logger.warning("[%s] download failed: %s", stats.chunk_file_name, e)
        raise
    finally:
        stats.update()
