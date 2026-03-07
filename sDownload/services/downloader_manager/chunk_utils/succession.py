import asyncio
import logging
from sDownload.interfaces.models import ChunkRange, ChunkDownloadStats, EDownloadStatus
from sDownload.interfaces.protocols import FileStorageProtocol
from sDownload.exceptions import ChunkSuccessionError

logger = logging.getLogger(__name__)


async def run_chunk_succession(
    storage: FileStorageProtocol,
    stats_predecessor: ChunkDownloadStats,
    stats_successor: ChunkDownloadStats,
    predecessor_task: asyncio.Task | None,
    init_signal: asyncio.Event | None = None,
) -> ChunkRange:
    """
    Orchestrates the succession from one chunk to another (resizing).
    """
    if init_signal:
        init_signal.set()

    range_predecessor = stats_predecessor.range
    range_successor = stats_successor.range
    predecessor_error: Exception | None = None

    try:
        if stats_successor.status != EDownloadStatus.AWAITING_SUCCESSION:
            raise RuntimeError("Successor is not in AWAITING_SUCCESSION state")

        if predecessor_task:
            await asyncio.wait([predecessor_task], return_when=asyncio.FIRST_COMPLETED)
            try:
                await predecessor_task
            except asyncio.CancelledError:
                logger.info(
                    "Predecessor task %s cancelled (expected during succession).",
                    range_predecessor,
                )
            except Exception as e:
                logger.warning("Predecessor task %s failed: %s", range_predecessor, e)
                predecessor_error = e

        limit = stats_predecessor.limit_qt_bytes

        if predecessor_error:
            err = ChunkSuccessionError(
                f"Predecessor {range_predecessor} failed: {predecessor_error}",
                original=predecessor_error,
            )
            stats_successor.set_error(err)
            raise err from predecessor_error

        if limit and stats_predecessor.bytes_downloaded < limit:
            err = ChunkSuccessionError(
                f"Insufficient data from {range_predecessor}: "
                f"{stats_predecessor.bytes_downloaded}/{limit} bytes"
            )
            stats_successor.set_error(err)
            raise err

        if stats_predecessor.bytes_downloaded <= 0:
            err = ChunkSuccessionError(
                f"Predecessor {range_predecessor} provided no data for succession to {range_successor}. "
                "Succession cannot result in a COMPLETED chunk without data."
            )
            stats_successor.set_error(err)
            raise err

        start_crop = range_successor.start - range_predecessor.start
        end_crop = (
            (range_successor.end - range_predecessor.start)
            if range_successor.end is not None
            else stats_predecessor.bytes_downloaded - 1
        )

        await storage.crop_file(stats_predecessor.chunk_file_name, start_crop, end_crop)
        await storage.move_data(
            stats_predecessor.chunk_file_name, stats_successor.chunk_file_name
        )

        stats_successor.bytes_downloaded = end_crop - start_crop + 1
        stats_successor.set_status(EDownloadStatus.COMPLETED)
        stats_predecessor.set_status(EDownloadStatus.DEPRECATED)

        logger.info("Succession complete: %s is now COMPLETED.", range_successor)
        return range_successor

    except asyncio.CancelledError:
        logger.info("Succession cancelled for %s.", range_successor)
        stats_successor.set_status(EDownloadStatus.CANCELLED)

        if predecessor_task and not predecessor_task.done():
            predecessor_task.cancel()
            try:
                await predecessor_task
            except asyncio.CancelledError:
                pass

        stats_predecessor.remove_limit_observer()
        raise

    except Exception as e:
        if stats_successor.status != EDownloadStatus.ERROR:
            stats_successor.set_error(e)
        logger.warning("Succession failed: %s", e)
        raise
