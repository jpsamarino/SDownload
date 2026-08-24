import asyncio
import logging
from collections.abc import Callable

from sDownload.interfaces.models import ChunkDownloadStats, ChunkRange

logger = logging.getLogger(__name__)


def format_chunk_file_name(chunk_range: ChunkRange, file_name: str) -> str:
    """
    Returns the standardized temporary file name for a chunk.
    """
    return f"{chunk_range}_{file_name}.sdownload"


def get_effective_range_info(
    chunk_range: ChunkRange, total_file_size: int | None
) -> tuple[int | None, int | None]:
    """
    Calculates the effective end byte and total size for a chunk range.

    Returns:
        tuple[int | None, int | None]: (effective_end, total_bytes)
    """
    if chunk_range.end is not None:
        effective_end = chunk_range.end
        total_bytes = effective_end - chunk_range.start + 1
    elif total_file_size is not None and total_file_size > 0:
        effective_end = total_file_size - 1
        total_bytes = effective_end - chunk_range.start + 1
    else:
        effective_end = None
        total_bytes = None

    return effective_end, total_bytes


def create_succession_stop_callback(
    current_range: ChunkRange,
    new_range: ChunkRange,
    stats_a: ChunkDownloadStats,
    predecessor_task: asyncio.Task | None,
) -> Callable[[], None]:
    """
    Creates a callback to stop the predecessor task when a limit is reached.
    """

    def stop_predecessor():
        if (
            predecessor_task
            and not predecessor_task.done()
            and stats_a.bytes_downloaded != stats_a.file_size
        ):
            logger.info(
                "Limit reached for %s. Triggering succession to %s.",
                current_range,
                new_range,
            )
            predecessor_task.cancel()
        elif stats_a.bytes_downloaded == stats_a.file_size:
            logger.info(
                "Task %s finished after limit. It will be succession to %s.",
                current_range,
                new_range,
            )

    return stop_predecessor
