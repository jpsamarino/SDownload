import asyncio
import logging
from collections.abc import Iterable
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
)
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol

logger = logging.getLogger(__name__)


async def cleanup_temp_files(
    storage: FileStorageProtocol, chunks_stats: Iterable[ChunkDownloadStats]
) -> None:
    """
    Cleans up temporary chunk files from storage.
    """
    logger.info("Cleaning up temp files")
    files_to_delete = [s.chunk_file_name for s in chunks_stats]

    # Only delete files that actually exist in storage
    files_names_in_storage = {s.key for s in await storage.list_data()}
    files_to_delete_in_storage = [
        s for s in files_to_delete if s in files_names_in_storage
    ]

    if files_to_delete_in_storage:
        logger.info("Files to delete: %s", files_to_delete_in_storage)
        delete_tasks = [storage.delete_data(s) for s in files_to_delete_in_storage]
        await asyncio.gather(*delete_tasks)
    else:
        logger.debug("No temp files to delete in storage.")
