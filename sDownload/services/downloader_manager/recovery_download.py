import json
import logging
import asyncio
from dataclasses import dataclass, asdict
from datetime import datetime
from types import SimpleNamespace
from typing import List, Optional

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    EDownloadStatus,
    ChunkRange,
    RecoveryChunkDTO,
    RecoveryStateDTO,
    DownloadInfo,
)
from sDownload.interfaces.protocols import FileStorageProtocol

logger = logging.getLogger(__name__)


class RecoveryDownload:
    def __init__(self, storage: FileStorageProtocol) -> None:
        self._storage = storage

    def _get_recovery_key(self, file_id: str) -> str:
        return f".sdown_resume_{file_id}.json"

    async def save_info(
        self,
        file_id: str,
        total_file_size: int,
        stats_list: List[ChunkDownloadStats],
        min_chunk_size: int = 1024 * 1024,  # Default 1MB filter
        delete_useless_chunks: bool = True,
    ) -> None:
        if not file_id:
            logger.warning("Cannot save recovery info: missing file_id")
            return

        dto_chunks: List[RecoveryChunkDTO] = []
        chunks_to_delete: List[str] = []

        files_in_storage = {
            s.key: s.size_bytes for s in await self._storage.list_data()
        }

        for stats in stats_list:
            actual_size = files_in_storage.get(stats.chunk_file_name, 0)

            if actual_size != stats.bytes_downloaded:
                logger.warning(
                    "Chunk file %s size mismatch (Disk: %d, Stats: %d), marking for deletion",
                    stats.chunk_file_name,
                    actual_size,
                    stats.bytes_downloaded,
                )
                chunks_to_delete.append(stats.chunk_file_name)
                continue

            is_finished = stats.status == EDownloadStatus.COMPLETED

            if actual_size > 0:
                if is_finished or actual_size >= min_chunk_size:
                    # Convert to minimal DTO for saving
                    dto_chunks.append(
                        RecoveryChunkDTO(
                            chunk_file_name=stats.chunk_file_name,
                            start=stats.range.start,
                            end=stats.range.start + actual_size - 1,
                            bytes=actual_size,
                        )
                    )
                else:
                    logger.debug(
                        "Skipping small chunk %s, marking for deletion",
                        stats.chunk_file_name,
                    )
                    chunks_to_delete.append(stats.chunk_file_name)

        state_dto = RecoveryStateDTO(
            file_id=file_id,
            file_size=total_file_size,
            chunks=dto_chunks,
            updated_at=datetime.now().timestamp(),
        )

        data_json = json.dumps(asdict(state_dto), indent=4)

        async def json_stream():
            yield data_json.encode("utf-8")

        recovery_key = self._get_recovery_key(file_id)
        await self._storage.save_binary_data(recovery_key, json_stream())
        logger.info("Recovery DTO saved to %s", recovery_key)

        if delete_useless_chunks and chunks_to_delete:
            logger.info(
                "Cleaning up %d useless/corrupted chunks in parallel",
                len(chunks_to_delete),
            )

            async def safe_delete(key):
                try:
                    await self._storage.delete_data(key)
                except Exception as e:
                    logger.warning("Failed to delete chunk %s: %s", key, e)

            await asyncio.gather(*(safe_delete(k) for k in chunks_to_delete))

    async def load_info(self, file_id: str) -> Optional[DownloadInfo]:
        recovery_key = self._get_recovery_key(file_id)
        files_in_storage = {
            s.key: s.size_bytes for s in await self._storage.list_data()
        }

        if recovery_key not in files_in_storage:
            logger.info("No recovery info found for file_id %s", file_id)
            return None

        try:
            content = b""
            async for chunk in self._storage.get_binary_data(recovery_key):
                content += chunk

            raw_data = json.loads(
                content.decode("utf-8"), object_hook=lambda d: SimpleNamespace(**d)
            )
            valid_stats: List[ChunkDownloadStats] = []
            for c in raw_data.chunks:
                c_name = c.chunk_file_name
                c_bytes = c.bytes

                if c_name in files_in_storage and files_in_storage[c_name] >= c_bytes:
                    valid_stats.append(
                        ChunkDownloadStats(
                            chunk_file_name=c_name,
                            range=ChunkRange(start=c.start, end=c.end),
                            file_size=c_bytes,
                            bytes_downloaded=c_bytes,
                            status=EDownloadStatus.COMPLETED,
                            progress=100.0,
                        )
                    )
                else:
                    logger.warning(
                        "Saved chunk %s is missing or changed on disk", c_name
                    )

            return DownloadInfo(
                file_id=raw_data.file_id,
                file_size=raw_data.file_size,
                chunks_finished=valid_stats,
                updated_at=raw_data.updated_at,
            )

        except Exception as e:
            logger.error("Failed to load recovery DTO for %s: %s", file_id, e)
            return None

    async def delete_info(self, file_id: str) -> None:
        recovery_key = self._get_recovery_key(file_id)
        try:
            await self._storage.delete_data(recovery_key)
        except Exception as e:
            logger.debug(
                "Recovery info %s already gone or inaccessible: %s", recovery_key, e
            )

    async def purge_all(self, file_id: str) -> None:
        """
        Deletes both the recovery metadata and all associated data chunks from disk.
        Useful when cancelling or completely removing a download.
        """
        info = await self.load_info(file_id)
        recovery_key = self._get_recovery_key(file_id)

        # 1. Collect all chunk files mentioned in JSON
        chunks_to_delete = []
        if info:
            chunks_to_delete = [c.chunk_file_name for c in info.chunks_finished]

        logger.info(
            "Purging all data for %s: %d chunks plus metadata",
            file_id,
            len(chunks_to_delete),
        )

        # 2. Parallel deletion of chunks
        if chunks_to_delete:

            async def safe_delete(key):
                try:
                    await self._storage.delete_data(key)
                except Exception as e:
                    logger.debug("Failed to delete chunk %s during purge: %s", key, e)

            await asyncio.gather(*(safe_delete(k) for k in chunks_to_delete))

        # 3. Final cleanup of the JSON itself
        try:
            await self._storage.delete_data(recovery_key)
        except Exception as e:
            logger.debug("Failed to delete recovery key %s: %s", recovery_key, e)
