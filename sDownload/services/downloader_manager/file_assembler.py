import logging
import asyncio
from sDownload.interfaces.protocols.file_storage_protocol import (
    FileStorageProtocol,
    FileRangeConfig,
)
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    EDownloadStatus,
)
from sDownload.utils.range_operations import calculate_optimal_coverage


class FileAssemblerError(Exception):
    pass


class FileAssembler:

    def __init__(
        self,
        storage: FileStorageProtocol,
        logger: logging.Logger | None = None,
    ):
        self._storage = storage
        self._logger = logger or logging.getLogger(__name__)

    async def merge_chunks(
        self,
        stats_list: list[ChunkDownloadStats],
        final_filename: str,
        total_file_size: int | None,
    ) -> str:
        """
        Merges completed chunks into the final file using optimal coverage.
        """
        completed_stats = [
            s for s in stats_list if s.status == EDownloadStatus.COMPLETED
        ]

        if not completed_stats:
            raise FileAssemblerError("No completed chunks available to merge.")

        ranges = [s.range for s in completed_stats]

        try:
            fragments = calculate_optimal_coverage(ranges, file_size=total_file_size)
        except Exception as e:
            raise FileAssemblerError(f"Failed to calculate coverage: {e}") from e

        range_map = {s.range: s for s in completed_stats}

        merge_configs = []
        for frag in fragments:
            stats = range_map.get(frag.range)
            if not stats:
                raise FileAssemblerError(f"Could not find stats for range {frag.range}")

            end_byte = (
                (frag.read_limit_qt_bytes - 1) if frag.read_limit_qt_bytes else None
            )

            merge_configs.append(
                FileRangeConfig(
                    key=stats.chunk_file_name,
                    start_byte=0,
                    end_byte=end_byte,
                )
            )

        self._logger.info(
            "Merging %d fragments into %s", len(merge_configs), final_filename
        )

        try:
            await self._storage.merge_ranges(merge_configs, final_filename)
        except Exception as e:
            raise FileAssemblerError(f"Storage merge failed: {e}") from e

        return final_filename
