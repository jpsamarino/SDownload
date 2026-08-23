import logging

from sDownload.exceptions import ReconstructionError
from sDownload.interfaces.models import ChunkDownloadStats, EDownloadStatus
from sDownload.interfaces.protocols import FileRangeParams, FileStorageProtocol
from sDownload.utils import calculate_optimal_coverage

logger = logging.getLogger(__name__)


async def reconstruct_file(
    storage: FileStorageProtocol,
    stats_list: list[ChunkDownloadStats],
    final_filename: str,
    total_file_size: int | None,
) -> str:
    """
    Merges completed chunks into the final file using optimal coverage.
    """
    completed_stats = [s for s in stats_list if s.status == EDownloadStatus.COMPLETED]

    if not completed_stats:
        raise ReconstructionError("No completed chunks available to reconstruct the file.")

    ranges = [s.range for s in completed_stats]

    try:
        fragments = calculate_optimal_coverage(ranges, file_size=total_file_size)
    except Exception as e:
        raise ReconstructionError(f"Failed to calculate optimal coverage: {e}", original=e) from e

    range_map = {s.range: s for s in completed_stats}

    merge_configs = []
    for frag in fragments:
        stats = range_map.get(frag.range)
        if not stats:
            raise ReconstructionError(f"Could not find stats for range {frag.range}")

        end_byte = (frag.read_limit_qt_bytes - 1) if frag.read_limit_qt_bytes else None

        merge_configs.append(
            FileRangeParams(
                key=stats.chunk_file_name,
                start_byte=0,
                end_byte=end_byte,
            )
        )

    logger.info("Merging %d fragments into %s", len(merge_configs), final_filename)

    try:
        await storage.merge_ranges(merge_configs, final_filename)
    except Exception as e:
        raise ReconstructionError(f"Storage merge failed: {e}", original=e) from e

    return final_filename
