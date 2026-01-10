from typing import List

from sDownload.interfaces.protocols.chunk_models import ChunkRange


def calculate_ranges(
    file_size: int, num_parts: int, cache: List[ChunkRange] | None = None
) -> List[ChunkRange]:
    """
    Calculates download ranges including cached parts and filling gaps.

    Args:
        file_size: Total size of the file.
        num_parts: Number of divisions (to calculate target chunk size).
        cache: List of existing cached ranges (start, end).

    Returns:
        List of ranges (start, end) covering the whole file.
    """
    if num_parts <= 0:
        num_parts = 1

    target_chunk_size = max(1, file_size // num_parts)

    sorted_cache = sorted(cache or [], key=lambda x: x.start)

    final_ranges = []
    current_pos = 0

    for cache_range in sorted_cache:
        if current_pos < cache_range.start:
            gap_start = current_pos
            gap_end = cache_range.start - 1
            temp_start = gap_start
            while temp_start <= gap_end:
                remaining = gap_end - temp_start + 1
                if remaining <= target_chunk_size:
                    final_ranges.append(ChunkRange(temp_start, gap_end))
                    temp_start = gap_end + 1
                else:
                    temp_end = temp_start + target_chunk_size - 1
                    final_ranges.append(ChunkRange(temp_start, temp_end))
                    temp_start = temp_end + 1

        final_ranges.append(cache_range)

        if cache_range.end is None:
            current_pos = file_size
        else:
            current_pos = cache_range.end + 1

    if current_pos < file_size:
        gap_start = current_pos
        gap_end = file_size - 1

        temp_start = gap_start
        while temp_start <= gap_end:
            remaining = gap_end - temp_start + 1

            if remaining <= target_chunk_size:
                final_ranges.append(ChunkRange(temp_start, None))
                break

            temp_end = temp_start + target_chunk_size - 1
            final_ranges.append(ChunkRange(temp_start, temp_end))
            temp_start = temp_end + 1

    return final_ranges
