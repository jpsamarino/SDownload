from collections import deque
from collections.abc import Iterable

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkFragment,
    ChunkRange,
    EDownloadStatus,
)


def calculate_downloaded_bytes(
    stats_list: Iterable[ChunkDownloadStats | None],
    file_size: int | None = None,
) -> int:
    """
    Calculates the total unique downloaded bytes across active and completed chunks,
    properly deduplicating overlapping ranges, respecting chunk/file bounds,
    and ignoring deprecated or cancelled chunks.
    """
    intervals: list[tuple[int, int]] = []
    for s in stats_list:
        if not s or not s.range:
            continue

        if (
            s.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.COMPLETED)
            and s.bytes_downloaded > 0
        ):
            start = s.range.start
            calculated_end = start + s.bytes_downloaded - 1
            # Respect the chunk's own defined boundary
            end = min(calculated_end, s.range.end) if s.range.end is not None else calculated_end

            # Respect total file_size upper bound if provided
            if file_size is not None and file_size > 0:
                end = min(end, file_size - 1)

            if end >= start:
                intervals.append((start, end))

    if not intervals:
        return 0

    intervals.sort(key=lambda x: x[0])

    total_bytes = 0
    cur_start, cur_end = intervals[0]

    for start, end in intervals[1:]:
        if start <= cur_end + 1:
            if end > cur_end:
                cur_end = end
        else:
            total_bytes += cur_end - cur_start + 1
            cur_start, cur_end = start, end

    total_bytes += cur_end - cur_start + 1

    if file_size is not None and file_size > 0:
        return min(total_bytes, file_size)

    return total_bytes


def calculate_ranges(
    file_size: int, num_parts: int, cache: list[ChunkRange] | None = None
) -> list[ChunkRange]:
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

        current_pos = file_size if cache_range.end is None else cache_range.end + 1

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


def calculate_optimal_coverage(
    chunks: list[ChunkRange], file_size: int | None = None
) -> list[ChunkFragment]:
    """
    Finds the minimum number of chunks needed to cover a file range [0, file_size).

    Strategy:
    - We model this as a shortest-path problem.
    - Each chunk's START is a "node" we can jump to.
    - We use BFS to find the path with the fewest jumps (chunks).
    - This guarantees using the minimum number of file operations.

    Example:
        chunks = [ChunkRange(0, 100), ChunkRange(50, 200), ChunkRange(101, 300)]
        file_size = 301

        Possible paths:
        - Path A: 0→50→101 (3 chunks)
        - Path B: 0→101 (2 chunks) ← BFS finds this first

    Args:
        chunks: Available chunk ranges.
        file_size: Target file size. If None, looks for a chunk with end=None.

    Returns:
        List of ChunkFragment with the range and how many bytes to read from it.
    """
    if not chunks:
        raise ValueError("No chunks provided for merge resolution.")

    # Helper: get the furthest byte a chunk can cover
    def get_reach(chunk: ChunkRange) -> float:
        return chunk.end if chunk.end is not None else float("inf")

    # STEP 1: Keep only the best chunk for each starting position
    # (the one that reaches the furthest)
    best_chunk_at: dict[int, ChunkRange] = {}
    for chunk in chunks:
        start = chunk.start
        if start not in best_chunk_at or get_reach(chunk) > get_reach(best_chunk_at[start]):
            best_chunk_at[start] = chunk

    # STEP 2: Validate that we have a chunk starting at byte 0
    start_positions = sorted(best_chunk_at.keys())
    if start_positions[0] > 0:
        raise ValueError(f"Gap at the beginning: first chunk starts at {start_positions[0]}")

    # STEP 3: Build the set of "cut points" (where we can transition to the next chunk)
    cut_points = set(start_positions)
    if file_size is not None:
        cut_points.add(file_size)
    cut_points = sorted(cut_points)

    # STEP 4: BFS to find the shortest path from position 0 to file_size
    queue: deque[tuple[int, list[ChunkFragment]]] = deque()
    queue.append((0, []))  # (current_position, path_so_far)
    visited = {0}

    while queue:
        current_pos, path = queue.popleft()

        # Goal reached?
        if file_size is not None and current_pos == file_size:
            return path

        # Get the best chunk that starts at this position
        chunk = best_chunk_at.get(current_pos)
        if chunk is None:
            continue  # No chunk starts here, skip

        chunk_reach = get_reach(chunk)

        # Special case: chunk with no end means "read until EOF"
        if file_size is None and chunk_reach == float("inf"):
            return path + [ChunkFragment(range=chunk, read_limit_qt_bytes=None)]

        # Find all cut points this chunk can reach
        reachable_cuts = [cp for cp in cut_points if current_pos < cp <= chunk_reach + 1]

        # Try each cut point, starting from the furthest (greedy BFS optimization)
        for next_pos in reversed(reachable_cuts):
            if next_pos in visited:
                continue
            visited.add(next_pos)

            # Calculate how many bytes we need from this chunk
            bytes_needed = next_pos - current_pos

            # Determine if we need to truncate or read the whole chunk
            uses_entire_chunk = (current_pos + bytes_needed) >= chunk_reach + 1
            read_limit = None if uses_entire_chunk or chunk_reach == float("inf") else bytes_needed

            new_fragment = ChunkFragment(range=chunk, read_limit_qt_bytes=read_limit)
            new_path = path + [new_fragment]

            # Early exit if we found the goal
            if file_size is not None and next_pos == file_size:
                return new_path

            queue.append((next_pos, new_path))

    raise ValueError("Gap detected: unable to cover the required file range.")
