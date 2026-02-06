from collections import deque
from typing import List, NamedTuple, Optional, Tuple
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.interfaces.protocols.file_storage_protocol import FileRangeConfig


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


class ChunkFragment(NamedTuple):
    range: ChunkRange
    read_limit_qt_bytes: Optional[int]


def calculate_optimal_coverage(
    chunks: list[ChunkRange], file_size: Optional[int] = None
) -> list[ChunkFragment]:
    """
    Finds the optimal sequence of chunks to rebuild a file using only "Tail Cuts".
    This ensures that every file read starts at offset 0 (no seek needed).

    Args:
        chunks: List of available ChunkRange objects.
        file_size: Expected total size of the file (optional).

    Returns:
        A list of ChunkFragment instructions.
    """
    if not chunks:
        raise RuntimeError("No chunks provided for merge resolution.")

    reach = lambda c: c.end if c.end is not None else float("inf")

    best_by_start: dict[int, ChunkRange] = {}
    for c in chunks:
        if c.start not in best_by_start or reach(c) > reach(best_by_start[c.start]):
            best_by_start[c.start] = c

    starts = sorted(best_by_start.keys())
    if starts[0] > 0:
        raise RuntimeError(f"Gap at the beginning: first chunk starts at {starts[0]}")

    # Possible exit/cut points (starts of all chunks + file_size)
    exit_points_set = set(starts)
    if file_size is not None:
        exit_points_set.add(file_size)
    exit_points = sorted(list(exit_points_set))

    # BFS to find the shortest path (least number of chunks) from 0 to target
    queue: deque[tuple[int, list[ChunkFragment]]] = deque([(0, [])])
    visited = {0}

    while queue:
        pos, path = queue.popleft()

        # Check if we reached the goal
        if file_size is not None and pos == file_size:
            return path

        chunk = best_by_start.get(pos)
        if not chunk:
            continue

        chunk_limit = reach(chunk)

        # Optimization: if this chunk can reach the end, we are done
        if file_size is None and chunk_limit == float("inf"):
            # read_limit_qt_bytes=None means read everything available
            return path + [ChunkFragment(range=chunk, read_limit_qt_bytes=None)]

        # Find all reachable exit points from current chunk starting at 'pos'
        current_exit_candidates = [
            ep for ep in exit_points if pos < ep <= chunk_limit + 1
        ]

        for next_pos in reversed(current_exit_candidates):
            if next_pos not in visited:
                visited.add(next_pos)

                # Quantity of bytes to take from this chunk:
                # If pos=0 and next_pos=100, we need 100 bytes.
                qt_to_read = next_pos - pos

                # If qt_to_read covers the entire remaining part of this chunk,
                # we don't need a limit (no truncation).
                # Note: chunk_limit is the last byte index of the chunk.
                if chunk_limit == float("inf"):
                    limit = None
                elif (pos + qt_to_read) >= chunk_limit + 1:
                    limit = None
                else:
                    limit = qt_to_read

                new_path = path + [
                    ChunkFragment(range=chunk, read_limit_qt_bytes=limit)
                ]
                queue.append((next_pos, new_path))

                if file_size is not None and next_pos == file_size:
                    return new_path

    raise RuntimeError("Gap detected: unable to cover the required file range.")
