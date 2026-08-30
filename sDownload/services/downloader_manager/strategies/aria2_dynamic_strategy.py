from sDownload.global_settings import global_settings
from sDownload.interfaces.models import (
    AnyStrategyAction,
    ChunkDownloadStats,
    ChunkRange,
    DownloadStats,
    EDownloadStatus,
    StrategyAction,
)
from sDownload.interfaces.protocols import DownloadStrategyProtocol
from sDownload.utils.range_operations import calculate_ranges


class Aria2DynamicStrategy(DownloadStrategyProtocol):
    """
    Dynamic segmentation download strategy inspired by aria2 (adaptive work-stealing).

    Key behaviors:
    1. Starts with a single primary connection on_start for [0, file_size - 1].
    2. In on_update, when available connection slots exist, finds the active chunk with
       the largest remaining un-downloaded byte range.
    3. If remaining_bytes >= 2 * min_split_size, splits the remaining range at the midpoint:
       - Resizes the existing chunk to cover up to midpoint.
       - Starts a new chunk for [midpoint + 1, end].
    4. Automatically adapts to streaming / single-stream when file_size is unknown.
    """

    max_conn: int

    def __init__(
        self,
        max_conn: int = 4,
        min_split_size: int | None = None,
        use_chunked_download: bool = True,
        cache: list[ChunkRange] | None = None,
    ):
        self.max_conn = max_conn
        self.min_split_size = (
            min_split_size
            if min_split_size is not None
            else global_settings.min_chunk_split_size_bytes
        )
        self.use_chunked_download = use_chunked_download
        self.cache = cache
        self._initialized = False

    def on_start(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        if chunks_stats or available_slots <= 0:
            return []

        if not self._initialized:
            self._initialized = True

            if not self.use_chunked_download or not dl_stats.file_size or dl_stats.file_size <= 0:
                return [StrategyAction.Start(range=ChunkRange(0, None))]

            if self.cache:
                initial_ranges = calculate_ranges(
                    dl_stats.file_size,
                    min(self.max_conn, available_slots),
                    self.cache,
                )
                return [StrategyAction.Start(range=r) for r in initial_ranges[:available_slots]]

            return [StrategyAction.Start(range=ChunkRange(0, dl_stats.file_size - 1))]

        return []

    def on_update(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        if (
            available_slots <= 0
            or not self.use_chunked_download
            or not dl_stats.file_size
            or dl_stats.file_size <= 0
        ):
            return []

        actions: list[AnyStrategyAction] = []
        slots_left = available_slots

        # Collect candidate active chunks
        active_candidates: list[tuple[ChunkRange, int, int]] = []
        for s in chunks_stats.values():
            if s.status == EDownloadStatus.DOWNLOADING and s.range.end is not None:
                current_cursor = s.range.start + max(0, s.bytes_downloaded)
                remaining = s.range.end - current_cursor + 1
                if remaining >= 2 * self.min_split_size:
                    active_candidates.append((s.range, current_cursor, s.range.end))

        while slots_left > 0 and active_candidates:
            # Pick chunk with largest remaining unwritten bytes
            active_candidates.sort(key=lambda item: item[2] - item[1] + 1, reverse=True)
            orig_range, current_cursor, current_end = active_candidates.pop(0)

            remaining = current_end - current_cursor + 1
            if remaining < 2 * self.min_split_size:
                break

            # Midpoint split of the unwritten region
            split_offset = remaining // 2
            midpoint = current_cursor + split_offset - 1

            first_half = ChunkRange(orig_range.start, midpoint)
            second_half = ChunkRange(midpoint + 1, orig_range.end)

            actions.append(StrategyAction.Resize(current_range=orig_range, new_range=first_half))
            actions.append(StrategyAction.Start(range=second_half))

            slots_left -= 1

            # Check if halves can be split again if more slots are available
            first_half_remaining = midpoint - current_cursor + 1
            if first_half_remaining >= 2 * self.min_split_size:
                active_candidates.append((first_half, current_cursor, midpoint))

            second_half_remaining = second_half.length or 0
            if second_half.end is not None and second_half_remaining >= 2 * self.min_split_size:
                active_candidates.append((second_half, second_half.start, second_half.end))

        return actions

    def on_end(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
    ) -> None:
        self._initialized = False
