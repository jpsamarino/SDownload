from sDownload.interfaces.models import (
    AnyStrategyAction,
    ChunkDownloadStats,
    ChunkRange,
    DownloadStats,
    StrategyAction,
)
from sDownload.interfaces.protocols import DownloadStrategyProtocol
from sDownload.utils import calculate_ranges


class SequentialChunkStrategy(DownloadStrategyProtocol):
    """
    A strategy that pre-calculates all chunk ranges at start.
    It sequentially issues Start commands for these chunks as slots become available.
    It does not perform dynamic segment resizing (splitting active chunks).
    """

    max_conn: int

    def __init__(
        self,
        max_conn: int = 1,
        use_chunked_download: bool = True,
        cache: list[ChunkRange] | None = None,
    ):
        self.max_conn = max_conn
        self.use_chunked_download = use_chunked_download
        self.cache = cache
        self._pending_ranges: list[ChunkRange] = []
        self._initialized = False

    def _calc_initial_ranges(
        self, file_size: int, cache: list[ChunkRange] | None = None
    ) -> list[ChunkRange]:
        if not self.use_chunked_download or file_size <= 0:
            return [ChunkRange(0, None)]

        limit_size_per_chunk = 2 * 1024 * 1024  # 2MB
        target_qt_conn = self.max_conn
        if file_size // self.max_conn < limit_size_per_chunk:
            target_qt_conn = max(1, file_size // limit_size_per_chunk)

        effective_cache = cache if cache is not None else self.cache
        return calculate_ranges(file_size, target_qt_conn, effective_cache)

    def on_start(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        if not self._initialized:
            cached_ranges = list(chunks_stats.keys()) if chunks_stats else self.cache
            all_ranges = self._calc_initial_ranges(dl_stats.file_size, cache=cached_ranges)
            self._pending_ranges = [r for r in all_ranges if r not in chunks_stats]
            self._initialized = True

        return self._issue_pending(available_slots)

    def on_update(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:

        # If we have slots and pending ranges, issue them.
        return self._issue_pending(available_slots)

    def _issue_pending(self, available_slots: int) -> list[AnyStrategyAction]:
        if available_slots <= 0 or not self._pending_ranges:
            return []

        actions: list[AnyStrategyAction] = []
        slots_to_use = min(available_slots, len(self._pending_ranges))

        for _ in range(slots_to_use):
            rng = self._pending_ranges.pop(0)
            actions.append(StrategyAction.Start(range=rng))

        return actions

    def on_end(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
    ) -> None:
        self._pending_ranges.clear()
        self._initialized = False
