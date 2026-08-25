from sDownload.global_settings import global_settings
from sDownload.interfaces.models import (
    AnyStrategyAction,
    ChunkDownloadStats,
    ChunkRange,
    DownloadStats,
    StrategyAction,
)
from sDownload.interfaces.protocols import DownloadStrategyProtocol
from sDownload.utils import calculate_ranges


class MultiChunkDownloadStrategy(DownloadStrategyProtocol):
    max_conn: int = 1
    target_qt_conn: int = 0

    def __init__(
        self,
        max_conn: int = 1,
        use_chunked_download: bool = True,
        cache: list[ChunkRange] | None = None,
        min_chunk_size: int | None = None,
    ):
        self.max_conn = max_conn
        self.target_qt_conn = max_conn
        self.use_chunked_download = use_chunked_download
        self.cache = cache
        self.min_chunk_size = (
            min_chunk_size
            if min_chunk_size is not None
            else global_settings.min_chunk_split_size_bytes
        )
        self._initialized = False

    def _calc_initial_ranges(self, file_size: int) -> list[ChunkRange]:
        if not self.use_chunked_download or file_size <= 0:
            return [ChunkRange(0, None)]

        limit_size_per_chunk = self.min_chunk_size
        if file_size // self.max_conn < limit_size_per_chunk:
            self.target_qt_conn = max(1, file_size // limit_size_per_chunk)

        return calculate_ranges(file_size, self.target_qt_conn, self.cache)

    def on_start(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        if chunks_stats or available_slots <= 0:
            return []

        # We limit the initial ranges to min(target_qt_conn, available_slots)
        # to ensure we don't request more connections than the manager allows.
        actual_conn_target = min(self.target_qt_conn, available_slots)
        if actual_conn_target == 0:
            return []

        ranges = calculate_ranges(dl_stats.file_size, actual_conn_target, self.cache)
        return [StrategyAction.Start(range=r) for r in ranges]

    def on_update(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        return []

    def on_end(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
    ) -> None: ...
