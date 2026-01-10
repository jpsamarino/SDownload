from sDownload.interfaces.protocols.chunk_models import (
    ChunkOperationPlanModel,
    ChunkRange,
)
from sDownload.interfaces.protocols.download_strategy_protocol import (
    DownloadStrategyProtocol,
)
from sDownload.utils.range_operations import calculate_ranges
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    DownloadStats,
)


class MultiChunkDownloadStrategy(DownloadStrategyProtocol):
    max_conn: int = 1
    target_qt_conn: int = 0

    def __init__(
        self,
        max_conn: int = 1,
        use_chunked_download: bool = True,
        cache: list[ChunkRange] | None = None,
    ):
        self.max_conn = max_conn
        self.target_qt_conn = max_conn
        self.use_chunked_download = use_chunked_download
        self.cache = cache
        self._initialized = False

    def _calc_initial_ranges(self, file_size: int) -> list[ChunkRange]:
        if not self.use_chunked_download or file_size <= 0:
            return [ChunkRange(0, None)]

        limit_size_per_chunk = 2 * 1024 * 1024  # 2MB
        if file_size // self.max_conn < limit_size_per_chunk:
            self.target_qt_conn = file_size // limit_size_per_chunk

        return calculate_ranges(file_size, self.target_qt_conn, self.cache)

    def on_start(
        self, dl_stats: DownloadStats, chunks_stats: dict[str, ChunkDownloadStats]
    ) -> ChunkOperationPlanModel:
        if chunks_stats:
            return {"chunks_to_start": None, "chunks_to_stop": None}

        ranges = self._calc_initial_ranges(dl_stats.file_size)
        return {"chunks_to_start": ranges, "chunks_to_stop": None}

    def on_update(
        self, dl_stats: DownloadStats, chunks_stats: dict[str, ChunkDownloadStats]
    ) -> ChunkOperationPlanModel:
        return {"chunks_to_start": None, "chunks_to_stop": None}

    def on_end(
        self, dl_stats: DownloadStats, chunks_stats: dict[str, ChunkDownloadStats]
    ) -> None: ...
