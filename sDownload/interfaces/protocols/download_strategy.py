from typing import Protocol, Dict, Iterable, TypeAlias, TypedDict

from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    DownloadStats,
)

ChunkRange: TypeAlias = tuple[int, int | None]
ChunkRangeList: TypeAlias = list[ChunkRange]


class ChunkOperationActions(TypedDict):
    chunks_to_start: ChunkRangeList | None
    chunks_to_stop: ChunkRangeList | None


class DownloadStrategy(Protocol):
    """
    Defines the interface for a download strategy.

    A strategy decides how to start, update, and stop chunk downloads.
    Implementations may return either a list or use `yield` to produce actions lazily.
    """

    max_conn: int

    def __init__(self, max_conn: int = 1) -> None:
        """Initialize the strategy with the maximum number of parallel connections."""
        ...

    def get_start_actions(
        self, dl_stats: DownloadStats, chunks_stats: Dict[str, ChunkDownloadStats]
    ) -> Iterable[ChunkOperationActions]:
        """Return actions that should start new chunk downloads."""
        ...

    def get_update_actions(
        self, dl_stats: DownloadStats, chunks_stats: Dict[str, ChunkDownloadStats]
    ) -> Iterable[ChunkOperationActions]:
        """Return actions that should update currently running downloads."""
        ...

    def get_stop_actions(
        self, dl_stats: DownloadStats, chunks_stats: Dict[str, ChunkDownloadStats]
    ) -> Iterable[ChunkOperationActions]:
        """Return actions that should stop active chunk downloads."""
        ...
