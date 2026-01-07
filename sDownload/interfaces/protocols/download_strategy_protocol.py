from typing import Protocol, Dict, TypedDict, NamedTuple

from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    DownloadStats,
)


class ChunkRange(NamedTuple):
    start: int
    end: int | None


class ChunkOperationActions(TypedDict):
    chunks_to_start: list[ChunkRange] | None
    chunks_to_stop: list[ChunkRange] | None


class DownloadStrategyProtocol(Protocol):
    """
    Defines the interface for a download strategy.

    A strategy decides how to start, update, and stop chunk downloads.
    """

    max_conn: int

    def __init__(
        self, max_conn: int = 1, cache: list[ChunkRange] | None = None
    ) -> None:
        """Initialize the strategy with the maximum number of parallel connections."""
        ...

    def on_start(
        self, dl_stats: DownloadStats, chunks_stats: Dict[str, ChunkDownloadStats]
    ) -> ChunkOperationActions:
        """Return actions that should start new chunk downloads."""
        ...

    def on_update(
        self, dl_stats: DownloadStats, chunks_stats: Dict[str, ChunkDownloadStats]
    ) -> ChunkOperationActions:
        """Return actions that should update currently running downloads."""
        ...

    def on_end(
        self, dl_stats: DownloadStats, chunks_stats: Dict[str, ChunkDownloadStats]
    ) -> None:
        """Execute any cleanup or finalization when downloads are stopped or ended."""
        ...
