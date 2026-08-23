from typing import Protocol

from sDownload.interfaces.models import (
    AnyStrategyAction,
    ChunkDownloadStats,
    ChunkRange,
    DownloadStats,
)


class DownloadStrategyProtocol(Protocol):
    """
    Defines the interface for a download strategy.

    A strategy decides how to start, update, and stop chunk downloads.
    """

    max_conn: int

    def __init__(self, max_conn: int = 1, cache: list[ChunkRange] | None = None) -> None:
        """Initialize the strategy with the maximum number of parallel connections."""
        ...

    def on_start(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        """
        Return actions that should start new chunk downloads.

        Args:
            dl_stats: Overall statistics for the download.
            chunks_stats: Statistics for each currently managed chunk.
            available_slots: The "balance" or "stock" of connections this strategy is allowed
                             to use at this moment. This value reflects global or domain
                             limits enforced by the DownloadManager. The strategy should
                             never return more `StartChunkAction`s than this available balance.
        """
        ...

    def on_update(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        """
        Return actions that should update currently running downloads.

        Args:
            dl_stats: Overall statistics for the download.
            chunks_stats: Statistics for each currently managed chunk.
            available_slots: The number of new connections this strategy can open right now.
                             This acts as a connection pool managed externally. If a chunk finishes
                             or is cancelled, slots become available again. The strategy uses
                             this to decide whether it can aggressively split active chunks
                             or if it must wait for ongoing connections to finish.
        """
        ...

    def on_end(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
    ) -> None:
        """Execute any cleanup or finalization when downloads are stopped or ended."""
        ...
