import logging

from sDownload.interfaces.models import (
    AnyStrategyAction,
    ChunkDownloadStats,
    ChunkRange,
    DownloadStats,
    StrategyAction,
)
from sDownload.interfaces.protocols import DownloadStrategyProtocol

logger = logging.getLogger(__name__)


class SingleStreamStrategy(DownloadStrategyProtocol):
    """
    Strategy that downloads a resource in a single continuous stream from byte 0 to EOF.
    Used automatically for streams, unknown file sizes, or servers without Range support.
    """

    def __init__(self) -> None:
        self._started = False

    def on_start(
        self,
        dl_stats: DownloadStats,
        chunks_stats: dict[ChunkRange, ChunkDownloadStats],
        available_slots: int,
    ) -> list[AnyStrategyAction]:
        if self._started or chunks_stats or available_slots <= 0:
            return []

        self._started = True
        logger.info("SingleStreamStrategy issuing single open-ended stream [0, EOF)")
        return [StrategyAction.Start(range=ChunkRange(start=0, end=None))]

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
    ) -> None:
        pass
