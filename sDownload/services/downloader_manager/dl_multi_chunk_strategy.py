from typing import Dict, Iterator, List, Optional, Tuple, TypeAlias, TypedDict

from sDownload.services.downloader_manager.download_stats_models import ChunkDownloadStats

ChunkRange: TypeAlias = Tuple[int, Optional[int]]
ChunkRangeList: TypeAlias = list[ChunkRange]


class ChunkOperationActions(TypedDict):
    chunks_to_start: Optional[ChunkRangeList]
    chunks_to_stop: Optional[ChunkRangeList]


class MultiChunkDownloadStrategy:
    max_conn: int = 1

    def calc_range(self,
                   file_size: int,
                   max_conn: int,
                   use_chunked_download: bool
                   ) -> ChunkRangeList:
        # add finished chunk with args
        _pending = []
        if not use_chunked_download:
            self.max_conn = 1
            _pending.append((0, None))
            return _pending
        total, parts = file_size, max_conn
        base, rem = divmod(total, parts)
        cur = 0
        for i in range(parts):
            extra = 1 if i < rem else 0
            end = cur + base + extra - 1
            _pending.append(
                (cur, end if end < total - 1 else None))
            cur = end + 1
        return _pending

    def get_start_actions(self, chunks: Dict[str, ChunkDownloadStats]) -> Iterator[ChunkOperationActions]:
        ...

    def get_update_actions(self, chunks: Dict[str, ChunkDownloadStats]) -> Iterator[ChunkOperationActions]:
        ...

    def get_stop_actions(self, chunks: Dict[str, ChunkDownloadStats]) -> Iterator[ChunkOperationActions]:
        ...
