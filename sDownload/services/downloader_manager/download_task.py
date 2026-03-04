from sDownload.services.downloader_manager.chunk_manager import ChunkManager
from sDownload.interfaces.models.params.chunk_manager_params import ChunkManagerParams
from sDownload.services.downloader_manager.strategies import MultiChunkDownloadStrategy
from sDownload.file_system import LocalStorage
from sDownload.interfaces.models import HttpConfigModel
import asyncio
import logging
from typing import Dict, Optional

from sDownload.interfaces.models import (
    ChunkRange,
    ChunkDownloadStats,
    DownloadStats,
    EDownloadStatus,
    AnyStrategyAction,
)
from sDownload.http_client import HttpxDownloader
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.interfaces.protocols import (
    DownloaderProtocol,
    DownloadStrategyProtocol,
    FileStorageProtocol,
)

logger = logging.getLogger(__name__)


class DownloadTask:

    def __init__(
        self,
        params: DownloadTaskParams,
        *,
        strategy: Optional[DownloadStrategyProtocol] = None,
        downloader: Optional[DownloaderProtocol] = None,
        storage: Optional[FileStorageProtocol] = None,
    ):
        self._params = params
        self._downloader = downloader or HttpxDownloader(
            config=HttpConfigModel(headers=params.headers)
        )
        self._storage = storage or LocalStorage(storage_dir=params.dest_dir)
        self._strategy = strategy or MultiChunkDownloadStrategy(
            max_conn=params.max_conn,
            use_chunked_download=params.use_chunked,
        )
        self._file_name: Optional[str] = params.file_name
        self._dl_stats: Optional[DownloadStats] = None  # need to create obj in init
        self._controller_task: Optional[asyncio.Task] = None
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._status: EDownloadStatus = EDownloadStatus.PENDING

    async def start(self) -> None:
        # verify if url is valid and get file info
        # try verify if file already exists if yes recover chunks
        # create a chunk manager object with chunks if it exists or create a new one
        # consult strategy for initial actions
        # execute actions
        # start controller loop
        ...

    async def pause(self) -> None:
        # if pause cancel all chunks and save chunks stats
        ...

    async def resume(self) -> None:
        # if resume create a chunk manager object with chunks if it exists or create a new one
        # similar to start but with chunks already loaded
        ...

    async def cancel(self, delete_temp_files: bool = True) -> None:
        # like pause but with delete_temp_files
        ...

    async def wait_until_done(self) -> None:
        # wait until the download is done and execute finalize??
        ...

    def set_target_speed(self, bytes_per_second: Optional[int]) -> None: ...

    @property
    def stats(self) -> Optional[DownloadStats]:
        return self._dl_stats

    def _finalize(self) -> None:
        # merge all chunks into a single file
        # remove all chunks
        # update stats
        ...

    def _resolve_dependencies(self) -> None:
        ...
        # create default dependencies in each module

    async def _load_recovery_state(self) -> None:
        ...
        # it should be externalized to a recovery manager

    async def _dl_controller(self) -> None:
        # control loop for download
        # update status
        # run watchdog in each loop after update status and verify each chunk??
        # consult strategy for actions
        # execute actions
        # check if download is done
        # repeat
        pass
