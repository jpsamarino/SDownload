import asyncio
import logging

from sDownload.exceptions import (
    FileAlreadyExistsError,
    ResourceNotFoundError,
)
from sDownload.file_system import LocalStorage
from sDownload.http_client import HttpxDownloader
from sDownload.interfaces.models import (
    DownloadStats,
    EDownloadStatus,
    HttpConfigModel,
    ResourceInfo,
)
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.interfaces.protocols import (
    DownloaderProtocol,
    DownloadStrategyProtocol,
    FileStorageProtocol,
)
from sDownload.services.downloader_manager.strategies import MultiChunkDownloadStrategy
from sDownload.utils import calculate_file_match_score

logger = logging.getLogger(__name__)


class DownloadTask:
    def __init__(
        self,
        params: DownloadTaskParams,
        *,
        strategy: DownloadStrategyProtocol | None = None,
        downloader: DownloaderProtocol | None = None,
        storage: FileStorageProtocol | None = None,
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
        self._file_name: str | None = params.file_name
        self._resource_info: ResourceInfo | None = None
        self._dl_stats: DownloadStats | None = None
        self._controller_task: asyncio.Task | None = None
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._status: EDownloadStatus = EDownloadStatus.PENDING
        self._use_chunked: bool = params.use_chunked
        self._max_conn: int = params.max_conn
        self._last_error: Exception | None = None

    @property
    def status(self) -> EDownloadStatus:
        return self._status

    @property
    def stats(self) -> DownloadStats | None:
        return self._dl_stats

    @property
    def file_name(self) -> str | None:
        return self._file_name

    @property
    def resource_info(self) -> ResourceInfo | None:
        return self._resource_info

    @property
    def last_error(self) -> Exception | None:
        return self._last_error

    async def _resolve_file_info(self) -> ResourceInfo:
        """
        Queries the remote resource info, validates existence in storage,
        and adapts chunking / connection settings based on server capabilities.
        """
        try:
            info = await self._downloader.get_file_info(self._params.url)
            if not info:
                raise ResourceNotFoundError(self._params.url)

            self._resource_info = info

            # 1. Resolve target file name
            if not self._file_name:
                self._file_name = info.file_name

            # 2. Check if file already exists in storage
            match_score = await calculate_file_match_score(
                storage=self._storage,
                file_name=self._file_name,
                expected_size=info.file_size,
                remote_created_at=info.file_created_at,
            )

            if match_score.file_exists:
                if self._params.overwrite_existing:
                    logger.info(
                        "File %s already exists in storage. overwrite_existing is True, will overwrite.",
                        self._file_name,
                    )
                elif match_score.score >= self._params.min_trust_score:
                    logger.info(
                        "File %s exists in storage with high confidence (score=%.2f, threshold=%.2f). "
                        "Marking completed: %s",
                        self._file_name,
                        match_score.score,
                        self._params.min_trust_score,
                        match_score.reason,
                    )
                    self._status = EDownloadStatus.COMPLETED
                    self._dl_stats = DownloadStats(
                        file_size=info.file_size,
                        bytes_downloaded=info.file_size,
                        progress=100.0,
                    )
                    return info
                else:
                    logger.warning(
                        "File %s exists in storage but is not trusted (score=%.2f < threshold=%.2f) "
                        "and overwrite_existing is False: %s",
                        self._file_name,
                        match_score.score,
                        self._params.min_trust_score,
                        match_score.reason,
                    )
                    raise FileAlreadyExistsError(self._file_name)

            # 3. Adapt chunking & concurrency to server capabilities
            if not info.server_accept_ranges or not info.file_size or info.file_size <= 0:
                logger.info(
                    "Server does not support ranges or file size is unknown for %s. "
                    "Downgrading to single-stream download.",
                    self._params.url,
                )
                self._use_chunked = False
                self._max_conn = 1
            else:
                self._use_chunked = self._params.use_chunked
                self._max_conn = self._params.max_conn

            # 4. Initialize download stats
            self._dl_stats = DownloadStats(file_size=info.file_size)
            return info

        except Exception as exc:
            self._status = EDownloadStatus.ERROR
            self._last_error = exc
            logger.error("Failed to resolve file info for %s: %s", self._params.url, exc)
            raise

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

    def set_target_speed(self, bytes_per_second: int | None) -> None: ...

    def _finalize(self) -> None:
        # merge all chunks into a single file
        # remove all chunks
        # update stats
        ...

    async def _load_recovery_state(self) -> None: ...

    async def _dl_controller(self) -> None:
        # control loop for download
        pass
