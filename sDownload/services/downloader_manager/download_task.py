import asyncio
import contextlib
import logging

from sDownload.exceptions import (
    FileAlreadyExistsError,
    ResourceNotFoundError,
)
from sDownload.interfaces.models import (
    AnyStrategyAction,
    DownloadStats,
    EDownloadStatus,
    EFileAction,
    ResourceInfo,
    StrategyAction,
)
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.interfaces.models.params.chunk_manager_params import ChunkManagerParams
from sDownload.interfaces.protocols import (
    DownloaderProtocol,
    DownloadStrategyProtocol,
    FileStorageProtocol,
    RecoveryProtocol,
)
from sDownload.services.downloader_manager.chunk_manager import ChunkManager
from sDownload.services.downloader_manager.chunk_utils import reconstruct_file
from sDownload.services.downloader_manager.default_providers import default_provider
from sDownload.services.downloader_manager.strategies import (
    SingleStreamStrategy,
)
from sDownload.utils import (
    calculate_downloaded_bytes,
    resolve_file_policy,
)

logger = logging.getLogger(__name__)


class DownloadTask:
    def __init__(
        self,
        params: DownloadTaskParams,
        downloader: DownloaderProtocol | None = None,
        storage: FileStorageProtocol | None = None,
        strategy: DownloadStrategyProtocol | None = None,
        recovery: RecoveryProtocol | None = None,
    ) -> None:
        # 1. Immutable initial configuration from user
        self._params = params

        # 2. Injected or default infrastructure protocols
        self._storage: FileStorageProtocol = storage or default_provider.get_storage(
            params.dest_dir
        )
        self._downloader: DownloaderProtocol = downloader or default_provider.get_downloader(
            params.url, headers=params.headers
        )
        self._strategy: DownloadStrategyProtocol = strategy or default_provider.get_strategy(
            use_chunked=params.use_chunked, max_conn=params.max_conn
        )
        self._recovery: RecoveryProtocol = recovery or default_provider.get_recovery(self._storage)

        # 3. Dynamic runtime execution state (adaptive to remote metadata & collision policies)
        self._file_name: str | None = params.file_name  # Discovered or updated via AUTO_RENAME
        self._use_chunked: bool = params.use_chunked  # Adapted if server does not accept ranges
        self._max_conn: int = params.max_conn  # Adapted to 1 for single-stream downloads
        self._status: EDownloadStatus = EDownloadStatus.PENDING
        self._dl_stats: DownloadStats | None = None
        self._resource_info: ResourceInfo | None = None
        self._last_error: Exception | None = None

        # 4. Supervision handles
        self._chunk_manager: ChunkManager | None = None
        self._controller_task: asyncio.Task | None = None
        self._done_event = asyncio.Event()

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

    @property
    def strategy(self) -> DownloadStrategyProtocol:
        return self._strategy

    # verificar
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

            # 2. Resolve file policy & handle collisions
            resolution = await resolve_file_policy(
                storage=self._storage,
                file_name=self._file_name,
                expected_size=info.file_size,
                remote_created_at=info.file_created_at,
                policy=self._params.file_policy,
                is_generated_name=not bool(self._params.file_name),
            )

            if resolution.action == EFileAction.ERROR:
                logger.warning(
                    "File policy %s failed for %s: %s",
                    self._params.file_policy,
                    resolution.target_file_name,
                    resolution.reason,
                )
                raise FileAlreadyExistsError(resolution.target_file_name)

            if resolution.action == EFileAction.REUSE:
                logger.info(
                    "File %s exists and policy is %s. Marking completed: %s",
                    resolution.target_file_name,
                    self._params.file_policy,
                    resolution.reason,
                )
                self._status = EDownloadStatus.COMPLETED
                self._dl_stats = DownloadStats(
                    file_size=info.file_size,
                    bytes_downloaded=info.file_size,
                    progress=100.0,
                )
                return info

            # Action is DOWNLOAD (update filename if auto-renamed)
            self._file_name = resolution.target_file_name

            # 3. Adapt chunking & concurrency to server capabilities
            is_stream = not info.server_accept_ranges or not info.file_size or info.file_size <= 0
            if is_stream:
                logger.info(
                    "Server does not support ranges or file size is unknown for %s. "
                    "Migrating to SingleStreamStrategy and single-stream download.",
                    self._params.url,
                )
                self._use_chunked = False
                self._max_conn = 1
                self._strategy = SingleStreamStrategy()
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
        """
        Starts the download task lifecycle:
        1. Resolves resource info (if not already resolved)
        2. Loads recovery state if available
        3. Initializes ChunkManager
        4. Launches background controller task
        """
        if self._status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.COMPLETED):
            return

        if self._resource_info is None:
            await self._resolve_file_info()

        if self._status == EDownloadStatus.COMPLETED:
            return

        # Check recovery state
        recovered_stats = None
        if (
            self._params.enable_recovery
            and self._resource_info
            and self._resource_info.file_id
            and self._use_chunked
        ):
            recovered_info = await self._recovery.load_info(self._resource_info.file_id)
            if recovered_info and recovered_info.chunks_finished:
                recovered_stats = recovered_info.chunks_finished
                logger.info(
                    "Recovered %d chunks for file_id %s",
                    len(recovered_stats),
                    self._resource_info.file_id,
                )

        # Initialize ChunkManager
        cm_params = ChunkManagerParams(
            file_name=self._file_name,
            file_size=self._resource_info.file_size if self._use_chunked else None,
            download_url=self._params.url,
        )
        self._chunk_manager = ChunkManager(
            params=cm_params,
            downloader=self._downloader,
            storage=self._storage,
            recovered_stats=recovered_stats,
        )

        self._status = EDownloadStatus.DOWNLOADING
        self._done_event.clear()
        self._controller_task = asyncio.create_task(self._dl_controller())

    async def _execute_strategy_action(self, action: AnyStrategyAction) -> None:
        if not self._chunk_manager:
            return
        if isinstance(action, StrategyAction.Start):
            self._chunk_manager.start_chunk(
                chunk_range=action.range,
                target_speed_bps=action.target_speed_bps,
            )
        elif isinstance(action, StrategyAction.Resize):
            self._chunk_manager.resize_chunk(
                current_range=action.current_range,
                new_range=action.new_range,
            )
            if action.target_speed_bps is not None:
                self._chunk_manager.set_speed_limit(
                    speed_bps=action.target_speed_bps,
                    chunk_range=action.new_range,
                )
        elif isinstance(action, StrategyAction.Cancel):
            await self._chunk_manager.cancel_chunk(action.range)
        elif isinstance(action, StrategyAction.SetSpeed):
            self._chunk_manager.set_speed_limit(
                speed_bps=action.target_speed_bps or 0.0,
                chunk_range=action.range,
            )
        else:
            logger.warning("Unknown strategy action received: %s", type(action))

    async def _finalize(self) -> None:
        if not self._chunk_manager:
            return

        stats = list(self._chunk_manager.stats.values())
        effective_file_size = (
            self._resource_info.file_size
            if (self._resource_info and self._use_chunked and self._resource_info.file_size > 0)
            else None
        )

        # Reconstruct final file from completed chunks
        await reconstruct_file(
            storage=self._storage,
            stats_list=stats,
            final_filename=self._file_name,
            total_file_size=effective_file_size,
        )

        # Clean up temporary chunk files
        await self._chunk_manager.cleanup(delete_files=True)

        # Remove recovery state if any
        if self._resource_info and self._resource_info.file_id:
            with contextlib.suppress(Exception):
                await self._recovery.delete_info(self._resource_info.file_id)

        # Update final stats with actual stored file size
        final_info = await self._storage.get_data_info(self._file_name)
        if final_info and self._dl_stats:
            self._dl_stats.set_bytes_downloaded(final_info.size_bytes)
            self._dl_stats.file_size = final_info.size_bytes
            self._dl_stats.progress = 100.0

        self._status = EDownloadStatus.COMPLETED
        logger.info("DownloadTask completed successfully for %s", self._file_name)

    async def _dl_controller(self) -> None:
        """
        Background supervisory loop coordinating chunk manager, strategy, and stats.
        """
        try:
            if not self._chunk_manager:
                return

            # 1. Query initial strategy actions
            initial_actions = self._strategy.on_start(
                dl_stats=self._dl_stats,
                chunks_stats=dict(self._chunk_manager.stats),
                available_slots=self._max_conn,
            )
            for action in initial_actions:
                await self._execute_strategy_action(action)

            # 2. Supervisory loop
            while self._status == EDownloadStatus.DOWNLOADING:
                await asyncio.sleep(0.2)

                if not self._chunk_manager:
                    break

                # Update task statistics
                current_stats = list(self._chunk_manager.stats.values())
                file_sz = self._resource_info.file_size if self._resource_info else None
                total_downloaded = calculate_downloaded_bytes(current_stats, file_size=file_sz)
                if self._dl_stats:
                    self._dl_stats.set_bytes_downloaded(total_downloaded)
                    self._dl_stats.update()

                # Check if any chunk failed
                errored = [s for s in current_stats if s.status == EDownloadStatus.ERROR]
                if errored:
                    raise errored[0].last_error or Exception("Chunk download failed")

                # Check if all chunks completed
                if current_stats and all(
                    s.status in (EDownloadStatus.COMPLETED, EDownloadStatus.DEPRECATED)
                    for s in current_stats
                ):
                    await self._finalize()
                    break

                # Query strategy for dynamic actions using O(1) active chunks property
                available_slots = max(0, self._max_conn - self._chunk_manager.qt_active_chunks)
                update_actions = self._strategy.on_update(
                    dl_stats=self._dl_stats,
                    chunks_stats=dict(self._chunk_manager.stats),
                    available_slots=available_slots,
                )
                for action in update_actions:
                    await self._execute_strategy_action(action)

        except asyncio.CancelledError:
            logger.debug("DownloadTask controller cancelled for %s", self._file_name)
            raise
        except Exception as exc:
            self._status = EDownloadStatus.ERROR
            self._last_error = exc
            logger.error("DownloadTask controller error for %s: %s", self._file_name, exc)
            if self._chunk_manager:
                await self._chunk_manager.cleanup(delete_files=False)
        finally:
            self._done_event.set()

    async def wait_until_done(self) -> None:
        """
        Waits for the download to finish.
        """
        if self._controller_task:
            await self._controller_task

    async def pause(self) -> None:
        """
        Pauses the download task and saves recovery state.
        """
        if self._status != EDownloadStatus.DOWNLOADING:
            return

        self._status = EDownloadStatus.PENDING
        if self._controller_task:
            self._controller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._controller_task

        if self._chunk_manager:
            if (
                self._params.enable_recovery
                and self._resource_info
                and self._resource_info.file_id
                and self._use_chunked
            ):
                stats_list = list(self._chunk_manager.stats.values())
                await self._recovery.save_info(
                    file_id=self._resource_info.file_id,
                    total_file_size=self._resource_info.file_size or 0,
                    stats_list=stats_list,
                )
            await self._chunk_manager.cleanup(delete_files=False)

    async def resume(self) -> None:
        """
        Resumes a paused download task.
        """
        await self.start()

    async def cancel(self, delete_temp_files: bool = True) -> None:
        """
        Cancels the download task and cleans up resources.
        """
        self._status = EDownloadStatus.CANCELLED
        if self._controller_task:
            self._controller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._controller_task

        if self._chunk_manager:
            await self._chunk_manager.cleanup(delete_files=delete_temp_files)

        if delete_temp_files and self._resource_info and self._resource_info.file_id:
            with contextlib.suppress(Exception):
                await self._recovery.delete_info(self._resource_info.file_id)
