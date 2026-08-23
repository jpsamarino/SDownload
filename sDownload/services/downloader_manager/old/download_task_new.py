import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime

from sDownload.services.downloader_manager.throttle_and_track_async_stream import (
    throttle_and_track_async_stream,
)

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
    DownloadStats,
    EDownloadStatus,
    StrategyAction,
)
from sDownload.interfaces.protocols import (
    DownloaderProtocol,
    DownloadStrategyProtocol,
    FileStorageProtocol,
)
from sDownload.services.downloader_manager.strategies.multi_chunk_strategy import (
    MultiChunkDownloadStrategy,
)


@dataclass
class DownloadConfig:
    file_name: str
    file_dir: str | None
    file_size: int
    file_id: str | None
    download_url: str
    file_created_at: datetime
    protocol_data: dict | None
    max_connections_per_download: int = 1
    max_speed_bytes_per_second: int | None = None  # use None for unlimited
    use_chunked_download: bool = (True,)


class DownloadTask:
    def __init__(
        self,
        cfg: DownloadConfig,
        downloader: DownloaderProtocol,
        storage: FileStorageProtocol,
        strategy: DownloadStrategyProtocol = None,
        logger: logging.Logger = logging.getLogger(__name__),
    ):
        self._cfg = cfg
        self._use_chunked_download = cfg.use_chunked_download
        self._downloader = downloader
        self._storage = storage
        self._logger = logger
        self._max_conn = max(1, cfg.max_connections_per_download)
        self._target_speed = cfg.max_speed_bytes_per_second
        self._download_stats = DownloadStats(cfg.file_size)
        self._chunks_stats: dict[str, ChunkDownloadStats] = {}  # never deleted ( only when error )
        self._chunks_tasks: dict[str, asyncio.Task] = {}  # active tasks
        # self._pending: List[Tuple[int, Optional[int]]] = []
        self._dl_controller_task: asyncio.Task | None = None
        self._pause_event = asyncio.Event()
        self._downloader_strategy = (
            strategy if strategy else MultiChunkDownloadStrategy(self._max_conn)
        )
        self._recovery_mode = False
        self._pause_event.set()

    def _key(self, start_byte: int, end_byte: int | None) -> str:
        return f"{start_byte}_{end_byte or 'EOF'}"

    async def _periodic_stats(
        self, stats: ChunkDownloadStats, stop: asyncio.Event, interval: float = 1.0
    ):
        while not stop.is_set():
            stats.update()
            self._logger.info(
                "[%s] %.1f%% @ %.2f MB/s - limit %.2f MB/s",
                stats.chunk_file_name,
                stats.progress,
                stats.speed_bps / (1024 * 1024),
                stats.target_speed_bps / (1024 * 1024),
            )
            await asyncio.sleep(interval)
        stats.update()

    async def _download_chunk(self, start: int, end: int | None) -> str | None:
        key = self._key(start, end)
        name = f"{key}_{self._cfg.file_name}.sdownload"
        end_byte = end if end is not None else self._cfg.file_size - 1
        file_size = end_byte - start + 1
        stats = ChunkDownloadStats(
            chunk_file_name=name,
            range=ChunkRange(start, end_byte),
            file_size=file_size,
            target_speed_bps=self._target_speed * 0.1,  # mudar para 1 está 0.1 para testes
        )
        self._chunks_stats[key] = stats

        self._logger.info("_download_chunk")
        stats.set_status(EDownloadStatus.DOWNLOADING)
        stop = asyncio.Event()
        stats_task = asyncio.create_task(self._periodic_stats(stats, stop))
        try:
            self._logger.info("byte range [%s]-[%s]", start, end or "EOF")
            raw_it = self._downloader.download_chunk(self._cfg.download_url, start, end_byte)
            tracked = throttle_and_track_async_stream(raw_it, stats)
            await self._storage.save_binary_data(name, tracked)
            stats.set_status(EDownloadStatus.COMPLETED)
            self._logger.info("[%s] ending", name)

        except asyncio.CancelledError:
            stats.set_status(EDownloadStatus.CANCELLED)
            self._logger.warning("[%s] download cancelled", name)
            raise

        except Exception as e:
            stats.set_status(EDownloadStatus.ERROR)
            self._logger.warning("[%s] failed: %s", name, e)
            stats.bytes_downloaded = 0
            stats.start_time = time.monotonic()
            stats.last_update = stats.start_time
            raise

        finally:
            stop.set()
            await stats_task
        return key

    def _get_bytes_dowloaded(self) -> int:
        return sum([s.bytes_downloaded for s in self._chunks_stats.values()]) or 0

    def _update_stats(self) -> None:
        self._logger.info("_update_stats loop")
        _bytes_downloaded = self._get_bytes_dowloaded()
        self._download_stats.set_bytes_downloaded(_bytes_downloaded)
        self._download_stats.update()
        # print download stats
        self._logger.info(
            "Downloaded %d of %d bytes (%.2f%%)",
            _bytes_downloaded,
            self._cfg.file_size,
            _bytes_downloaded / self._cfg.file_size * 100,
        )

    def _set_speed_per_chunk(self, speed: float) -> None:
        self._logger.info("_set_speed_per_chunk")
        if self._target_speed < float("inf"):
            for s in self._chunks_stats.values():
                s.target_speed_bps = speed

    async def _dl_controller(self, timeout: float = 2):
        await self._pause_event.wait()
        self._update_stats()

        while self._download_stats.bytes_downloaded < self._cfg.file_size or self._chunks_tasks:
            await self._pause_event.wait()
            self._logger.info("_dl_controller loop")

            if self._chunks_tasks:
                done, _not_done = await asyncio.wait(
                    list(self._chunks_tasks.values()),
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in done:
                    try:
                        key = task.result()
                        del self._chunks_tasks[key]
                    except Exception as e:
                        self._logger.warning("Download task failed: %s", e, exc_info=True)

            self._update_stats()

            operation_actions = self._downloader_strategy.on_update(
                self._download_stats, self._chunks_stats
            )
            for action in operation_actions:
                match action:
                    case StrategyAction.Start(range, speed):
                        self._chunks_tasks[self._key(range.start, range.end)] = asyncio.create_task(
                            self._download_chunk(range.start, range.end)
                        )
                    case StrategyAction.Cancel(range):
                        key = self._key(range.start, range.end)
                        if key in self._chunks_tasks:
                            self._chunks_tasks[key].cancel()
                            try:
                                await self._chunks_tasks[key]
                            except asyncio.CancelledError:
                                self._logger.info("Chunk task %s cancelled.", range)

            qt_chunks = len(self._chunks_tasks)
            if qt_chunks > 0:
                speed_per_chunk = self._target_speed / qt_chunks
                self._set_speed_per_chunk(speed_per_chunk)

        elapsed = time.monotonic() - self._download_stats.start_time
        final_speed = self._download_stats.bytes_downloaded / elapsed
        self._logger.info(
            "Finished downloading %s | size: %d bytes | time: %.2f sec | avg speed: %.2f MB/s",
            self._cfg.file_name,
            self._cfg.file_size,
            elapsed,
            final_speed / (1024 * 1024),
        )

        await self._join_all_files()

    async def _delete_all_temp_files(self):
        self._logger.info("_delete_all_temp_files")
        files_to_delete = [s.chunk_file_name for s in self._chunks_stats.values()]
        files_names_in_storage = [s.key for s in await self._storage.list_data()]
        files_to_delete_in_storage = [s for s in files_to_delete if s in files_names_in_storage]
        self._logger.info(files_to_delete_in_storage)
        delete_tasks = [self._storage.delete_data(s) for s in files_to_delete_in_storage]
        self._logger.info(files_to_delete)
        await asyncio.gather(*delete_tasks)

    async def _join_all_files(self, delete_temp_files: bool = True):
        file_names = [s.chunk_file_name for s in self._chunks_stats.values()]
        await self._storage.merge_binary_files(file_names, self._cfg.file_name)
        if delete_temp_files:
            await self._delete_all_temp_files()

    async def _delete_all_tasks(self):
        for key_n, task in self._chunks_tasks.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                self._logger.info("Chunk task %s cancelled.", key_n)
        self._chunks_tasks.clear()
        await self._delete_all_temp_files()
        self._chunks_stats.clear()

    async def _watch_dog(self, timeout_without_update: int = 3): ...

    def _start_download(self):
        operation_actions = self._downloader_strategy.on_start(
            self._download_stats, self._chunks_stats
        )
        for action in operation_actions:
            match action:
                case StrategyAction.Start(range, speed):
                    self._chunks_tasks[self._key(range.start, range.end)] = asyncio.create_task(
                        self._download_chunk(range.start, range.end)
                    )

    def start(self):
        self._logger.info("subtask to [%s] started.", self._cfg.file_name)
        self._start_download()
        self._dl_controller_task = asyncio.create_task(self._dl_controller())

    async def wait_util_done(self):
        if self._dl_controller_task:
            await self._dl_controller_task

    def set_target_speed(self, target_speed_bs: int):
        self._target_speed = target_speed_bs

    async def get_status(self): ...

    async def cancel(self): ...

    async def resume(self): ...

    async def pause(self): ...

    async def stop(self): ...
