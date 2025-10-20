import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import logging
import time
from typing import AsyncIterator, Dict, List, Optional, Tuple
from sDownload.interfaces.protocols.dowloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    DownloadStats,
    EDownloadStatus,
)


@dataclass
class DownloadConfig:
    file_name: str
    file_dir: Optional[str]
    file_size: int
    file_id: Optional[str]
    download_url: str
    file_created_at: datetime
    protocol_data: Optional[dict]
    max_connections_per_download: int = 1
    max_speed_bytes_per_second: float = float("inf")  # bytes/s


class DownloadTask:
    def __init__(
        self,
        cfg: DownloadConfig,
        downloader: DownloaderProtocol,
        storage: FileStorageProtocol,
        use_chunked_download: bool = True,
        logger: logging.Logger = logging.getLogger(__name__),
    ):
        self._cfg = cfg
        self._use_chunked_download = use_chunked_download
        self._downloader = downloader
        self._storage = storage
        self._logger = logger
        self._max_conn = max(1, cfg.max_connections_per_download)
        self._target_speed = cfg.max_speed_bytes_per_second
        self._download_stats = DownloadStats(cfg.file_size)
        self._chunks_stats: Dict[str, ChunkDownloadStats] = {}
        self._chunks_tasks: Dict[str, asyncio.Task] = {}
        self._pending: List[Tuple[int, Optional[int]]] = []
        self._dl_controller_task: Optional[asyncio.Task] = None
        self._pause_event = asyncio.Event()
        self._recovery_mode = False
        self._pause_event.set()
        self._init_ranges()

    def _init_ranges(self):
        self._pending = []
        if not self._use_chunked_download:
            self._max_conn = 1
            self._pending.append((0, None))
            return
        total, parts = self._cfg.file_size, self._max_conn
        base, rem = divmod(total, parts)
        cur = 0
        for i in range(parts):
            extra = 1 if i < rem else 0
            end = cur + base + extra - 1
            self._pending.append((cur, end if end < total - 1 else None))  # verificar
            cur = end + 1

    def _key(self, start_byte: int, end_byte: Optional[int]) -> str:
        return f"{start_byte}_{end_byte or 'EOF'}"

    async def _tracker(self, it: AsyncIterator[bytes], stats: ChunkDownloadStats):
        try:
            start_time = time.monotonic()
            accumulated_bytes = 0
            async for data in it:
                qt_bytes = len(data)
                stats.add_qt_bytes_downloaded(qt_bytes)
                accumulated_bytes += qt_bytes
                yield data
                if (
                    stats.target_speed_bps
                    and accumulated_bytes > stats.target_speed_bps
                ):
                    time_elapsed = time.monotonic() - start_time
                    time_expected = accumulated_bytes / stats.target_speed_bps
                    if time_elapsed < time_expected:
                        await asyncio.sleep(min(1, time_expected - time_elapsed))
                    start_time = time.monotonic()
                    accumulated_bytes = 0
        finally:
            self._logger.info("_tracker finished - %s", stats.chunk_file_name)
            stats.update()
            try:
                await it.aclose()
            except (RuntimeError, AttributeError):
                pass

    async def _periodic_stats(
        self, stats: ChunkDownloadStats, stop: asyncio.Event, interval: float = 1.0
    ):
        while not stop.is_set():
            self._logger.info("_periodic_stats loop")
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

    async def _download_chunk(
        self, start: int, end: Optional[int], max_retries: int = 3
    ) -> str:
        key = self._key(start, end)
        name = f"{key}_{self._cfg.file_name}.sdownload"
        end_byte = end if end is not None else self._cfg.file_size - 1
        file_size = end_byte - start + 1
        stats = ChunkDownloadStats(
            chunk_file_name=name,
            start_byte=start,
            end_byte=end_byte,
            file_size=file_size,
            target_speed_bps=self._target_speed
            * 0.1,  # mudar para 1 está 0.1 para testes
        )
        self._chunks_stats[key] = stats

        for attempt in range(1, max_retries + 1):
            self._logger.info("_download_chunk loop")
            stats.set_status(EDownloadStatus.DOWNLOADING)
            stop = asyncio.Event()
            stats_task = asyncio.create_task(self._periodic_stats(stats, stop))
            try:
                self._logger.info(
                    "[Attempt %s] byte range [%s]-[%s]", attempt, start, end or "EOF"
                )
                raw_it = self._downloader.download_chunk(
                    self._cfg.download_url, start, end_byte
                )
                tracked = self._tracker(raw_it, stats)
                await self._storage.save_binary_data(name, tracked)
                stats.set_status(EDownloadStatus.COMPLETED)
                self._logger.info("[%s] ending attempt %s", name, attempt)

            except asyncio.CancelledError:
                self._logger.info("_download_chunk cancelled - %s", name)
                stats.set_status(EDownloadStatus.CANCELLED)
                self._logger.warning(
                    "[%s] download cancelled on attempt %s", name, attempt
                )
                # stop.set()
                # await stats_task
                raise

            except Exception as e:
                stats.set_status(EDownloadStatus.ERROR)
                self._logger.warning("[%s] failed attempt %s: %s", name, attempt, e)
                stats.bytes_downloaded = 0
                stats.start_time = time.monotonic()
                stats.last_update = stats.start_time
                if attempt == max_retries:
                    self._logger.error("Chunk %s failed %s attempts", name, max_retries)
                    raise
                await asyncio.sleep(attempt)

            finally:
                stop.set()
                await stats_task
            return key

    def _get_bytes_dowloaded(self) -> int:
        return sum([s.bytes_downloaded for s in self._chunks_stats.values()]) or 0

    def _update_stats(self) -> int:
        self._logger.info("_update_stats loop")
        _bytes_downloaded = self._get_bytes_dowloaded()
        self._download_stats.set_bytes_downloaded(_bytes_downloaded)
        self._download_stats.update()

    def _set_speed_per_chunk(self, speed: float) -> None:
        if self._target_speed < float("inf"):
            for s in self._chunks_stats.values():
                s.target_speed_bps = speed

    async def _dl_controller(self, timeout: float = 2):
        await self._pause_event.wait()

        self._update_stats()
        self._set_speed_per_chunk(self._target_speed)

        while (
            self._download_stats.bytes_downloaded < self._cfg.file_size
            or self._chunks_tasks
        ):
            await self._pause_event.wait()
            await self._watch_dog()

            self._logger.info("_dl_controller loop")

            if self._chunks_tasks.keys():
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
                        self._logger.warning(
                            "Download task failed: %s", e, exc_info=True
                        )

            elif not self._pending:
                self._logger.info("Possible error: _dl_controller")

            else:
                self._logger.info(
                    f"_dl_controller no chunks tasks qt task: {len(_not_done)}/{len(self._chunks_tasks)}"
                )
                await asyncio.sleep(timeout)

            qt_connections = len(self._chunks_tasks)

            if (
                qt_connections < self._max_conn
                and self._download_stats.speed_bps < self._target_speed
                and self._pending
            ) or qt_connections == 0:
                self._start_next_chunk()

            self._update_stats()
            connections = max(len(self._chunks_tasks), 1)
            self._logger.info(
                "Downloaded %s | size: %d bytes | speed: %.2f MB/s | connections: %s | limit speed: %.2f MB/s",
                self._cfg.file_name,
                self._cfg.file_size,
                self._download_stats.speed_bps / (1024 * 1024),
                connections,
                (self._target_speed / connections) / (1024 * 1024),
            )
            self._set_speed_per_chunk(self._target_speed / connections)

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

    def _start_next_chunk(self):
        if self._pending:
            start, end = self._pending.pop(0)
            task = asyncio.create_task(self._download_chunk(start, end))
            self._chunks_tasks[self._key(start, end)] = task
            self._logger.info("Chunk %s-%s started", start, end or "EOF")

    async def _delete_all_temp_files(self):
        self._logger.info("_delete_all_temp_files")
        files_to_delete = [s.chunk_file_name for s in self._chunks_stats.values()]
        files_names_in_storage = [s.key for s in await self._storage.list_data()]
        files_to_delete_in_storage = [
            s for s in files_to_delete if s in files_names_in_storage
        ]
        self._logger.info(files_to_delete_in_storage)
        delete_tasks = [
            self._storage.delete_data(s) for s in files_to_delete_in_storage
        ]
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

    def _start_download(self, connections: int = 1):
        self._max_conn = connections
        self._download_stats = DownloadStats(self._cfg.file_size)
        self._init_ranges()
        self._logger.warning("Starting download with %s connections", self._max_conn)
        self._start_next_chunk()

    async def _watch_dog(self, timeout_without_update: int = 3):
        try:
            now = time.monotonic()
            restart_dl = False

            for _, stats in list(self._chunks_stats.items()):
                if (
                    stats.status == EDownloadStatus.DOWNLOADING and stats.speed_bps == 0
                ) or stats.status == EDownloadStatus.ERROR:
                    if (now - stats.last_update) > timeout_without_update:
                        restart_dl = True
                        self._logger.info(
                            f"[WacthDog] chunk {stats.chunk_file_name} entered reason {stats.status} , speed {stats.speed_bps}"
                        )
                        break

            if restart_dl and (not self._recovery_mode):
                self._recovery_mode = True
                await self._delete_all_tasks()
                # await to avoid some server blocks system
                await asyncio.sleep(5)
                self._start_download(connections=1)

            elif restart_dl:
                await self._delete_all_tasks()
                self._logger.info("Restarting download in 120 seconds")
                await asyncio.sleep(120)
                self._start_download(connections=1)

        except Exception as e:
            self._logger.error(f"[Watchdog] Unexpected error: {e}", exc_info=True)
            raise e

    def start(self):
        self._logger.info("subtask to [%s] started.", self._cfg.file_name)
        self._start_next_chunk()
        self._dl_controller_task = asyncio.create_task(self._dl_controller())
        # self._watchdog_task = asyncio.create_task(
        #     self._watch_dog_runtime())

    async def wait_util_done(self):
        await self._dl_controller_task
        # if hasattr(self, "_watchdog_task"):
        #     self._watchdog_task.cancel()
        #     try:
        #         await self._watchdog_task
        #     except asyncio.CancelledError:
        #         self._logger.info("Watchdog task cancelled.")

    def set_target_speed(self, target_speed_bs: int):
        self._target_speed = target_speed_bs

    async def get_status(self): ...

    async def cancel(self): ...

    async def resume(self): ...

    async def pause(self): ...

    async def stop(self): ...
