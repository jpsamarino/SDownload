import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import logging
import time
from typing import AsyncIterator, Dict, List, Optional, Tuple
from sDownload.interfaces.protocols.dowloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol


@dataclass
class ChunkDownloadStats:
    chunk_file_name: str
    start_byte: int
    end_byte: Optional[int]
    file_size: int
    bytes_downloaded: int = 0
    qt_bytes_last_update: int = 0
    progress: float = 0.0
    speed_bps: float = 0.0
    status: str = "pending"  # pending | downloading | completed | error
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)
    target_speed_bps: float = float("inf")

    def update(self):
        qt_bytes_elapsed = self.bytes_downloaded - self.qt_bytes_last_update
        self.qt_bytes_last_update = self.bytes_downloaded
        now = time.monotonic()
        time_elapsed = now - self.last_update
        self.last_update = now
        self.progress = 100.0 * self.bytes_downloaded / self.file_size
        self.speed_bps = qt_bytes_elapsed / time_elapsed if time_elapsed > 0 else 0


@dataclass
class DownloadStats:
    file_size: int
    bytes_downloaded: int = 0
    qt_bytes_last_update: int = 0
    progress: float = 0.0
    speed_bps: float = 0.0
    avg_speed_bps: float = 0.0
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)

    def update(self):
        now = time.monotonic()
        time_elapsed_avg = now - self.start_time
        self.progress = 100.0 * self.bytes_downloaded / self.file_size
        self.avg_speed_bps = self.bytes_downloaded / \
            time_elapsed_avg if time_elapsed_avg > 0 else 0
        qt_bytes_elapsed = self.bytes_downloaded - self.qt_bytes_last_update
        self.qt_bytes_last_update = self.bytes_downloaded
        time_elapsed_period = now - self.last_update
        self.speed_bps = qt_bytes_elapsed / \
            time_elapsed_period if time_elapsed_period > 0 else 0
        self.last_update = now


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
        self._init_ranges()

    def _init_ranges(self):
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
            self._pending.append(
                (cur, end if end < total - 1 else None))  # verificar
            cur = end + 1

    def _key(self, start_byte: int, end_byte: Optional[int]) -> str:
        return f"{start_byte}_{end_byte or 'EOF'}"

    async def _tracker(self, it: AsyncIterator[bytes], stats: ChunkDownloadStats):
        try:
            start_time = time.monotonic()
            accumulated_bytes = 0
            async for data in it:
                qt_bytes = len(data)
                stats.bytes_downloaded += qt_bytes
                accumulated_bytes += qt_bytes
                yield data
                # to control the dl speed based on target speed
                if accumulated_bytes > stats.target_speed_bps:
                    time_elapsed = time.monotonic() - start_time
                    time_expected = accumulated_bytes / stats.target_speed_bps
                    if time_elapsed < time_expected:
                        await asyncio.sleep(min(1, time_expected - time_elapsed))
                    start_time = time.monotonic()
                    accumulated_bytes = 0

        finally:
            stats.update()

    async def _periodic_stats(self, stats: ChunkDownloadStats, stop: asyncio.Event, interval: float = 1.0):
        while not stop.is_set():
            stats.update()
            self._logger.debug(
                "[%s] %.1f%% @ %.2f MB/s - limit %.2f MB/s",
                stats.chunk_file_name,
                stats.progress,
                stats.speed_bps / (1024 * 1024),
                stats.target_speed_bps / (1024 * 1024)
            )
            await asyncio.sleep(interval)
        stats.update()

    async def _download_chunk(self, start: int, end: Optional[int], max_retries: int = 3) -> str:
        key = self._key(start, end)
        name = f"{key}_{self._cfg.file_name}.sdownload"
        end_byte = end if end is not None else self._cfg.file_size - 1
        file_size = end_byte - start + 1
        stats = ChunkDownloadStats(
            chunk_file_name=name,
            start_byte=start,
            end_byte=end_byte,
            file_size=file_size,
            status="pending"
        )
        self._chunks_stats[key] = stats

        for attempt in range(1, max_retries + 1):
            stats.status = "downloading"
            stop = asyncio.Event()
            stats_task = asyncio.create_task(self._periodic_stats(stats, stop))
            try:
                self._logger.debug(
                    "[Attempt %s] byte range [%s]-[%s]", attempt, start, end or "EOF")
                raw_it = self._downloader.download_chunk(
                    self._cfg.download_url, start, end_byte)
                tracked = self._tracker(raw_it, stats)
                await self._storage.save_binary_data(name, tracked)
                stats.status = "completed"
                self._logger.info(
                    "[%s] ending attempt %s", name, attempt)

            except Exception as e:
                stats.status = "error"
                self._logger.warning(
                    "[%s] failed attempt %s: %s", name, attempt, e
                )
                stats.bytes_downloaded = 0
                stats.start_time = time.monotonic()
                stats.last_update = stats.start_time
                if attempt == max_retries:
                    self._logger.error(
                        "Chunk %s failed %s attempts", name, max_retries)
                    raise
                await asyncio.sleep(attempt)

            finally:
                stop.set()
                await stats_task
            return key

    def _get_bytes_dowloaded(self) -> int:
        return sum([s.bytes_downloaded for s in self._chunks_stats.values()]) or 0

    def _update_stats(self) -> int:
        _bytes_downloaded = self._get_bytes_dowloaded()
        self._download_stats.bytes_downloaded = _bytes_downloaded
        self._download_stats.update()

    def _set_speed_per_chunk(self, speed: float) -> None:
        if self._target_speed < float("inf"):
            for s in self._chunks_stats.values():
                s.target_speed_bps = speed

    async def _dl_controller(self, timeout: float = 2):
        self._update_stats()
        self._set_speed_per_chunk(self._target_speed)

        while (
            self._download_stats.bytes_downloaded < self._cfg.file_size
            or self._chunks_tasks
        ):
            done, _ = await asyncio.wait(
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
                        "Download task failed: %s", e, exc_info=True)

            if (
                len(self._chunks_tasks) < self._max_conn
                and self._download_stats.speed_bps < self._target_speed
                and self._pending
            ):
                self._start_next_chunk()

            self._update_stats()
            connections = max(len(self._chunks_tasks), 1)
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

    async def _join_all_files(self, delete_temp_files: bool = False):
        file_names = [s.chunk_file_name for s in self._chunks_stats.values()]
        await self._storage.merge_binary_files(file_names, self._cfg.file_name)
        if delete_temp_files:
            for file_name in file_names:
                await self._storage.delete_data(file_name)

    async def _watch_dog_verification(self, timeout: int = 10):
        # make it work
        """
        each 10 seconds check if all chunks runing if not working stop
        if not working delete task and put back to pending and retry with only 1 connection
        """
        while True:
            await asyncio.sleep(timeout)
            for stats in self._chunks_stats.values():
                if stats.speed_bps == 0 and stats.status == "downloading":
                    self._max_conn = self._max_conn - 1 if self._max_conn > 1 else 1
                    self._logger.info(
                        f"chunk {stats.chunk_file_name} failed")
                    del self._chunks_tasks[self._key(
                        stats.start_byte, stats.end_byte)]
                    # del files in future
                    self._pending.append(
                        (stats.start_byte, stats.end_byte))
                    self._download_stats.bytes_downloaded -= stats.bytes_downloaded

    def start(self):
        self._logger.info("subtask to [%s] started.", self._cfg.file_name)
        self._start_next_chunk()
        self._dl_controller_task = asyncio.create_task(self._dl_controller())

    async def wait_util_done(self):
        await self._dl_controller_task

    def set_target_speed(self, target_speed_bs: int):
        self._target_speed = target_speed_bs

    async def get_status(self):
        ...

    async def cancel(self):
        ...

    async def resume(self):
        ...

    async def pause(self):
        ...

    async def stop(self):
        ...
