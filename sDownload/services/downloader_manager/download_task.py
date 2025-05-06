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
    progress: float = 0.0
    speed_bps: float = 0.0
    status: str = "pending"  # pending | downloading | completed | error
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)

    def finalize(self):
        now = time.monotonic()
        elapsed = now - self.start_time
        self.progress = 100.0
        self.speed_bps = self.bytes_downloaded / elapsed if elapsed > 0 else 0
        self.last_update = now


@dataclass
class DownloadStats:
    file_size: int
    bytes_downloaded: int = 0
    progress: float = 0.0
    speed_bps: float = 0.0
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)

    def update(self):
        now = time.monotonic()
        elapsed = now - self.start_time
        self.progress = 100.0 * self.bytes_downloaded / self.file_size
        self.speed_bps = self.bytes_downloaded / elapsed if elapsed > 0 else 0
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
        self._monitor_task: Optional[asyncio.Task] = None
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
            async for data in it:
                stats.bytes_downloaded += len(data)
                yield data
        finally:
            stats.finalize()

    async def _periodic_stats(self, stats: ChunkDownloadStats, stop: asyncio.Event, interval: float = 1.0):
        prev = stats.bytes_downloaded
        while not stop.is_set():
            now = time.monotonic()
            elapsed = now - stats.start_time
            stats.progress = 100.0 * stats.bytes_downloaded / stats.file_size
            stats.speed_bps = (stats.bytes_downloaded - prev) / \
                elapsed if elapsed > 0 else 0
            prev = stats.bytes_downloaded
            self._logger.debug(
                f"[{stats.chunk_file_name}] {stats.progress:.1f}% @ {stats.speed_bps:.1f} B/s")
            await asyncio.sleep(interval)
        stats.finalize()

    async def _download_chunk(self, start: int, end: Optional[int], max_retries: int = 3):
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
                    self._cfg.download_url, start, end)
                tracked = self._tracker(raw_it, stats)
                await self._storage.save_binary_data(tracked, name)
                stats.status = "completed"
                self._logger.info(
                    "[%s] ending attempt %d", name, attempt)
                return

            except Exception as e:
                stats.status = "error"
                self._logger.warning(
                    "[%s] failed attempt %d: %s", name, attempt, e
                )
                stats.bytes_downloaded = 0
                stats.start_time = time.monotonic()
                stats.last_update = stats.start_time
                if attempt == max_retries:
                    self._logger.error(
                        "Chunk %s failed %d attempts", name, max_retries)
                    raise
                await asyncio.sleep(attempt)

            finally:
                stop.set()
                await stats_task
                del self._chunks_tasks[key]

    def get_bytes_dowloaded(self) -> int:
        return sum([s.bytes_downloaded for s in self._chunks_stats.values()]) or 0

    def update_stats(self) -> int:
        _bytes_downloaded = self.get_bytes_dowloaded()
        self._download_stats.bytes_downloaded = _bytes_downloaded
        self._download_stats.update()

    async def _monitor(self):
        last_speed = 0.0
        self.update_stats()
        cs = 1
        while self._download_stats.bytes_downloaded < self._cfg.file_size or cs > 0:
            await asyncio.sleep(2)
            self.update_stats()
            cs = len(self._chunks_tasks)
            if (
                cs < self._max_conn
                # and self._target_speed > 0
                and self._download_stats.speed_bps < self._target_speed  # change it
                # and self._download_stats.speed_bps > last_speed
                and self._pending
            ):
                start, end = self._pending.pop(0)  # fix
                task = asyncio.create_task(self._download_chunk(start, end))
                self._chunks_tasks[self._key(start, end)] = task
                self._logger.info(
                    f"new chunk {start}-{end or 'EOF'} started")
            last_speed = self._download_stats.speed_bps
        self._logger.info("Monitor task end.")
        # join all finished FILES  and delete them

    async def _watch_dog_verification(self, timeout: int = 10):
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

    async def start(self):
        if not self._pending:
            return
        start, end = self._pending.pop(0)
        first = asyncio.create_task(self._download_chunk(start, end))
        self._chunks_tasks[self._key(start, end)] = first
        self._monitor_task = asyncio.create_task(self._monitor())
        await self._monitor_task
        self._logger.info("DownloadTask finished.")

    async def wait_util_done(self):
        ...

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
