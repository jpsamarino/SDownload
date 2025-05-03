import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import logging
import time
from typing import Any, AsyncIterator, Dict, Optional
from sDownload.interfaces.protocols.dowloader_manager_protocol import DLManagerConfig, URLConfig
from sDownload.interfaces.protocols.dowloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_info_model import FileInfoModel
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol


@dataclass
class ChunkDownloadStats:
    chunk_file_name: str
    start_byte: int
    end_byte: int
    file_size: int
    bytes_downloaded: int = 0
    progress: float = 0.0
    speed_bps: float = 0.0
    status: str = "pending" | "downloading" | "completed"
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)


@dataclass
class DownloadStats:
    file_size: int
    bytes_downloaded: int = 0
    progress: float = 0.0
    speed_bps: float = 0.0
    start_time: float = field(default_factory=time.monotonic)


@dataclass
class DownloadConfig:
    file_name: str
    file_dir: str | None
    file_size: int
    file_id: str | None
    download_url: str
    file_created_at: datetime
    protocol_data: dict | None
    max_connections_per_download: int | None = None
    max_speed_bytes_per_second: int | None = None


class DownloadTask:
    def __init__(
            self,
            download_config: DownloadConfig,
            use_chunked_download: bool,
            downloader_executor: DownloaderProtocol,
            file_storage_handler: FileStorageProtocol,
            _logger: logging.Logger,):
        self._use_chunked_download = use_chunked_download
        self._downloader_executor = downloader_executor
        self._file_storage_handler = file_storage_handler
        self._logger = _logger
        self._download_config = download_config
        self._chunks_download_tasks: Dict[str, asyncio.Task] = {}
        self._chunks_stats: Dict[str, ChunkDownloadStats] = {}
        self._download_stats: DownloadStats | None = None
        self._list_ranges_to_download: list[tuple[int, int]] = []

    async def _tracker_async_iterator(
        self,
        chunk_iterator: AsyncIterator[bytes],
        stats: ChunkDownloadStats,
    ) -> AsyncIterator[bytes]:
        async for chunk in chunk_iterator:
            stats.bytes_downloaded += len(chunk)
            yield chunk

    async def _update_chunk_stats_periodically(
        self,
        stats: ChunkDownloadStats,
        stop_event: asyncio.Event,
        timeout_seconds: float = 1.0,
    ):
        last_bytes_downloaded = stats.bytes_downloaded

        def update_stats():
            nonlocal last_bytes_downloaded
            now = time.monotonic()
            elapsed = now - stats.start_time
            stats.progress = 100.0 * stats.bytes_downloaded / stats.file_size
            stats.speed_bps = (
                stats.bytes_downloaded - last_bytes_downloaded) / elapsed
            stats.last_update = now
            self._logger.debug(
                f"[{stats.chunk_file_name}] {stats.progress:.2f}% - "
                f"{stats.bytes_downloaded} bytes at {stats.speed_bps:.2f} B/s"
            )
            last_bytes_downloaded = stats.bytes_downloaded

        while not stop_event.is_set():
            update_stats()
            await asyncio.sleep(timeout_seconds)

        update_stats()

    async def _download_chunk_part(self, start_byte: int = 0, end_byte: int = None):
        chunk_key = self._get_chunk_stats_name(start_byte, end_byte)
        chunk_name = f"{chunk_key}-{self._download_config.file_name}.sdownload"

        stats = ChunkDownloadStats(
            chunk_file_name=chunk_name,
            start_byte=start_byte,
            end_byte=end_byte,
            file_size=end_byte - start_byte + 1,
            status="pending",
        )

        self._chunks_stats[chunk_key] = stats
        stop_event = asyncio.Event()

        stats_task = asyncio.create_task(
            self._update_chunk_stats_periodically(stats, stop_event))

        try:
            chunk_iterator = self._downloader_executor.download_chunk(
                self._download_config.download_url,
                start_byte,
                end_byte,
            )

            tracked_iterator = self._tracker_async_iterator(
                chunk_iterator, stats)

            await self._file_storage_handler.save_binary_data(tracked_iterator, chunk_name)
            stats.status = "completed"
            self._logger.info(f"[{chunk_name}] download completed")

        except Exception as e:
            stats.status = "error"
            self._logger.error(f"[{chunk_name}] download failed: {e}")
            raise
        finally:
            stop_event.set()
            await stats_task

    def _get_chunk_stats_name(self, start_byte: int = 0, end_byte: int = None):
        return f"{start_byte}-{end_byte or 'EOF'}"

    def _verify_chunk_is_running(self, start_byte: int = 0, end_byte: int = None):
        return self._get_chunk_stats_name(start_byte, end_byte) in self._chunks_stats

    def _get_chunk_stats(self, start_byte: int = 0, end_byte: int = None):
        return self._chunks_stats.get(self._get_chunk_stats_name(start_byte, end_byte))

    def _get_chunk_task(self, start_byte: int = 0, end_byte: int = None):
        return self._chunks_download_tasks.get(self._get_chunk_stats_name(start_byte, end_byte))

    async def _download_chunks_sequential(self):
        for start_byte, end_byte in self._list_ranges_to_download:
            if self._verify_chunk_is_running(start_byte, end_byte):
                continue
            # add task in dict and wait
            chunk_task = asyncio.create_task(
                self._download_chunk_part(start_byte, end_byte))
            self._chunks_download_tasks[self._get_chunk_stats_name(
                start_byte, end_byte)] = chunk_task
            await chunk_task

        for start_byte, end_byte in self._list_ranges_to_download:
            chunk_stats = self._get_chunk_stats(start_byte, end_byte)
            if chunk_stats.progress < 100.0:  # error
                chunk_task = self._get_chunk_task(start_byte, end_byte)
                if chunk_task is None:
                    chunk_task = asyncio.create_task(
                        self._download_chunk_part(start_byte, end_byte))
                    self._chunks_download_tasks[self._get_chunk_stats_name(
                        start_byte, end_byte)] = chunk_task
                    await chunk_task
                else:
                    await chunk_task

    async def _update_download_stats_periodically(
        self,
        stats: ChunkDownloadStats,
        stop_event: asyncio.Event,
        timeout_seconds: float = 2.0,
    ):
        "it will update download stats periodically and add more chunks if needed"
        pass

    async def start(self):
        """
        """
        total_size = self._download_config.file_size
        self._download_stats = DownloadStats(file_size=total_size)

        max_conn = self._download_config.max_connections_per_download or 1
        use_chunks = self._use_chunked_download and max_conn > 1

        chunk_count = max_conn
        base = total_size // chunk_count
        remainder = total_size % chunk_count
        cursor = 0
        list_ranges_to_download = []

        for i in range(chunk_count):
            start_byte = cursor
            extra = 1 if i < remainder else 0
            end_byte = cursor + base + extra - 1
            cursor = end_byte + 1
            list_ranges_to_download.append((start_byte, end_byte))

        # start dowload sequentially
        asyncio.create_task(self._download_chunks_sequential())
        # start function to monitor download progress

        self._logger.info(
            f"Download started with {len(self._chunks_download_tasks)} task(s)"
        )

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
