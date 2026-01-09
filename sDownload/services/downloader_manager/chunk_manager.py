import asyncio
import logging
import time
from sDownload.interfaces.protocols.downloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    EDownloadStatus,
)
from sDownload.services.downloader_manager.download_task import DownloadConfig
from sDownload.services.downloader_manager.throttle_and_track_async_stream import (
    throttle_and_track_async_stream,
)


class ChunkManager:
    def __init__(
        self,
        cfg: DownloadConfig,
        downloader: DownloaderProtocol,
        storage: FileStorageProtocol,
        logger: logging.Logger = logging.getLogger(__name__),
    ):
        self._cfg = cfg
        self._downloader = downloader
        self._storage = storage
        self._logger = logger
        self._chunks_stats: dict[str, ChunkDownloadStats] = {}
        self._chunks_tasks: dict[str, asyncio.Task] = {}

    def _key(self, start_byte: int, end_byte: None | int) -> str:
        return f"{start_byte}_{end_byte or 'EOF'}"

    async def _download_chunk(self, start: int, end: None | int) -> str | None:
        key = self._key(start, end)
        name = f"{key}_{self._cfg.file_name}.sdownload"
        end_byte = end if end is not None else self._cfg.file_size - 1
        file_size = end_byte - start + 1
        stats = ChunkDownloadStats(
            chunk_file_name=name,
            start_byte=start,
            end_byte=end_byte,
            file_size=file_size,
            target_speed_bps=self._cfg.max_speed_bytes_per_second * 0.1,
        )
        self._chunks_stats[key] = stats

        self._logger.info("_download_chunk")
        stats.set_status(EDownloadStatus.DOWNLOADING)
        stop = asyncio.Event()
        stats_task = asyncio.create_task(self._periodic_stats(stats, stop))
        try:
            self._logger.info("byte range [%s]-[%s]", start, end or "EOF")
            raw_it = self._downloader.download_chunk(
                self._cfg.download_url, start, end_byte
            )
            tracked = throttle_and_track_async_stream(raw_it, stats)
            await self._storage.save_binary_data(name, tracked)
            if stats.bytes_downloaded != file_size:
                raise IOError(
                    f"Chunk size error: expected {file_size} bytes, got {stats.bytes_downloaded} bytes"
                )
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
            # del self._chunks_tasks[key]
        return key

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

    def start_chunk(self, start: int, end: None | int) -> None:
        key = self._key(start, end)
        if key not in self._chunks_tasks:
            self._chunks_tasks[key] = asyncio.create_task(
                self._download_chunk(start, end),
                name=key,
            )

    async def cancel_chunk(self, start: int, end: None | int) -> bool:
        key = self._key(start, end)
        if key in self._chunks_tasks:
            self._chunks_stats[key].update()
            if self._chunks_stats[key].status == EDownloadStatus.DOWNLOADING:
                self._chunks_tasks[key].cancel()
                try:
                    await self._chunks_tasks[key]
                except asyncio.CancelledError:
                    self._logger.info("Chunk task %s cancelled.", key)

                del self._chunks_tasks[key]
                return True

            self._logger.info(
                "Chunk %s not cancelled because its status is %s",
                key,
                self._chunks_stats[key].status,
            )
            return False

    def get_active_chunks(self) -> list[str]:
        return list(self._chunks_tasks.keys())

    def get_chunk_stats(self, start: int, end: None | int) -> None | ChunkDownloadStats:
        key = self._key(start, end)
        return self._chunks_stats.get(key)

    def get_all_chunk_stats(self) -> dict[str, ChunkDownloadStats]:
        return self._chunks_stats.copy()

    def get_downloaded_bytes(self) -> int:
        return sum([s.bytes_downloaded for s in self._chunks_stats.values()]) or 0

    async def cleanup_temp_files(self) -> None:
        self._logger.info("Cleaning up temp files")
        files_to_delete = [s.chunk_file_name for s in self._chunks_stats.values()]
        files_names_in_storage = [s.key for s in await self._storage.list_data()]
        files_to_delete_in_storage = [
            s for s in files_to_delete if s in files_names_in_storage
        ]
        self._logger.info("Files to delete: %s", files_to_delete_in_storage)
        delete_tasks = [
            self._storage.delete_data(s) for s in files_to_delete_in_storage
        ]
        await asyncio.gather(*delete_tasks)

    async def cancel_all_chunks(self) -> None:
        for key, task in list(self._chunks_tasks.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                self._logger.info("Chunk task %s cancelled.", key)
        self._chunks_tasks.clear()

    async def wait_for_completed_chunks(self, timeout: float = 2.0) -> list[str]:
        if not self._chunks_tasks:
            return []

        done, _ = await asyncio.wait(
            self._chunks_tasks.values(),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        completed_keys: list[str] = []

        for task in done:
            key = task.get_name()
            try:
                task.result()
                completed_keys.append(key)
            except Exception as e:
                self._logger.warning("Chunk task %s failed: %s", key, e, exc_info=True)
            finally:
                self._chunks_tasks.pop(key, None)

        return completed_keys
