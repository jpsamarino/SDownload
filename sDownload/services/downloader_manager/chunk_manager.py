import asyncio
import logging
import time
from sDownload.interfaces.protocols.chunk_models import ChunkRange
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
        self._chunks_stats: dict[ChunkRange, ChunkDownloadStats] = {}
        self._chunks_tasks: dict[ChunkRange, asyncio.Task] = {}

    async def _download_chunk(self, chunk_range: ChunkRange) -> ChunkRange | None:

        name = f"{chunk_range}_{self._cfg.file_name}.sdownload"
        end_byte = (
            chunk_range.end if chunk_range.end is not None else self._cfg.file_size - 1
        )
        file_size = end_byte - chunk_range.start + 1
        stats = ChunkDownloadStats(
            chunk_file_name=name,
            start_byte=chunk_range.start,
            end_byte=end_byte,
            file_size=file_size,
            target_speed_bps=self._cfg.max_speed_bytes_per_second * 0.1,
        )
        self._chunks_stats[chunk_range] = stats

        self._logger.info("_download_chunk")
        stats.set_status(EDownloadStatus.DOWNLOADING)
        stop = asyncio.Event()
        stats_task = asyncio.create_task(self._periodic_stats(stats, stop))
        try:
            self._logger.info(
                "byte range [%s]-[%s]", chunk_range.start, chunk_range.end or "EOF"
            )
            raw_it = self._downloader.download_chunk(
                self._cfg.download_url, chunk_range.start, end_byte
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
        return chunk_range

    async def _periodic_stats(
        self, stats: ChunkDownloadStats, stop: asyncio.Event, interval: float = 1.0
    ):
        while not stop.is_set():
            stats.update()
            self._logger.debug(
                "[%s] %.1f%% @ %.2f MB/s - limit %.2f MB/s",
                stats.chunk_file_name,
                stats.progress,
                stats.speed_bps / (1024 * 1024),
                stats.target_speed_bps / (1024 * 1024),
            )
            await asyncio.sleep(interval)
        stats.update()

    def start_chunk(self, chunk_range: ChunkRange) -> None:
        if chunk_range not in self._chunks_tasks:
            self._chunks_tasks[chunk_range] = asyncio.create_task(
                self._download_chunk(chunk_range),
            )

    async def cancel_chunk(self, chunk_range: ChunkRange) -> bool:

        if chunk_range in self._chunks_tasks:
            self._chunks_stats[chunk_range].update()
            if self._chunks_stats[chunk_range].status == EDownloadStatus.DOWNLOADING:
                self._chunks_tasks[chunk_range].cancel()
                try:
                    await self._chunks_tasks[chunk_range]
                except asyncio.CancelledError:
                    self._logger.info("Chunk task %s cancelled.", chunk_range)
                del self._chunks_tasks[chunk_range]
                return True

            self._logger.info(
                "Chunk %s not cancelled because its status is %s",
                chunk_range,
                self._chunks_stats[chunk_range].status,
            )
            return False

    def get_active_chunks(self) -> list[ChunkRange]:
        return list(self._chunks_tasks.keys())

    def get_chunk_stats(self, chunk_range: ChunkRange) -> None | ChunkDownloadStats:
        return self._chunks_stats.get(chunk_range)

    def get_all_chunk_stats(self) -> dict[ChunkRange, ChunkDownloadStats]:
        return self._chunks_stats.copy()

    def set_speed_limit(
        self, speed_bps: float, chunk_range: ChunkRange | None = None
    ) -> None:
        if chunk_range:
            stats = self._chunks_stats.get(chunk_range)
            if stats:
                stats.target_speed_bps = speed_bps
            else:
                self._logger.warning("No chunk stats found for key: %s", chunk_range)
        else:
            for stats in self._chunks_stats.values():
                stats.target_speed_bps = speed_bps

    def get_downloaded_bytes(self) -> int:
        # only not error/cancelled chunks
        return sum(
            s.bytes_downloaded
            for s in self._chunks_stats.values()
            if s.status not in (EDownloadStatus.ERROR, EDownloadStatus.CANCELLED)
        )

    async def cleanup_temp_files(self) -> None:
        self._logger.info("Cleaning up temp files")
        files_to_delete = [s.chunk_file_name for s in self._chunks_stats.values()]
        files_names_in_storage = {s.key for s in await self._storage.list_data()}
        files_to_delete_in_storage = [
            s for s in files_to_delete if s in files_names_in_storage
        ]
        self._logger.info("Files to delete: %s", files_to_delete_in_storage)
        delete_tasks = [
            self._storage.delete_data(s) for s in files_to_delete_in_storage
        ]
        await asyncio.gather(*delete_tasks)

    async def cancel_all_chunks(self) -> None:
        for chunk_range, task in list(self._chunks_tasks.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                self._logger.info("Chunk task %s cancelled.", chunk_range)
        self._chunks_tasks.clear()

    async def wait_for_completed_chunks(self, timeout: float = 2.0) -> list[ChunkRange]:
        if not self._chunks_tasks:
            return []

        done, _ = await asyncio.wait(
            self._chunks_tasks.values(),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        completed: list[ChunkRange] = []
        to_remove = [
            (chunk_range, task)
            for chunk_range, task in self._chunks_tasks.items()
            if task in done
        ]

        for chunk_range, task in to_remove:
            try:
                task.result()
                completed.append(chunk_range)
            except Exception as e:
                self._logger.warning(
                    "%s: Chunk %s failed: %s",
                    self._cfg.file_name,
                    chunk_range,
                    e,
                    exc_info=True,
                )
            finally:
                self._chunks_tasks.pop(chunk_range, None)

        return completed
