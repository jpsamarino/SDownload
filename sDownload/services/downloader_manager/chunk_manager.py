import asyncio
import logging
import time
from collections.abc import AsyncIterable, Mapping
from types import MappingProxyType
from typing import Literal
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
        self._wait_lock = asyncio.Lock()
        self._monitor_task: asyncio.Task | None = None

    async def _download_chunk(self, chunk_range: ChunkRange) -> ChunkRange | None:

        name = f"{chunk_range}_{self._cfg.file_name}.sdownload"
        end_byte = (
            chunk_range.end
            if chunk_range.end is not None
            else (self._cfg.file_size - 1) if self._cfg.file_size is not None else None
        )
        file_size = end_byte - chunk_range.start + 1 if end_byte is not None else None

        stats = ChunkDownloadStats(
            chunk_file_name=name,
            range=chunk_range,
            file_size=file_size,
            target_speed_bps=self._cfg.max_speed_bytes_per_second * 0.1,
        )
        self._chunks_stats[chunk_range] = stats

        self._logger.info("_download_chunk")
        stats.set_status(EDownloadStatus.DOWNLOADING)

        try:
            self._logger.info(
                "byte range [%s]-[%s]", chunk_range.start, chunk_range.end or "EOF"
            )
            raw_it = self._downloader.download_chunk(
                self._cfg.download_url, chunk_range.start, end_byte
            )
            tracked = throttle_and_track_async_stream(raw_it, stats)
            await self._storage.save_binary_data(name, tracked)
            if file_size is not None and stats.bytes_downloaded != file_size:
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
            raise

        finally:
            stats.update()

        return chunk_range

    async def _monitor_loop(self, interval: float = 0.5) -> None:
        try:
            while self._chunks_tasks:
                total_speed = 0.0
                active_count = 0
                active_stats = [
                    s
                    for s in self._chunks_stats.values()
                    if s.status == EDownloadStatus.DOWNLOADING
                ]
                for stats in active_stats:
                    stats.update()
                    total_speed += stats.speed_bps
                    active_count += 1

                if active_count > 0:
                    self._logger.info(
                        "(%s) SPEED: %.2f MB/s | Active Chunks: %d",
                        self._cfg.file_name,
                        total_speed / (1024 * 1024),
                        active_count,
                    )

                    if self._logger.isEnabledFor(logging.DEBUG):
                        for stats in active_stats:
                            self._logger.debug(
                                " └──▶ Chunk [%d-%d] %.1f%% @ %.2f MB/s",
                                stats.start_byte,
                                stats.end_byte,
                                stats.progress,
                                stats.speed_bps / (1024 * 1024),
                            )
                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            pass
        finally:
            self._monitor_task = None

    def _check_stop_monitor(self) -> None:
        if (
            not self._chunks_tasks
            and self._monitor_task
            and not self._monitor_task.done()
        ):
            self._monitor_task.cancel()

    def start_chunk(self, chunk_range: ChunkRange) -> None:
        if chunk_range not in self._chunks_tasks:
            self._chunks_tasks[chunk_range] = asyncio.create_task(
                self._download_chunk(chunk_range),
            )

        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_loop())

    def resize_chunk(self, current_range: ChunkRange, new_range: ChunkRange) -> None:
        # used to finish a chunk in specific range and create a new chunk with the progress of the old chunk
        ...

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

    def get_all_chunk_stats(self) -> Mapping[ChunkRange, ChunkDownloadStats]:
        return MappingProxyType(self._chunks_stats)

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
        self._check_stop_monitor()

    async def _wait_for_chunks(
        self,
        timeout: float | None,
        return_when: Literal[
            asyncio.FIRST_COMPLETED,
            asyncio.ALL_COMPLETED,
        ],
    ) -> list[ChunkDownloadStats]:
        async with self._wait_lock:
            if not self._chunks_tasks:
                return []

            done, _ = await asyncio.wait(
                self._chunks_tasks.values(),
                timeout=timeout,
                return_when=return_when,
            )

            completed: list[ChunkDownloadStats] = []

            for chunk_range, task in self._chunks_tasks.items():
                if task in done:
                    completed.append(self._chunks_stats[chunk_range])
                    try:
                        _ = task.result()
                    except Exception as e:
                        self._logger.warning(
                            "%s: Chunk %s failed: %s",
                            self._cfg.file_name,
                            chunk_range,
                            e,
                            exc_info=True,
                        )

            for stats in completed:
                self._chunks_tasks.pop(stats.range, None)

            self._check_stop_monitor()
            return completed

    async def wait_for_completed_chunks(
        self, timeout: float | None = None
    ) -> list[ChunkDownloadStats]:
        return await self._wait_for_chunks(
            timeout=timeout, return_when=asyncio.ALL_COMPLETED
        )

    async def wait_for_first_completed_chunk(
        self, timeout: float | None = None
    ) -> list[ChunkDownloadStats]:
        return await self._wait_for_chunks(
            timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )

    async def as_stream(self) -> AsyncIterable[ChunkDownloadStats]:
        while self._chunks_tasks:
            completed_batch = await self.wait_for_first_completed_chunk()
            for item in completed_batch:
                yield item
