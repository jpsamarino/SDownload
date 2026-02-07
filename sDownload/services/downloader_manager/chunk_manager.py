import asyncio
import logging
import time
from collections.abc import AsyncIterable, Mapping
from types import MappingProxyType
from typing import Literal, NamedTuple
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.interfaces.protocols.downloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    EDownloadStatus,
)
from sDownload.services.downloader_manager.download_config import DownloadConfig
from sDownload.services.downloader_manager.throttle_and_track_async_stream import (
    throttle_and_track_async_stream,
)
from sDownload.utils.range_operations import calculate_optimal_coverage
from sDownload.interfaces.protocols.file_storage_protocol import (
    FileStorageProtocol,
    FileRangeConfig,
)


class ChunkTaskContext(NamedTuple):
    task: asyncio.Task
    init_signal: asyncio.Event


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
        self._chunks_tasks: dict[ChunkRange, ChunkTaskContext] = {}
        self._wait_lock = asyncio.Lock()
        self._monitor_task: asyncio.Task | None = None

    def _register_chunk_stats(
        self,
        chunk_range: ChunkRange,
        target_speed_bps: float | None = None,
        status: EDownloadStatus = EDownloadStatus.PENDING,
    ) -> ChunkDownloadStats:

        effective_end = chunk_range.end
        if effective_end is None and self._cfg.file_size is not None:
            effective_end = self._cfg.file_size - 1

        file_size = (
            (effective_end - chunk_range.start + 1)
            if effective_end is not None
            else None
        )
        name = f"{chunk_range}_{self._cfg.file_name}.sdownload"
        _target_speed_bps = (
            target_speed_bps or self._cfg.max_speed_bytes_per_second * 0.1
        )

        stats = ChunkDownloadStats(
            chunk_file_name=name,
            range=chunk_range,
            file_size=file_size,
            status=status,
            target_speed_bps=_target_speed_bps,
        )

        self._chunks_stats[chunk_range] = stats

        return stats

    async def _download_chunk(self, chunk_range: ChunkRange) -> ChunkRange | None:

        task_context = self._chunks_tasks[chunk_range]
        stats = self._chunks_stats[chunk_range]

        stats.set_status(EDownloadStatus.DOWNLOADING)
        task_context.init_signal.set()
        try:
            self._logger.info(
                "byte range [%s]-[%s]", chunk_range.start, chunk_range.end or "EOF"
            )
            raw_it = self._downloader.download_chunk(
                self._cfg.download_url, stats.range.start, stats.range.end
            )
            tracked = throttle_and_track_async_stream(raw_it, stats)
            await self._storage.save_binary_data(stats.chunk_file_name, tracked)
            if (
                stats.file_size is not None
                and stats.bytes_downloaded != stats.file_size
            ):
                raise IOError(
                    f"Chunk size error: expected {stats.file_size} bytes, got {stats.bytes_downloaded} bytes"
                )
            stats.set_status(EDownloadStatus.COMPLETED)
            self._logger.info("[%s] ending", stats.chunk_file_name)

        except asyncio.CancelledError:
            # If the cancellation happened because we reached the useful limit
            if stats.limit_qt_bytes and stats.bytes_downloaded >= stats.limit_qt_bytes:
                stats.set_status(EDownloadStatus.DEPRECATED)
                self._logger.info(
                    "[%s] goal reached, marked as DEPRECATED.", stats.chunk_file_name
                )
            else:
                stats.set_status(EDownloadStatus.CANCELLED)
                self._logger.warning("[%s] download cancelled", stats.chunk_file_name)
            raise

        except Exception as e:
            stats.set_status(EDownloadStatus.ERROR)
            self._logger.warning("[%s] failed: %s", stats.chunk_file_name, e)
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
                                stats.range.start,
                                stats.range.end,
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

    def start_chunk(
        self, chunk_range: ChunkRange, target_speed_bps: float | None = None
    ) -> None:
        stats = self._chunks_stats.get(chunk_range)
        if stats and stats.status == EDownloadStatus.COMPLETED:
            self._logger.info("Chunk %s already completed", chunk_range)
            return
        if chunk_range not in self._chunks_tasks:
            self._register_chunk_stats(chunk_range, target_speed_bps)
            self._chunks_tasks[chunk_range] = ChunkTaskContext(
                task=asyncio.create_task(self._download_chunk(chunk_range)),
                init_signal=asyncio.Event(),
            )
            if self._monitor_task is None or self._monitor_task.done():
                self._monitor_task = asyncio.create_task(self._monitor_loop())
        else:
            self._logger.info("Already have a task for chunk %s", chunk_range)

    def resize_chunk(self, current_range: ChunkRange, new_range: ChunkRange) -> None:
        if new_range not in current_range:
            raise ValueError(
                f"New range {new_range} must be contained within {current_range}"
            )

        if current_range not in self._chunks_stats:
            if current_range not in self._chunks_tasks:
                raise KeyError(f"Range {current_range} not found in active chunks")
            raise ValueError(
                f"Is necessary wait for range {current_range} init to resize"
            )

        if new_range == current_range:
            return

        stats_a = self._chunks_stats[current_range]

        if stats_a.status not in (
            EDownloadStatus.PENDING,
            EDownloadStatus.DOWNLOADING,
            EDownloadStatus.COMPLETED,
        ):
            raise ValueError(
                f"Range {current_range} is not in DOWNLOADING or COMPLETED state"
            )

        self._register_chunk_stats(
            chunk_range=new_range,
            target_speed_bps=stats_a.target_speed_bps,
            status=EDownloadStatus.AWAITING_SUCCESSION,
        )
        limit = (
            new_range.end - current_range.start + 1
            if new_range.end is not None
            else None
        )

        if limit:

            def stop_predecessor():
                task_a = self._chunks_tasks.get(current_range)
                task_a_stats = self._chunks_stats.get(current_range)
                if (
                    task_a.task
                    and not task_a.task.done()
                    and task_a_stats.bytes_downloaded != task_a_stats.file_size
                ):
                    self._logger.info(
                        "Limit reached for %s. Triggering succession to %s.",
                        current_range,
                        new_range,
                    )
                    task_a.task.cancel()
                elif task_a_stats.bytes_downloaded == task_a_stats.file_size:
                    self._logger.info(
                        "Task %s finished after limit. It will be succession to %s.",
                        current_range,
                        new_range,
                    )

            if stats_a.status in (
                EDownloadStatus.PENDING,
                EDownloadStatus.DOWNLOADING,
            ):
                stats_a.add_limit_observer(limit, stop_predecessor)

        self._chunks_tasks[new_range] = ChunkTaskContext(
            task=asyncio.create_task(self._run_succession(current_range, new_range)),
            init_signal=asyncio.Event(),
        )

    async def _run_succession(
        self, range_predecessor: ChunkRange, range_successor: ChunkRange
    ) -> ChunkRange:

        stats_predecessor = self._chunks_stats[range_predecessor]
        stats_successor = self._chunks_stats[range_successor]
        ctx_predecessor = self._chunks_tasks.get(range_predecessor)
        ctx_successor = self._chunks_tasks[range_successor]

        ctx_successor.init_signal.set()

        predecessor_error: Exception | None = None

        try:
            if ctx_predecessor:
                # need use asyncio.wait to indentify self.cancelled() (_run_succession)
                await asyncio.wait(
                    [ctx_predecessor.task], return_when=asyncio.FIRST_COMPLETED
                )
                try:
                    await ctx_predecessor.task
                except asyncio.CancelledError:
                    self._logger.info(
                        "Predecessor task %s cancelled.", range_predecessor
                    )
                except Exception as e:
                    self._logger.warning(
                        "Predecessor task %s failed: %s", range_predecessor, e
                    )
                    predecessor_error = e

            limit = stats_predecessor.limit_qt_bytes

            if predecessor_error:
                stats_successor.set_status(EDownloadStatus.ERROR)
                raise RuntimeError(
                    f"Predecessor failed: {predecessor_error}"
                ) from predecessor_error

            if limit and stats_predecessor.bytes_downloaded < limit:
                stats_successor.set_status(EDownloadStatus.ERROR)
                raise RuntimeError(
                    f"Insufficient data: {stats_predecessor.bytes_downloaded}/{limit} bytes"
                )

            start_crop = range_successor.start - range_predecessor.start
            end_crop = (
                (range_successor.end - range_predecessor.start)
                if range_successor.end is not None
                else stats_predecessor.bytes_downloaded - 1
            )

            await self._storage.crop_file(
                stats_predecessor.chunk_file_name, start_crop, end_crop
            )
            await self._storage.move_data(
                stats_predecessor.chunk_file_name, stats_successor.chunk_file_name
            )

            stats_successor.set_status(EDownloadStatus.COMPLETED)
            stats_predecessor.set_status(EDownloadStatus.DEPRECATED)
            stats_successor.bytes_downloaded = end_crop - start_crop + 1
            self._logger.info(
                "Succession complete: %s is now COMPLETED.", range_successor
            )

            return range_successor

        except asyncio.CancelledError:
            self._logger.info("_run_succession cancelled in task %s.", range_successor)
            stats_successor.set_status(EDownloadStatus.CANCELLED)
            if ctx_predecessor and not ctx_predecessor.task.done():
                ctx_predecessor.task.cancel()
                await ctx_predecessor.task
            stats_predecessor.remove_limit_observer()
            raise

        except Exception:
            if stats_successor.status != EDownloadStatus.ERROR:
                stats_successor.set_status(EDownloadStatus.ERROR)
            raise

    async def cancel_chunk(self, chunk_range: ChunkRange) -> bool:

        if chunk_range in self._chunks_tasks:
            self._chunks_stats[chunk_range].update()
            if self._chunks_stats[chunk_range].status in (
                EDownloadStatus.DOWNLOADING,
                EDownloadStatus.PENDING,
                EDownloadStatus.AWAITING_SUCCESSION,
            ):
                task_context_to_cancel = self._chunks_tasks[chunk_range]

                try:
                    await task_context_to_cancel.init_signal.wait()
                    task_context_to_cancel.task.cancel()
                    await task_context_to_cancel.task
                except asyncio.CancelledError:
                    self._logger.info("Chunk task %s cancelled.", chunk_range)
                except Exception as e:
                    self._logger.warning("Chunk task %s failed: %s", chunk_range, e)
                del self._chunks_tasks[chunk_range]
                return True

            self._logger.info(
                "Chunk %s not cancelled because its status is %s",
                chunk_range,
                self._chunks_stats[chunk_range].status,
            )

        return False

    async def delete_chunk_data(self, chunk_range: ChunkRange) -> None:
        if chunk_range in self._chunks_tasks:
            self._logger.info(
                "Cancelling active task for chunk %s before removal.", chunk_range
            )
            await self.cancel_chunk(chunk_range)

        stats = self._chunks_stats.pop(chunk_range, None)

        if stats:
            self._logger.info(
                "Removing chunk %s: deleting file %s",
                chunk_range,
                stats.chunk_file_name,
            )
            try:
                await self._storage.delete_data(stats.chunk_file_name)
            except Exception as e:
                self._logger.warning(
                    "Failed to delete file %s for chunk %s: %s",
                    stats.chunk_file_name,
                    chunk_range,
                    e,
                )
        else:
            self._logger.warning("No stats found for chunk %s to remove.", chunk_range)

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
            if s.status in (EDownloadStatus.COMPLETED, EDownloadStatus.DOWNLOADING)
        )

    async def cleanup(self) -> None:
        """
        Stops all active processes, deletes temporary files, and clears internal state.
        """
        self._logger.info("Performing comprehensive cleanup of ChunkManager")

        # 1. Stop all active chunks and the monitor
        await self.cancel_all_chunks()

        # 2. Delete temporary files from disk
        await self._cleanup_temp_files()

        # 3. Clear internal statistics
        self._chunks_stats.clear()
        self._logger.info(
            "Cleanup complete: all tasks stopped, files deleted, and state cleared."
        )

    async def _cleanup_temp_files(self) -> None:
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
        if delete_tasks:
            await asyncio.gather(*delete_tasks)

    async def cancel_all_chunks(self) -> None:
        for task in self._chunks_tasks.values():
            task.task.cancel()

        if self._chunks_tasks:
            all_tasks = [context.task for context in self._chunks_tasks.values()]
            await asyncio.gather(*all_tasks, return_exceptions=True)

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

            all_tasks = [context.task for context in self._chunks_tasks.values()]
            done, _ = await asyncio.wait(
                all_tasks,
                timeout=timeout,
                return_when=return_when,
            )

            completed: list[ChunkDownloadStats] = []

            for chunk_range, task_context in self._chunks_tasks.items():
                if task_context.task in done:
                    completed.append(self._chunks_stats[chunk_range])
                    try:
                        _ = task_context.task.result()
                    except asyncio.CancelledError:
                        self._logger.info("Chunk %s cancelled by asyncio", chunk_range)
                    except Exception as e:
                        self._logger.warning(
                            "%s: Chunk %s failed: %s",
                            self._cfg.file_name,
                            chunk_range,
                            e,
                            exc_info=False,
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

    async def merge_chunks(self, cleanup: bool = True) -> str:
        """
        Calculates the optimal coverage and merges all completed chunks into the final file.

        Args:
            cleanup: Whether to delete the temporary chunk files after merging.

        Returns:
            The key of the merged file.
        """
        completed_stats = [
            s
            for s in self._chunks_stats.values()
            if s.status == EDownloadStatus.COMPLETED
        ]

        if not completed_stats:
            raise RuntimeError("No completed chunks to merge.")

        ranges = [s.range for s in completed_stats]
        fragments = calculate_optimal_coverage(ranges, file_size=self._cfg.file_size)

        merge_configs = []
        for frag in fragments:
            stats = self._chunks_stats[frag.range]
            merge_configs.append(
                FileRangeConfig(
                    key=stats.chunk_file_name,
                    start_byte=0,
                    end_byte=(
                        (frag.read_limit_qt_bytes - 1)
                        if frag.read_limit_qt_bytes
                        else None
                    ),
                )
            )

        dest_key = self._cfg.file_name
        self._logger.info("Merging %d fragments into %s", len(merge_configs), dest_key)
        await self._storage.merge_ranges(merge_configs, dest_key)

        if cleanup:
            self._logger.info("Cleaning up ChunkManager after merge...")
            await self.cleanup()

        return dest_key
