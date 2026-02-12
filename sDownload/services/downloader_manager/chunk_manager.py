import asyncio
import logging
from collections.abc import AsyncIterable, Mapping
from types import MappingProxyType
from typing import Literal, NamedTuple
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.interfaces.protocols.downloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_storage_protocol import (
    FileStorageProtocol,
    FileRangeConfig,
)
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    EDownloadStatus,
)
from sDownload.services.downloader_manager.download_config import DownloadConfig
from sDownload.services.downloader_manager.throttle_and_track_async_stream import (
    throttle_and_track_async_stream,
)
from sDownload.services.downloader_manager.chunk_utils import (
    monitor_download_progress,
    run_chunk_succession,
    cleanup_temp_files,
)
from sDownload.utils.range_operations import calculate_optimal_coverage

logger = logging.getLogger(__name__)


class ChunkTaskContext(NamedTuple):
    task: asyncio.Task
    init_signal: asyncio.Event


class ChunkManager:
    def __init__(
        self,
        cfg: DownloadConfig,
        downloader: DownloaderProtocol,
        storage: FileStorageProtocol,
    ):
        self._cfg = cfg
        self._downloader = downloader
        self._storage = storage
        self._chunks_stats: dict[ChunkRange, ChunkDownloadStats] = {}
        self._chunks_tasks: dict[ChunkRange, ChunkTaskContext] = {}
        self._wait_lock = asyncio.Lock()
        self._monitor_task: asyncio.Task | None = None

    @property
    def stats(self) -> Mapping[ChunkRange, ChunkDownloadStats]:
        """
        Returns a read-only view of all chunk stats.
        """
        return MappingProxyType(self._chunks_stats)

    def _register_chunk_stats(
        self,
        chunk_range: ChunkRange,
        target_speed_bps: int | None = None,
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

        stats = ChunkDownloadStats(
            chunk_file_name=name,
            range=chunk_range,
            file_size=file_size,
            status=status,
            target_speed_bps=target_speed_bps,
        )

        self._chunks_stats[chunk_range] = stats

        return stats

    async def _download_chunk(self, chunk_range: ChunkRange) -> ChunkRange | None:

        task_context = self._chunks_tasks[chunk_range]
        stats = self._chunks_stats[chunk_range]

        stats.set_status(EDownloadStatus.DOWNLOADING)
        task_context.init_signal.set()
        try:
            logger.info(
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
            logger.info("[%s] ending", stats.chunk_file_name)

        except asyncio.CancelledError:
            # If the cancellation happened because we reached the useful limit
            if stats.limit_qt_bytes and stats.bytes_downloaded >= stats.limit_qt_bytes:
                stats.set_status(EDownloadStatus.DEPRECATED)
                logger.info(
                    "[%s] goal reached, marked as DEPRECATED.", stats.chunk_file_name
                )
            else:
                stats.set_status(EDownloadStatus.CANCELLED)
                logger.warning("[%s] download cancelled", stats.chunk_file_name)
            raise

        except Exception as e:
            stats.set_status(EDownloadStatus.ERROR)
            logger.warning("[%s] failed: %s", stats.chunk_file_name, e)
            raise

        finally:
            stats.update()

        return chunk_range

    def _check_stop_monitor(self) -> None:
        if not self._chunks_tasks and self._monitor_task:
            if not self._monitor_task.done():
                self._monitor_task.cancel()
            self._monitor_task = None

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
                        logger.info("Chunk %s cancelled by asyncio", chunk_range)
                    except Exception as e:
                        logger.warning(
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

    def start_chunk(
        self, chunk_range: ChunkRange, target_speed_bps: int | None = None
    ) -> None:
        stats = self._chunks_stats.get(chunk_range)
        if stats and stats.status == EDownloadStatus.COMPLETED:
            logger.info("Chunk %s already completed", chunk_range)
            return
        if chunk_range not in self._chunks_tasks:
            self._register_chunk_stats(chunk_range, target_speed_bps)
            self._chunks_tasks[chunk_range] = ChunkTaskContext(
                task=asyncio.create_task(self._download_chunk(chunk_range)),
                init_signal=asyncio.Event(),
            )
            if self._monitor_task is None or self._monitor_task.done():
                self._monitor_task = asyncio.create_task(
                    monitor_download_progress(self._chunks_stats, self._cfg.file_name)
                )
        else:
            logger.info("Already have a task for chunk %s", chunk_range)

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

        stats_b = self._register_chunk_stats(
            chunk_range=new_range,
            target_speed_bps=stats_a.target_speed_bps,
        )
        stats_b.set_status(EDownloadStatus.AWAITING_SUCCESSION)
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
                    logger.info(
                        "Limit reached for %s. Triggering succession to %s.",
                        current_range,
                        new_range,
                    )
                    task_a.task.cancel()
                elif task_a_stats.bytes_downloaded == task_a_stats.file_size:
                    logger.info(
                        "Task %s finished after limit. It will be succession to %s.",
                        current_range,
                        new_range,
                    )

            if stats_a.status in (
                EDownloadStatus.PENDING,
                EDownloadStatus.DOWNLOADING,
            ):
                stats_a.add_limit_observer(limit, stop_predecessor)

        ctx_predecessor = self._chunks_tasks.get(current_range)
        init_signal = asyncio.Event()

        self._chunks_tasks[new_range] = ChunkTaskContext(
            task=asyncio.create_task(
                run_chunk_succession(
                    storage=self._storage,
                    stats_predecessor=stats_a,
                    stats_successor=stats_b,
                    predecessor_task=ctx_predecessor.task if ctx_predecessor else None,
                    init_signal=init_signal,
                )
            ),
            init_signal=init_signal,
        )

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
                    logger.info("Chunk task %s cancelled.", chunk_range)
                except Exception as e:
                    logger.warning("Chunk task %s failed: %s", chunk_range, e)
                del self._chunks_tasks[chunk_range]
                return True

            logger.info(
                "Chunk %s not cancelled because its status is %s",
                chunk_range,
                self._chunks_stats[chunk_range].status,
            )

        return False

    async def delete_chunk_data(self, chunk_range: ChunkRange) -> None:
        if chunk_range in self._chunks_tasks:
            logger.info(
                "Cancelling active task for chunk %s before removal.", chunk_range
            )
            await self.cancel_chunk(chunk_range)

        stats = self._chunks_stats.pop(chunk_range, None)

        if stats:
            logger.info(
                "Removing chunk %s: deleting file %s",
                chunk_range,
                stats.chunk_file_name,
            )
            try:
                await self._storage.delete_data(stats.chunk_file_name)
            except Exception as e:
                logger.warning(
                    "Failed to delete file %s for chunk %s: %s",
                    stats.chunk_file_name,
                    chunk_range,
                    e,
                )
        else:
            logger.warning("No stats found for chunk %s to remove.", chunk_range)

    def set_speed_limit(
        self, speed_bps: float, chunk_range: ChunkRange | None = None
    ) -> None:
        if chunk_range:
            stats = self._chunks_stats.get(chunk_range)
            if stats:
                stats.target_speed_bps = speed_bps
            else:
                logger.warning("No chunk stats found for key: %s", chunk_range)
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
        logger.info("Performing comprehensive cleanup of ChunkManager")

        await self.cancel_all_chunks()
        await cleanup_temp_files(self._storage, self._chunks_stats.values())
        self._chunks_stats.clear()
        logger.info(
            "Cleanup complete: all tasks stopped, files deleted, and state cleared."
        )

    async def cancel_all_chunks(self) -> None:
        for chunk_range, task in self._chunks_tasks.items():
            task.task.cancel()
            # if task is not initialized, set status to cancelled, dont need wait start_signal
            stat = self._chunks_stats.get(chunk_range)
            if stat and stat.status in (
                EDownloadStatus.PENDING,
                EDownloadStatus.AWAITING_SUCCESSION,
            ):
                stat.status = EDownloadStatus.CANCELLED

        if self._chunks_tasks:
            all_tasks = [context.task for context in self._chunks_tasks.values()]
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._chunks_tasks.clear()
        self._check_stop_monitor()

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
        logger.info("Merging %d fragments into %s", len(merge_configs), dest_key)
        await self._storage.merge_ranges(merge_configs, dest_key)

        if cleanup:
            logger.info("Cleaning up ChunkManager after merge...")
            await self.cleanup()

        return dest_key
