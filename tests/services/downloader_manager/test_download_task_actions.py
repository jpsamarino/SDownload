import asyncio
from collections.abc import AsyncIterable
from datetime import UTC, datetime

import pytest

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
    ResourceInfo,
    StoredFileInfo,
    StrategyAction,
)
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.interfaces.protocols import (
    DownloaderProtocol,
    DownloadStrategyProtocol,
    FileRangeParams,
    FileStorageProtocol,
    RecoveryProtocol,
)
from sDownload.services.downloader_manager.download_task import DownloadTask


class MockDownloader(DownloaderProtocol):
    def __init__(self, resource_info: ResourceInfo, chunk_delay: float = 0.05):
        self._info = resource_info
        self._chunk_delay = chunk_delay

    async def get_file_info(self, url: str) -> ResourceInfo:
        return self._info

    async def download_chunk(
        self, url: str, start_byte: int = 0, end_byte: int | None = None
    ) -> AsyncIterable[bytes]:
        chunk_len = (
            (end_byte - start_byte + 1)
            if end_byte is not None
            else (self._info.file_size - start_byte if self._info.file_size else 1024)
        )
        half = chunk_len // 2
        yield b"X" * half
        if self._chunk_delay > 0:
            await asyncio.sleep(self._chunk_delay)
        yield b"Y" * (chunk_len - half)


class MockStorage(FileStorageProtocol):
    def __init__(self):
        self.files: dict[str, bytes] = {}

    async def get_data_info(self, key: str) -> StoredFileInfo | None:
        if key in self.files:
            return StoredFileInfo(
                key=key, size_bytes=len(self.files[key]), created_at=datetime.now(UTC)
            )
        return None

    async def save_binary_data(self, key: str, data: AsyncIterable[bytes]):
        buf = bytearray()
        async for chunk in data:
            buf.extend(chunk)
        self.files[key] = bytes(buf)

    async def merge_binary_files(self, source_keys: list[str], dest_key: str):
        buf = bytearray()
        for k in source_keys:
            buf.extend(self.files.get(k, b""))
        self.files[dest_key] = bytes(buf)

    async def merge_ranges(self, configs: list[FileRangeParams], dest_key: str):
        buf = bytearray()
        for cfg in configs:
            content = self.files.get(cfg.key, b"")
            start = cfg.start_byte or 0
            end = cfg.end_byte + 1 if cfg.end_byte is not None else len(content)
            buf.extend(content[start:end])
        self.files[dest_key] = bytes(buf)

    async def delete_data(self, key: str):
        self.files.pop(key, None)

    async def shrink_file_to(self, key: str, target_size_bytes: int):
        if key in self.files:
            self.files[key] = self.files[key][:target_size_bytes]

    async def move_data(self, src_key: str, dest_key: str):
        if src_key in self.files:
            self.files[dest_key] = self.files.pop(src_key)

    async def crop_file(self, key: str, start_byte: int, end_byte: int):
        if key in self.files:
            self.files[key] = self.files[key][start_byte : end_byte + 1]

    async def list_data(self) -> list[StoredFileInfo]:
        return [
            StoredFileInfo(key=k, size_bytes=len(v), created_at=datetime.now(UTC))
            for k, v in self.files.items()
        ]

    def get_binary_data(self, key: str) -> AsyncIterable[bytes]:
        async def stream():
            if key in self.files:
                yield self.files[key]

        return stream()


class MockRecovery(RecoveryProtocol):
    async def get_info(self, file_id: str) -> list[ChunkDownloadStats]:
        return []

    async def save_info(self, file_id: str, chunks_stats: list[ChunkDownloadStats]) -> None:
        pass

    async def delete_info(self, file_id: str) -> None:
        pass


def make_task_setup(strategy: DownloadStrategyProtocol, file_size: int = 1000):
    res_info = ResourceInfo(
        file_name="file.bin",
        file_dir=None,
        file_size=file_size,
        file_id="etag-123",
        download_url="https://example.com/file.bin",
        transmission_protocol="http",
        server_accept_ranges=True,
        file_created_at=datetime.now(UTC),
        protocol_data=None,
    )
    downloader = MockDownloader(res_info, chunk_delay=0.5)
    storage = MockStorage()
    recovery = MockRecovery()
    params = DownloadTaskParams(
        url="https://example.com/file.bin",
        dest_dir="/tmp",
        file_name="file.bin",
        max_conn=4,
        use_chunked=True,
    )
    task = DownloadTask(
        params,
        downloader=downloader,
        storage=storage,
        strategy=strategy,
        recovery=recovery,
    )
    return task, downloader, storage


@pytest.mark.asyncio
async def test_execute_start_action_with_speed_limit():
    """Validates that Start action starts the chunk and applies target_speed_bps."""

    class StartWithSpeedStrategy(DownloadStrategyProtocol):
        def on_start(self, dl_stats, chunks_stats, available_slots):
            return [
                StrategyAction.Start(
                    range=ChunkRange(0, 499),
                    target_speed_bps=50_000,
                )
            ]

        def on_update(self, dl_stats, chunks_stats, available_slots):
            return []

        def on_end(self, dl_stats, chunks_stats):
            pass

    task, _, _ = make_task_setup(StartWithSpeedStrategy())
    await task.start()
    await asyncio.sleep(0.05)

    stats = task._chunk_manager.stats.get(ChunkRange(0, 499))
    assert stats is not None
    assert stats.target_speed_bps == 50_000

    await task.cancel()


@pytest.mark.asyncio
async def test_execute_set_speed_action():
    """Validates that SetSpeed action adjusts target_speed_bps on the chunk."""

    class SetSpeedStrategy(DownloadStrategyProtocol):
        def __init__(self):
            self._updated = False

        def on_start(self, dl_stats, chunks_stats, available_slots):
            return [StrategyAction.Start(range=ChunkRange(0, 499))]

        def on_update(self, dl_stats, chunks_stats, available_slots):
            if not self._updated:
                self._updated = True
                return [
                    StrategyAction.SetSpeed(
                        range=ChunkRange(0, 499),
                        target_speed_bps=99_999,
                    )
                ]
            return []

        def on_end(self, dl_stats, chunks_stats):
            pass

    task, _, _ = make_task_setup(SetSpeedStrategy())
    await task.start()
    await asyncio.sleep(0.25)

    stats = task._chunk_manager.stats.get(ChunkRange(0, 499))
    assert stats is not None
    assert stats.target_speed_bps == 99_999

    await task.cancel()


@pytest.mark.asyncio
async def test_execute_cancel_action():
    """Validates that Cancel action cancels the specified chunk task cleanly."""

    class CancelStrategy(DownloadStrategyProtocol):
        def __init__(self):
            self._cancelled = False

        def on_start(self, dl_stats, chunks_stats, available_slots):
            return [
                StrategyAction.Start(range=ChunkRange(0, 499)),
                StrategyAction.Start(range=ChunkRange(500, 999)),
            ]

        def on_update(self, dl_stats, chunks_stats, available_slots):
            if not self._cancelled:
                self._cancelled = True
                return [StrategyAction.Cancel(range=ChunkRange(500, 999))]
            return []

        def on_end(self, dl_stats, chunks_stats):
            pass

    task, _, _ = make_task_setup(CancelStrategy())
    await task.start()
    await asyncio.sleep(0.25)

    # Chunk [500, 999] was cancelled, so its active task context was removed
    assert ChunkRange(500, 999) not in task._chunk_manager._chunks_tasks

    await task.cancel()


@pytest.mark.asyncio
async def test_execute_resize_action():
    """Validates that Resize action triggers chunk succession with speed limit without AttributeError."""

    class ResizeStrategy(DownloadStrategyProtocol):
        def __init__(self):
            self._resized = False

        def on_start(self, dl_stats, chunks_stats, available_slots):
            return [StrategyAction.Start(range=ChunkRange(0, 999))]

        def on_update(self, dl_stats, chunks_stats, available_slots):
            if not self._resized:
                self._resized = True
                return [
                    StrategyAction.Resize(
                        current_range=ChunkRange(0, 999),
                        new_range=ChunkRange(0, 499),
                        target_speed_bps=30_000,
                    )
                ]
            return []

        def on_end(self, dl_stats, chunks_stats):
            pass

    task, _, _ = make_task_setup(ResizeStrategy())
    await task.start()
    await asyncio.sleep(0.25)

    # Successor chunk [0, 499] was registered
    stats_successor = task._chunk_manager.stats.get(ChunkRange(0, 499))
    assert stats_successor is not None
    assert stats_successor.target_speed_bps == 30_000

    await task.cancel()


@pytest.mark.asyncio
async def test_qt_active_chunks_and_available_slots():
    """Validates that ChunkManager.qt_active_chunks accurately reflects in-flight tasks and feeds DownloadTask available_slots."""
    observed_slots: list[int] = []

    class SlotObserverStrategy(DownloadStrategyProtocol):
        def on_start(self, dl_stats, chunks_stats, available_slots):
            # Start 2 chunks out of max_conn=4
            return [
                StrategyAction.Start(range=ChunkRange(0, 499)),
                StrategyAction.Start(range=ChunkRange(500, 999)),
            ]

        def on_update(self, dl_stats, chunks_stats, available_slots):
            observed_slots.append(available_slots)
            return []

        def on_end(self, dl_stats, chunks_stats):
            pass

    task, _, _ = make_task_setup(SlotObserverStrategy())
    await task.start()
    await asyncio.sleep(0.25)

    # ChunkManager must report 2 active chunk tasks in O(1)
    assert task._chunk_manager.qt_active_chunks == 2

    # Since max_conn=4 and 2 chunks are active, available_slots passed to strategy must be 2
    assert len(observed_slots) > 0
    assert 2 in observed_slots

    await task.cancel()
