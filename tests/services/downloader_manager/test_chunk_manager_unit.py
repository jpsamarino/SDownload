import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock
import pytest
from sDownload.services.downloader_manager.chunk_manager import (
    ChunkManager,
    ChunkRange,
    DownloadConfig,
    EDownloadStatus,
)
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
)


class MockStorage:
    async def save_binary_data(self, name, data_stream):
        async for _ in data_stream:
            pass

    async def list_data(self):
        return []

    async def delete_data(self, name):
        pass


class MockDownloader:
    def __init__(self):
        self.download_chunk = AsyncMock()

    pass


async def mock_download_generator(size, speed_limit=None):
    chunk_size = 1024
    generated = 0
    while generated < size:
        yield b"x" * min(chunk_size, size - generated)
        generated += chunk_size
        await asyncio.sleep(0.01)  # Simulate network latency


@pytest.fixture
def mock_downloader():
    downloader = MagicMock()
    downloader.download_chunk.side_effect = (
        lambda url, start, end: mock_download_generator(end - start + 1)
    )
    return downloader


@pytest.fixture
def mock_storage():
    return MockStorage()


@pytest.fixture
def download_config():
    return DownloadConfig(
        file_name="test_file",
        file_dir="/tmp",
        file_size=10000,
        file_id="123",
        download_url="http://test.com",
        file_created_at=None,
        protocol_data=None,
        max_connections_per_download=1,
        max_speed_bytes_per_second=100000,
    )


@pytest.mark.asyncio
async def test_chunk_manager_monitor_loop(
    download_config, mock_downloader, mock_storage
):
    logger = logging.getLogger("test_chunk_manager")
    logger.setLevel(logging.DEBUG)

    manager = ChunkManager(download_config, mock_downloader, mock_storage, logger)

    chunk = ChunkRange(0, 4999)
    manager.start_chunk(chunk)

    assert manager._monitor_task is not None
    assert not manager._monitor_task.done()

    completed = await manager.wait_for_completed_chunks()

    assert len(completed) == 1
    assert completed[0].range == chunk
    assert completed[0].status == EDownloadStatus.COMPLETED

    await asyncio.sleep(1.1)
    task = manager._monitor_task
    if task:
        assert task.done() or manager._monitor_task is None


@pytest.mark.asyncio
async def test_cancel_all_chunks_cleans_up(
    download_config, mock_downloader, mock_storage
):
    logger = logging.getLogger("test_chunk_manager")
    manager = ChunkManager(download_config, mock_downloader, mock_storage, logger)

    manager.start_chunk(ChunkRange(0, 4999))
    manager.start_chunk(ChunkRange(5000, 9999))

    await asyncio.sleep(0.05)

    await manager.cancel_all_chunks()

    assert len(manager.get_active_chunks()) == 0
    await asyncio.sleep(1.1)
    if manager._monitor_task:
        assert manager._monitor_task.done()


@pytest.mark.asyncio
async def test_wait_for_first_completed_chunk(
    download_config, mock_downloader, mock_storage
):
    logger = logging.getLogger("test_concurrency")
    manager = ChunkManager(download_config, mock_downloader, mock_storage, logger)

    # Let's mock _download_chunk to sleep for a requested time
    async def _fake_download(chunk_range, duration):
        await asyncio.sleep(duration)
        # Mock updating stats
        stats = manager._chunks_stats[chunk_range]
        stats.bytes_downloaded = 10  # pretend we did it
        return chunk_range

    manager._download_chunk = _fake_download

    # ranges
    r1 = ChunkRange(0, 10)
    r2 = ChunkRange(11, 20)

    # Init stats manually since we are bypassing start_chunk logic slightly or need it
    from sDownload.services.downloader_manager.download_stats_models import (
        ChunkDownloadStats,
    )

    manager._chunks_stats[r1] = ChunkDownloadStats(
        "c1", r1, 10, 11, EDownloadStatus.PENDING
    )
    manager._chunks_stats[r2] = ChunkDownloadStats(
        "c2", r2, 10, 11, EDownloadStatus.PENDING
    )

    # Add tasks manually to avoid complex mocking of start_chunk calling _download_chunk
    from sDownload.services.downloader_manager.chunk_manager import ChunkTaskContext

    t1 = asyncio.create_task(_fake_download(r1, 0.1))  # Fast
    t2 = asyncio.create_task(_fake_download(r2, 0.5))  # Slow

    manager._chunks_tasks[r1] = ChunkTaskContext(task=t1, init_signal=asyncio.Event())
    manager._chunks_tasks[r2] = ChunkTaskContext(task=t2, init_signal=asyncio.Event())

    # Act: Wait for first
    start_time = asyncio.get_running_loop().time()
    results = await manager.wait_for_first_completed_chunk(timeout=1.0)
    end_time = asyncio.get_running_loop().time()

    # Assert
    assert len(results) == 1
    assert results[0].range == r1
    assert (end_time - start_time) < 0.4  # Should return fast, not wait for t2

    # cleanup t2
    await t2


@pytest.mark.asyncio
async def test_lock_mechanism(download_config, mock_downloader, mock_storage):
    logger = logging.getLogger("test_concurrency")
    manager = ChunkManager(download_config, mock_downloader, mock_storage, logger)

    # Test that we cannot run two wait operations concurrently
    # We will start a LONG wait, and try to start another one immediately.

    async def _long_task():
        await asyncio.sleep(0.5)
        return

    from sDownload.services.downloader_manager.chunk_manager import ChunkTaskContext

    r1 = ChunkRange(0, 10)
    manager._chunks_tasks[r1] = ChunkTaskContext(
        task=asyncio.create_task(_long_task()), init_signal=asyncio.Event()
    )

    # Start a wait that will take 0.5s (or timeout 0.5s)
    task1 = asyncio.create_task(manager.wait_for_completed_chunks(timeout=0.2))

    # Give it a tiny moment to acquire lock
    await asyncio.sleep(0.01)

    # This should block until task1 releases lock
    start_time = asyncio.get_running_loop().time()
    await manager.wait_for_completed_chunks(timeout=0.01)  # fast timeout
    end_time = asyncio.get_running_loop().time()

    # Verify that it waited for task1 roughly (0.2s)
    # Note: wait_for_completed_chunks acquires lock, waits for tasks (or timeout).
    # task1 waits 0.2s.
    # task2 tries to acquire lock. It must wait until task1 is done.

    assert (end_time - start_time) >= 0.15  # Approx 0.2s minus slop

    await task1
