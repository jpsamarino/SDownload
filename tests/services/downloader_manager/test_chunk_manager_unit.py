import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock
import pytest
from sDownload.services.downloader_manager.chunk_manager import (
    ChunkManager,
    ChunkRange,
    DownloadConfig,
    EDownloadStatus,
    ChunkTaskContext,
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
    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_set_speed_limit_global(download_config, mock_downloader, mock_storage):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(101, 200)

    manager.start_chunk(r1)
    manager.start_chunk(r2)

    # Default is config based, let's change it
    new_speed = 5000.0
    manager.set_speed_limit(new_speed)

    stats1 = manager.get_chunk_stats(r1)
    stats2 = manager.get_chunk_stats(r2)

    assert stats1.target_speed_bps == new_speed
    assert stats2.target_speed_bps == new_speed

    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_set_speed_limit_specific_chunk(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(101, 200)

    manager.start_chunk(r1)
    manager.start_chunk(r2)

    new_speed = 2000.0
    manager.set_speed_limit(new_speed, chunk_range=r1)

    stats1 = manager.get_chunk_stats(r1)
    stats2 = manager.get_chunk_stats(r2)

    assert stats1.target_speed_bps == new_speed
    assert stats2.target_speed_bps != new_speed  # Should remain unchanged

    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_set_speed_limit_invalid_chunk(
    download_config, mock_downloader, mock_storage, caplog
):
    logger = logging.getLogger("test_limit")
    manager = ChunkManager(download_config, mock_downloader, mock_storage, logger)

    # Just ensure it doesn't raise
    with caplog.at_level(logging.WARNING):
        manager.set_speed_limit(1000, chunk_range=ChunkRange(999, 9999))

    assert "No chunk stats found" in caplog.text
    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_start_chunk_idempotency_active(
    download_config, mock_downloader, mock_storage
):
    logger = logging.getLogger("test_idempotency")
    manager = ChunkManager(download_config, mock_downloader, mock_storage, logger)
    range_ = ChunkRange(0, 100)

    manager.start_chunk(range_)
    first_task = manager._chunks_tasks[range_]

    # Try starting again
    manager.start_chunk(range_)
    second_task = manager._chunks_tasks[range_]

    assert first_task is second_task  # Should be the same object
    assert len(manager._chunks_tasks) == 1
    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_start_chunk_idempotency_completed(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    range_ = ChunkRange(0, 100)

    # Manually mark as completed
    manager._chunks_stats[range_] = ChunkDownloadStats(
        chunk_file_name="test",
        range=range_,
        file_size=101,
        status=EDownloadStatus.COMPLETED,
    )

    manager.start_chunk(range_)

    assert range_ not in manager._chunks_tasks


@pytest.mark.asyncio
async def test_resize_same_range_noop(download_config, mock_downloader, mock_storage):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    range_ = ChunkRange(0, 100)
    manager.start_chunk(range_)

    # Wait a bit for status to transition to DOWNLOADING
    await asyncio.sleep(0.05)

    # Resize to self
    manager.resize_chunk(range_, range_)

    stats = manager.get_chunk_stats(range_)
    assert stats.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.COMPLETED)
    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_resize_chunk_not_in_stats(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(0, 50)

    # r1 not started
    with pytest.raises(KeyError, match="Range .* not found"):
        manager.resize_chunk(r1, r2)


@pytest.mark.asyncio
async def test_resize_chunk_invalid_status(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(0, 50)

    manager.start_chunk(r1)

    # Force status to something invalid for resize (e.g. ERROR)
    manager._chunks_stats[r1].status = EDownloadStatus.ERROR

    with pytest.raises(ValueError, match="not in DOWNLOADING or COMPLETED"):
        manager.resize_chunk(r1, r2)

    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_as_stream_iterator(download_config, mock_downloader, mock_storage):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)

    async def smart_mock(chunk_range):
        await asyncio.sleep(0.01)
        stats = manager._chunks_stats[chunk_range]
        stats.set_status(EDownloadStatus.COMPLETED)
        stats.bytes_downloaded = stats.file_size or 0
        return chunk_range

    # Override BEFORE starting chunks
    manager._download_chunk = smart_mock

    r1 = ChunkRange(0, 10)
    r2 = ChunkRange(11, 20)
    manager.start_chunk(r1)
    manager.start_chunk(r2)

    results = []
    async for stats in manager.as_stream():
        results.append(stats)

    assert len(results) == 2
    assert {s.range for s in results} == {r1, r2}
    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_chunk_manager_get_active(download_config, mock_downloader, mock_storage):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    r1 = ChunkRange(0, 10)
    manager.start_chunk(r1)

    active = manager.get_active_chunks()
    assert active == [r1]

    await manager.cancel_all_chunks()
    assert manager.get_active_chunks() == []


@pytest.mark.asyncio
async def test_wait_for_chunks_timeout(download_config, mock_downloader, mock_storage):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)

    async def slow_download(chunk_range):
        await asyncio.sleep(1.0)
        return chunk_range

    manager._download_chunk = slow_download
    r1 = ChunkRange(0, 100)
    manager.start_chunk(r1)

    # Wait with short timeout
    completed = await manager.wait_for_completed_chunks(timeout=0.1)

    assert len(completed) == 0
    assert r1 in manager._chunks_tasks  # Task still active
    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_cancel_chunk_not_in_tasks(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    # Range not started
    res = await manager.cancel_chunk(ChunkRange(0, 100))
    assert res is False


@pytest.mark.asyncio
async def test_cancel_chunk_already_completed(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)

    async def instant_download(chunk_range):
        # Update stats to completed
        manager._chunks_stats[chunk_range].set_status(EDownloadStatus.COMPLETED)
        return chunk_range

    manager._download_chunk = instant_download
    r1 = ChunkRange(0, 100)
    manager.start_chunk(r1)

    # Wait for it to finish and be removed from tasks
    await manager.wait_for_completed_chunks()

    # Now try to cancel it
    res = await manager.cancel_chunk(r1)
    assert res is False


@pytest.mark.asyncio
async def test_register_chunk_stats_file_size_none(mock_downloader, mock_storage):
    # Config with file_size None
    cfg = DownloadConfig(
        file_name="test",
        file_dir="/tmp",
        file_size=None,
        file_id="1",
        download_url="http://test",
        file_created_at=None,
        protocol_data=None,
        max_connections_per_download=1,
        max_speed_bytes_per_second=1000,
    )
    manager = ChunkManager(cfg, mock_downloader, mock_storage)
    r1 = ChunkRange(0, 100)

    stats = manager._register_chunk_stats(r1)
    assert stats.file_size == 101  # effective_end is 100

    r2 = ChunkRange(0, None)
    stats2 = manager._register_chunk_stats(r2)
    assert stats2.file_size is None  # end is None and file_size is None


@pytest.mark.asyncio
async def test_get_downloaded_bytes_status_filter(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    r1 = ChunkRange(0, 10)
    r2 = ChunkRange(11, 20)
    r3 = ChunkRange(21, 30)

    # Manually add stats with different statuses using keyword arguments for safety
    manager._chunks_stats[r1] = ChunkDownloadStats(
        chunk_file_name="c1",
        range=r1,
        file_size=11,
        bytes_downloaded=11,
        status=EDownloadStatus.COMPLETED,
    )

    manager._chunks_stats[r2] = ChunkDownloadStats(
        chunk_file_name="c2",
        range=r2,
        file_size=10,
        bytes_downloaded=5,
        status=EDownloadStatus.DOWNLOADING,
    )

    manager._chunks_stats[r3] = ChunkDownloadStats(
        chunk_file_name="c3",
        range=r3,
        file_size=10,
        bytes_downloaded=10,
        status=EDownloadStatus.CANCELLED,
    )

    total = manager.get_downloaded_bytes()
    assert total == 11 + 5  # Only Completed and Downloading


@pytest.mark.asyncio
async def test_remove_chunk_active_cancels_and_cleans(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    range_ = ChunkRange(0, 100)

    async def slow_download(chunk_range):
        manager._chunks_tasks[chunk_range].init_signal.set()
        await asyncio.sleep(5.0)
        return chunk_range

    manager._download_chunk = slow_download

    # Mock storage.delete_data to track calls
    mock_storage.delete_data = AsyncMock()

    manager.start_chunk(range_)
    # Give it a tiny moment to start the task
    await asyncio.sleep(0.01)

    task_context = manager._chunks_tasks[range_]
    assert not task_context.task.done()

    await manager.remove_chunk(range_)

    assert range_ not in manager._chunks_tasks
    assert range_ not in manager._chunks_stats
    assert task_context.task.cancelled() or task_context.task.done()
    mock_storage.delete_data.assert_called_once()


@pytest.mark.asyncio
async def test_remove_chunk_completed_cleans_up(
    download_config, mock_downloader, mock_storage
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    range_ = ChunkRange(0, 100)
    mock_storage.delete_data = AsyncMock()

    # Manually add completed stats
    manager._chunks_stats[range_] = ChunkDownloadStats(
        chunk_file_name="test_chunk.sdownload",
        range=range_,
        file_size=101,
        status=EDownloadStatus.COMPLETED,
    )

    await manager.remove_chunk(range_)

    assert range_ not in manager._chunks_stats
    mock_storage.delete_data.assert_called_once_with("test_chunk.sdownload")


@pytest.mark.asyncio
async def test_remove_chunk_non_existent_is_safe(
    download_config, mock_downloader, mock_storage, caplog
):
    manager = ChunkManager(download_config, mock_downloader, mock_storage)
    range_ = ChunkRange(999, 9999)

    with caplog.at_level(logging.WARNING):
        await manager.remove_chunk(range_)

    # Should not raise, just log warning
    assert "No stats found for chunk" in caplog.text
