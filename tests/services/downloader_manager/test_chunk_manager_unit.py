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

from sDownload.file_system.local_storage import LocalStorage
from datetime import datetime


async def iter_helper(data):
    yield data


@pytest.fixture
def temp_storage(tmp_path):
    return LocalStorage(tmp_path)


@pytest.fixture
def download_config(temp_storage):
    return DownloadConfig(
        file_name="test_file",
        file_dir=str(temp_storage.storage_dir),
        file_size=10000,
        file_id="123",
        download_url="http://test.com",
        file_created_at=datetime.now(),
        protocol_data=None,
        max_connections_per_download=1,
        max_speed_bytes_per_second=100000,
    )


async def mock_download_generator(size, speed_limit=None):
    chunk_size = 1024
    generated = 0
    while generated < size:
        yield b"x" * min(chunk_size, size - generated)
        generated += chunk_size
        await asyncio.sleep(0.01)


@pytest.fixture
def mock_downloader():
    downloader = MagicMock()
    downloader.download_chunk.side_effect = (
        lambda url, start, end: mock_download_generator(end - start + 1)
    )
    return downloader


@pytest.fixture
def chunk_manager(download_config, mock_downloader, temp_storage):
    return ChunkManager(download_config, mock_downloader, temp_storage)


@pytest.mark.asyncio
async def test_chunk_manager_monitor_loop(chunk_manager):
    manager = chunk_manager

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
async def test_cancel_all_chunks_cleans_up(chunk_manager):
    manager = chunk_manager

    manager.start_chunk(ChunkRange(0, 4999))
    manager.start_chunk(ChunkRange(5000, 9999))

    await asyncio.sleep(0.05)

    await manager.cancel_all_chunks()

    # All chunks should be stopped (Cancelled or Completed)
    assert all(
        s.status in (EDownloadStatus.CANCELLED, EDownloadStatus.COMPLETED)
        for s in manager.stats.values()
    )
    await asyncio.sleep(1.1)
    if manager._monitor_task:
        assert manager._monitor_task.done()


@pytest.mark.asyncio
async def test_wait_for_first_completed_chunk(chunk_manager):
    manager = chunk_manager

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
async def test_lock_mechanism(chunk_manager):
    manager = chunk_manager

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
async def test_set_speed_limit_global(chunk_manager):
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(101, 200)

    chunk_manager.start_chunk(r1)
    chunk_manager.start_chunk(r2)

    # Default is config based, let's change it
    new_speed = 5000.0
    chunk_manager.set_speed_limit(new_speed)

    stats1 = chunk_manager.stats.get(r1)
    stats2 = chunk_manager.stats.get(r2)

    assert stats1.target_speed_bps == new_speed
    assert stats2.target_speed_bps == new_speed

    await chunk_manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_set_speed_limit_specific_chunk(chunk_manager):
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(101, 200)

    chunk_manager.start_chunk(r1)
    chunk_manager.start_chunk(r2)

    new_speed = 2000.0
    chunk_manager.set_speed_limit(new_speed, chunk_range=r1)

    stats1 = chunk_manager.stats.get(r1)
    stats2 = chunk_manager.stats.get(r2)

    assert stats1.target_speed_bps == new_speed
    assert stats2.target_speed_bps != new_speed  # Should remain unchanged

    await chunk_manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_set_speed_limit_invalid_chunk(chunk_manager, caplog):
    manager = chunk_manager

    # Just ensure it doesn't raise
    with caplog.at_level(logging.WARNING):
        manager.set_speed_limit(1000, chunk_range=ChunkRange(999, 9999))

    assert "No chunk stats found" in caplog.text
    await manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_start_chunk_idempotency_active(chunk_manager):
    manager = chunk_manager
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
async def test_start_chunk_idempotency_completed(chunk_manager):
    range_ = ChunkRange(0, 100)

    # Manually mark as completed
    chunk_manager._chunks_stats[range_] = ChunkDownloadStats(
        chunk_file_name="test",
        range=range_,
        file_size=101,
        status=EDownloadStatus.COMPLETED,
    )

    chunk_manager.start_chunk(range_)

    assert range_ not in chunk_manager._chunks_tasks


@pytest.mark.asyncio
async def test_resize_same_range_noop(chunk_manager):
    range_ = ChunkRange(0, 100)
    chunk_manager.start_chunk(range_)

    # Wait a bit for status to transition to DOWNLOADING
    await asyncio.sleep(0.05)

    # Resize to self
    chunk_manager.resize_chunk(range_, range_)

    stats = chunk_manager.stats.get(range_)
    assert stats.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.COMPLETED)
    await chunk_manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_resize_chunk_not_in_stats(chunk_manager):
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(0, 50)

    # r1 not started
    with pytest.raises(KeyError, match="Range .* not found"):
        chunk_manager.resize_chunk(r1, r2)


@pytest.mark.asyncio
async def test_resize_chunk_invalid_status(chunk_manager):
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(0, 50)

    chunk_manager.start_chunk(r1)

    # Force status to something invalid for resize (e.g. ERROR)
    chunk_manager._chunks_stats[r1].status = EDownloadStatus.ERROR

    with pytest.raises(ValueError, match="not in DOWNLOADING or COMPLETED"):
        chunk_manager.resize_chunk(r1, r2)

    await chunk_manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_as_stream_iterator(chunk_manager):
    async def smart_mock(chunk_range):
        await asyncio.sleep(0.01)
        stats = chunk_manager._chunks_stats[chunk_range]
        stats.set_status(EDownloadStatus.COMPLETED)
        stats.bytes_downloaded = stats.file_size or 0
        return chunk_range

    # Override BEFORE starting chunks
    chunk_manager._download_chunk = smart_mock

    r1 = ChunkRange(0, 10)
    r2 = ChunkRange(11, 20)
    chunk_manager.start_chunk(r1)
    chunk_manager.start_chunk(r2)

    results = []
    async for stats in chunk_manager.as_stream():
        results.append(stats)

    assert len(results) == 2
    assert {s.range for s in results} == {r1, r2}
    await chunk_manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_chunk_manager_get_active(chunk_manager):
    manager = chunk_manager
    r1 = ChunkRange(0, 10)
    manager.start_chunk(r1)

    # Filter to get only active (downloading/pending) chunks
    active = [
        r
        for r, s in manager.stats.items()
        if s.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.PENDING)
    ]
    assert active == [r1]

    await manager.cancel_all_chunks()
    active_after = [
        r
        for r, s in manager.stats.items()
        if s.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.PENDING)
    ]
    assert active_after == []


@pytest.mark.asyncio
async def test_wait_for_chunks_timeout(chunk_manager):
    async def slow_download(chunk_range):
        await asyncio.sleep(1.0)
        return chunk_range

    chunk_manager._download_chunk = slow_download
    r1 = ChunkRange(0, 100)
    chunk_manager.start_chunk(r1)

    # Wait with short timeout
    completed = await chunk_manager.wait_for_completed_chunks(timeout=0.1)

    assert len(completed) == 0
    assert r1 in chunk_manager._chunks_tasks  # Task still active
    await chunk_manager.cancel_all_chunks()


@pytest.mark.asyncio
async def test_cancel_chunk_not_in_tasks(chunk_manager):
    res = await chunk_manager.cancel_chunk(ChunkRange(0, 100))
    assert res is False


@pytest.mark.asyncio
async def test_cancel_chunk_already_completed(chunk_manager):
    async def instant_download(chunk_range):
        # Update stats to completed
        chunk_manager._chunks_stats[chunk_range].set_status(EDownloadStatus.COMPLETED)
        return chunk_range

    chunk_manager._download_chunk = instant_download
    r1 = ChunkRange(0, 100)
    chunk_manager.start_chunk(r1)

    # Wait for it to finish and be removed from tasks
    await chunk_manager.wait_for_completed_chunks()

    # Now try to cancel it
    res = await chunk_manager.cancel_chunk(r1)
    assert res is False


@pytest.mark.asyncio
async def test_register_chunk_stats_file_size_none(mock_downloader, temp_storage):
    # Config with file_size None
    cfg = DownloadConfig(
        file_name="test",
        file_dir=str(temp_storage.storage_dir),
        file_size=None,
        file_id="1",
        download_url="http://test",
        file_created_at=datetime.now(),
        protocol_data=None,
        max_connections_per_download=1,
        max_speed_bytes_per_second=1000,
    )
    manager = ChunkManager(cfg, mock_downloader, temp_storage)
    r1 = ChunkRange(0, 100)

    stats = manager._register_chunk_stats(r1)
    assert stats.file_size == 101  # effective_end is 100

    r2 = ChunkRange(0, None)
    stats2 = manager._register_chunk_stats(r2)
    assert stats2.file_size is None  # end is None and file_size is None


@pytest.mark.asyncio
async def test_get_downloaded_bytes_status_filter(chunk_manager):
    r1 = ChunkRange(0, 10)
    r2 = ChunkRange(11, 20)
    r3 = ChunkRange(21, 30)

    # Manually add stats with different statuses using keyword arguments for safety
    chunk_manager._chunks_stats[r1] = ChunkDownloadStats(
        chunk_file_name="c1",
        range=r1,
        file_size=11,
        bytes_downloaded=11,
        status=EDownloadStatus.COMPLETED,
    )

    chunk_manager._chunks_stats[r2] = ChunkDownloadStats(
        chunk_file_name="c2",
        range=r2,
        file_size=10,
        bytes_downloaded=5,
        status=EDownloadStatus.DOWNLOADING,
    )

    chunk_manager._chunks_stats[r3] = ChunkDownloadStats(
        chunk_file_name="c3",
        range=r3,
        file_size=10,
        bytes_downloaded=10,
        status=EDownloadStatus.CANCELLED,
    )

    total = chunk_manager.get_downloaded_bytes()
    assert total == 11 + 5  # Only Completed and Downloading


@pytest.mark.asyncio
async def test_remove_chunk_active_cancels_and_cleans(chunk_manager, temp_storage):
    range_ = ChunkRange(0, 100)

    async def slow_download(chunk_range):
        chunk_manager._chunks_tasks[chunk_range].init_signal.set()
        await asyncio.sleep(5.0)
        return chunk_range

    chunk_manager._download_chunk = slow_download

    # Mock storage.delete_data to track calls
    temp_storage.delete_data = AsyncMock()

    chunk_manager.start_chunk(range_)
    # Give it a tiny moment to start the task
    await asyncio.sleep(0.01)

    task_context = chunk_manager._chunks_tasks[range_]
    assert not task_context.task.done()

    await chunk_manager.delete_chunk_data(range_)

    assert range_ not in chunk_manager._chunks_tasks
    assert range_ not in chunk_manager._chunks_stats
    assert task_context.task.cancelled() or task_context.task.done()
    temp_storage.delete_data.assert_called_once()


@pytest.mark.asyncio
async def test_remove_chunk_completed_cleans_up(chunk_manager, temp_storage):
    manager = chunk_manager
    range_ = ChunkRange(0, 100)
    temp_storage.delete_data = AsyncMock()

    # Manually add completed stats
    manager._chunks_stats[range_] = ChunkDownloadStats(
        chunk_file_name="test_chunk.sdownload",
        range=range_,
        file_size=101,
        status=EDownloadStatus.COMPLETED,
    )

    await manager.delete_chunk_data(range_)

    assert range_ not in manager._chunks_stats
    temp_storage.delete_data.assert_called_once_with("test_chunk.sdownload")


@pytest.mark.asyncio
async def test_remove_chunk_non_existent_is_safe(chunk_manager, caplog):
    manager = chunk_manager
    range_ = ChunkRange(999, 9999)

    with caplog.at_level(logging.WARNING):
        await manager.delete_chunk_data(range_)

    # Should not raise, just log warning
    assert "No stats found for chunk" in caplog.text


@pytest.mark.asyncio
async def test_chunk_manager_merge_with_overlaps(chunk_manager, temp_storage):
    # Setup: 3 chunks with overlaps
    # C1: 0-14 (15 bytes)
    # C2: 10-24 (15 bytes)
    # C3: 20-29 (10 bytes)

    c1_range = ChunkRange(0, 14)
    c2_range = ChunkRange(10, 24)
    c3_range = ChunkRange(20, 29)

    # Standardize config for this test: total coverage is 30 bytes
    chunk_manager._cfg.file_size = 30

    s1 = chunk_manager._register_chunk_stats(c1_range)
    s2 = chunk_manager._register_chunk_stats(c2_range)
    s3 = chunk_manager._register_chunk_stats(c3_range)

    await temp_storage.save_binary_data(
        s1.chunk_file_name, iter_helper(b"0123456789ABCDE")
    )
    await temp_storage.save_binary_data(
        s2.chunk_file_name, iter_helper(b"ABCDEfghijklmno")
    )
    await temp_storage.save_binary_data(s3.chunk_file_name, iter_helper(b"klmnoPQRST"))

    s1.set_status(EDownloadStatus.COMPLETED)
    s2.set_status(EDownloadStatus.COMPLETED)
    s3.set_status(EDownloadStatus.COMPLETED)

    dest_key = await chunk_manager.merge_chunks(cleanup=True)

    assert dest_key == chunk_manager._cfg.file_name

    content = b""
    async for chunk in temp_storage.get_binary_data(dest_key):
        content += chunk

    expected = b"0123456789ABCDE" + b"fghijklmno" + b"PQRST"
    assert content == expected
    # The expected total size in the test data setup is 30, but the config says 10000.
    # merge_chunks uses the actual data from fragments, so this check is fine.
    assert len(content) == 30

    storage_files = await temp_storage.list_data()
    storage_keys = [f.key for f in storage_files]
    assert dest_key in storage_keys
    assert s1.chunk_file_name not in storage_keys
    assert s2.chunk_file_name not in storage_keys
    assert s3.chunk_file_name not in storage_keys


@pytest.mark.asyncio
async def test_cleanup_comprehensive(chunk_manager, temp_storage):
    range_ = ChunkRange(0, 100)

    async def slow_download(chunk_range):
        chunk_manager._chunks_tasks[chunk_range].init_signal.set()
        await asyncio.sleep(5.0)
        return chunk_range

    chunk_manager._download_chunk = slow_download

    # 1. Start a chunk (creates stats)
    chunk_manager.start_chunk(range_)
    stats = chunk_manager.stats.get(range_)

    # Manually create the file so cleanup_temp_files finds it
    await temp_storage.save_binary_data(stats.chunk_file_name, iter_helper(b"test"))

    # Mock delete_data AFTER creating the file to track the call
    temp_storage.delete_data = AsyncMock()

    await asyncio.sleep(0.01)  # task starts, monitor starts

    assert range_ in chunk_manager._chunks_tasks
    assert range_ in chunk_manager._chunks_stats
    assert chunk_manager._monitor_task is not None

    # 2. Perform cleanup
    await chunk_manager.cleanup()

    # Assertions
    assert len(chunk_manager._chunks_tasks) == 0
    assert len(chunk_manager._chunks_stats) == 0
    assert chunk_manager._monitor_task is None
    temp_storage.delete_data.assert_called_with(stats.chunk_file_name)
