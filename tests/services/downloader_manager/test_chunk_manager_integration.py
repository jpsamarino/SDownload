import asyncio
import logging
from collections.abc import AsyncIterable
from unittest.mock import AsyncMock, MagicMock
import pytest
from datetime import datetime, timezone

from sDownload.file_system.local_storage import LocalStorage
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.interfaces.protocols.downloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_info_model import FileInfoModel
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel
from sDownload.services.downloader_manager.chunk_manager import ChunkManager
from sDownload.services.downloader_manager.download_config import DownloadConfig
from sDownload.services.downloader_manager.download_stats_models import EDownloadStatus


@pytest.fixture
def storage(tmp_path: str):
    return LocalStorage(storage_dir=tmp_path)


@pytest.fixture
def setup_downloader_and_config(nginx_custom):
    async def _setup(file_name="file_100k.bin", limit_speed=True):
        path = "limited_speed" if limit_speed else "default"
        config = HttpConfigModel(timeout_connect_s=20.0)
        downloader = HttpxDownloader(config)
        result_list = await downloader.get_file_info(
            f"{nginx_custom['http']}/{path}/{file_name}"
        )
        result = result_list[0]

        download_config = DownloadConfig(
            file_name=result.file_name,
            file_dir=result.file_dir,
            file_size=result.file_size,
            file_id=result.file_id,
            download_url=result.download_url,
            file_created_at=datetime.now(timezone.utc),
            protocol_data=None,
            max_connections_per_download=2,
            max_speed_bytes_per_second=1024 * 1024,
        )
        return {
            "config": config,
            "downloader": downloader,
            "result": result,
            "download_config": download_config,
        }

    return _setup


@pytest.mark.asyncio
async def test_chunk_manager_with_nginx(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config(
        file_name="file_100k.bin", limit_speed=False
    )
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    assert setup["result"].file_name == "file_100k.bin"
    assert setup["result"].file_size == 102400

    chunk_manager = ChunkManager(download_config, downloader, storage)

    # Start chunks
    chunk_manager.start_chunk(ChunkRange(0, 51199))
    chunk_manager.start_chunk(ChunkRange(51200, None))

    # Wait for completion
    await chunk_manager.wait_for_completed_chunks()

    # Verify downloaded bytes
    total_downloaded = chunk_manager.get_downloaded_bytes()
    assert total_downloaded == download_config.file_size

    # Cleanup
    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_cancel_chunks(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    chunk_manager.start_chunk(ChunkRange(0, 51200))
    chunk_manager.start_chunk(ChunkRange(51201, 102399))

    await asyncio.sleep(0.1)
    is_cancelled = await chunk_manager.cancel_chunk(ChunkRange(0, 51200))
    await chunk_manager.wait_for_completed_chunks(0.5)

    assert is_cancelled is True
    assert (
        chunk_manager.stats.get(ChunkRange(0, 51200)).status
        == EDownloadStatus.CANCELLED
    )
    assert chunk_manager.get_downloaded_bytes() < download_config.file_size

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_wrong_partial_chunks_sizes(
    setup_downloader_and_config, storage
):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    chunk_manager.start_chunk(ChunkRange(102390, 511990))
    chunk_manager.start_chunk(ChunkRange(51200, 102381))

    await chunk_manager.wait_for_completed_chunks(0.5)

    assert (
        chunk_manager.stats.get(ChunkRange(102390, 511990)).status
        == EDownloadStatus.ERROR
    )
    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_cleanup_temp_files(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    chunk_manager.start_chunk(ChunkRange(0, 51192))
    chunk_manager.start_chunk(ChunkRange(51203, 102399))

    await chunk_manager.wait_for_completed_chunks(1.0)

    temp_files = [stats.chunk_file_name for stats in chunk_manager.stats.values()]
    listed_files = await storage.list_data()
    listed_keys = [f.key for f in listed_files]
    assert any(f in listed_keys for f in temp_files)

    await chunk_manager.cleanup()

    listed_files_after = await storage.list_data()
    listed_keys_after = [f.key for f in listed_files_after]
    assert not any(f in listed_keys_after for f in temp_files)


@pytest.mark.asyncio
async def test_chunk_manager_stats_tracking(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    chunk_manager.start_chunk(ChunkRange(0, 51170))
    chunk_manager.start_chunk(ChunkRange(51171, 102399))

    await asyncio.sleep(0.1)
    # At least some part of the download should be happening or ready to happen
    active = [
        s
        for s in chunk_manager.stats.values()
        if s.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.PENDING)
    ]
    assert len(active) > 0
    stats = chunk_manager.stats.get(ChunkRange(0, 51170))
    assert stats is not None
    assert stats.status == EDownloadStatus.DOWNLOADING

    await chunk_manager.wait_for_completed_chunks(1.0)

    all_stats = chunk_manager.stats
    assert len(all_stats) == 2
    for key, stat in all_stats.items():
        assert stat.status == EDownloadStatus.COMPLETED
        assert stat.bytes_downloaded == stat.file_size

    total_downloaded = chunk_manager.get_downloaded_bytes()
    assert total_downloaded == download_config.file_size

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_cancel_all_chunks(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config(file_name="file_1M.bin")
    download_config = setup["download_config"]
    downloader = setup["downloader"]
    download_config.max_speed_bytes_per_second = 1024  # Override for this test

    chunk_manager = ChunkManager(download_config, downloader, storage)

    chunk_manager.start_chunk(ChunkRange(0, 511990))
    chunk_manager.start_chunk(ChunkRange(512000, 1023990))

    await asyncio.sleep(0.5)
    await chunk_manager.cancel_all_chunks()

    # No chunks should be in an active state
    active = [
        s
        for s in chunk_manager.stats.values()
        if s.status in (EDownloadStatus.DOWNLOADING, EDownloadStatus.PENDING)
    ]
    assert len(active) == 0

    all_stats = chunk_manager.stats
    for key, stat in all_stats.items():
        assert stat.status in [EDownloadStatus.CANCELLED]

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_resize_chunk_success_with_finished_chunks(
    setup_downloader_and_config, storage
):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)
    original_range = ChunkRange(0, 51199)
    chunk_manager.start_chunk(original_range)
    done_chunks = await chunk_manager.wait_for_completed_chunks(1.0)
    assert len(done_chunks) == 1
    assert done_chunks[0].range == original_range
    assert done_chunks[0].status == EDownloadStatus.COMPLETED
    assert done_chunks[0].bytes_downloaded == done_chunks[0].file_size

    new_range = ChunkRange(0, 25599)
    chunk_manager.resize_chunk(original_range, new_range)

    assert (
        chunk_manager.stats.get(new_range).status == EDownloadStatus.AWAITING_SUCCESSION
    )

    await chunk_manager.wait_for_completed_chunks(0.1)

    assert chunk_manager.stats.get(new_range).status == EDownloadStatus.COMPLETED
    assert chunk_manager.stats.get(original_range).status == EDownloadStatus.DEPRECATED

    assert chunk_manager.stats.get(new_range).bytes_downloaded == 25600
    assert chunk_manager.get_downloaded_bytes() == 25600

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_resize_chunk_success_with_downloading_chunks(
    setup_downloader_and_config, storage
):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)
    original_range = ChunkRange(0, 51199)
    chunk_manager.start_chunk(original_range)
    await asyncio.sleep(0.1)
    new_range = ChunkRange(0, 25599)
    chunk_manager.resize_chunk(original_range, new_range)

    assert (
        chunk_manager.stats.get(new_range).status == EDownloadStatus.AWAITING_SUCCESSION
    )

    await chunk_manager.wait_for_completed_chunks(1)

    assert chunk_manager.stats.get(new_range).status == EDownloadStatus.COMPLETED
    assert chunk_manager.stats.get(original_range).status == EDownloadStatus.DEPRECATED

    assert chunk_manager.stats.get(new_range).bytes_downloaded == 25600
    assert chunk_manager.get_downloaded_bytes() == 25600

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_resize_chunk_prefix_range(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    original_range = ChunkRange(2, 30000)
    chunk_manager.start_chunk(original_range)
    await asyncio.sleep(0.1)

    new_range = ChunkRange(239, 10239)  # 10KB
    chunk_manager.resize_chunk(original_range, new_range)

    await chunk_manager.wait_for_completed_chunks(1)

    assert chunk_manager.stats.get(new_range).status == EDownloadStatus.COMPLETED
    assert chunk_manager.stats.get(original_range).status == EDownloadStatus.DEPRECATED

    assert chunk_manager.stats.get(new_range).bytes_downloaded == 10001
    assert chunk_manager.get_downloaded_bytes() == 10001

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_resize_chunk_head_cut(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    original_range = ChunkRange(0, 51123)
    chunk_manager.start_chunk(original_range)

    await asyncio.sleep(0.1)

    new_range = ChunkRange(10240, 51123)
    chunk_manager.resize_chunk(original_range, new_range)

    await chunk_manager.wait_for_completed_chunks(1.0)

    assert chunk_manager.stats.get(new_range).status == EDownloadStatus.COMPLETED
    assert chunk_manager.stats.get(original_range).status == EDownloadStatus.DEPRECATED
    assert chunk_manager.stats.get(new_range).bytes_downloaded == 51123 - 10240 + 1

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_resize_chunk_cancel_successor(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config()
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    original_range = ChunkRange(0, 52221)
    chunk_manager.start_chunk(original_range)

    await asyncio.sleep(0.01)

    new_range = ChunkRange(0, 25511)
    chunk_manager.resize_chunk(original_range, new_range)
    await chunk_manager.cancel_chunk(new_range)

    await asyncio.sleep(1)

    assert chunk_manager.stats.get(new_range).status == EDownloadStatus.CANCELLED
    assert chunk_manager.stats.get(original_range).status == EDownloadStatus.CANCELLED

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_resize_chunk_validation_error():
    mock_downloader = MagicMock(spec=DownloaderProtocol)
    mock_storage = MagicMock(spec=FileStorageProtocol)

    download_config = DownloadConfig(
        file_name="test.bin",
        file_dir="/tmp",
        file_size=100000,
        file_id="123",
        download_url="http://example.com/test.bin",
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    chunk_manager = ChunkManager(download_config, mock_downloader, mock_storage)

    with pytest.raises(KeyError):
        chunk_manager.resize_chunk(ChunkRange(0, 100), ChunkRange(0, 50))

    from sDownload.services.downloader_manager.download_stats_models import (
        ChunkDownloadStats,
    )

    chunk_manager._chunks_stats[ChunkRange(0, 100)] = ChunkDownloadStats(
        chunk_file_name="test.sdownload",
        range=ChunkRange(0, 100),
        file_size=101,
    )

    with pytest.raises(ValueError):
        chunk_manager.resize_chunk(ChunkRange(0, 100), ChunkRange(0, 200))

    with pytest.raises(ValueError):
        chunk_manager.resize_chunk(ChunkRange(50, 100), ChunkRange(40, 80))


@pytest.mark.asyncio
async def test_chunk_manager_remove_chunk_integration(
    setup_downloader_and_config, storage
):
    setup = await setup_downloader_and_config(file_name="file_1M.bin")
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    range_1 = ChunkRange(0, 102399)  # 100KB
    range_2 = ChunkRange(102400, 204799)  # 100KB

    chunk_manager.start_chunk(range_1)
    chunk_manager.start_chunk(range_2)

    # Wait for some data to be downloaded
    await asyncio.sleep(0.1)

    stats_1 = chunk_manager.stats.get(range_1)
    stats_2 = chunk_manager.stats.get(range_2)
    file_1 = stats_1.chunk_file_name
    file_2 = stats_2.chunk_file_name

    # 1. Remove range_1 while downloading
    await chunk_manager.delete_chunk_data(range_1)

    assert range_1 not in chunk_manager.stats
    assert chunk_manager.stats.get(range_1) is None

    # Check file is deleted
    listed_files = await storage.list_data()
    listed_keys = [f.key for f in listed_files]
    assert file_1 not in listed_keys

    # 2. Wait for range_2 to complete
    await chunk_manager.wait_for_completed_chunks()
    assert chunk_manager.stats.get(range_2).status == EDownloadStatus.COMPLETED

    # 3. Remove completed range_2
    await chunk_manager.delete_chunk_data(range_2)
    assert chunk_manager.stats.get(range_2) is None

    listed_files_after = await storage.list_data()
    listed_keys_after = [f.key for f in listed_files_after]
    assert file_2 not in listed_keys_after

    # Verify total downloaded bytes is 0 because all chunks were removed
    assert chunk_manager.get_downloaded_bytes() == 0

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_merge_basic(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config(
        file_name="file_100k.bin", limit_speed=False
    )
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    # 1. Download in two contiguous parts
    r1 = ChunkRange(0, 51199)
    r2 = ChunkRange(51200, None)

    chunk_manager.start_chunk(r1)
    chunk_manager.start_chunk(r2)

    await chunk_manager.wait_for_completed_chunks()

    # 2. Merge
    dest_file = await chunk_manager.merge_chunks(cleanup=True)

    # 3. Verify
    assert dest_file == download_config.file_name
    listed = await storage.list_data()
    listed_keys = [f.key for f in listed]
    assert dest_file in listed_keys

    # Check size
    file_info = next(f for f in listed if f.key == dest_file)
    assert file_info.size_bytes == 102400

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_merge_with_overlaps(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config(
        file_name="file_100k.bin", limit_speed=False
    )
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    # 1. Start overlapping chunks
    # Part A: 0-50000
    # Part B: 25000-75000
    # Part C: 50000-None (102399)
    r1 = ChunkRange(0, 50000)
    r2 = ChunkRange(25000, 75000)
    r3 = ChunkRange(50000, None)

    chunk_manager.start_chunk(r1)
    chunk_manager.start_chunk(r2)
    chunk_manager.start_chunk(r3)

    await chunk_manager.wait_for_completed_chunks()

    # 2. Merge - should use optimal coverage
    dest_file = await chunk_manager.merge_chunks(cleanup=True)

    # 3. Verify
    assert dest_file == download_config.file_name
    listed = await storage.list_data()
    listed_keys = [f.key for f in listed]
    assert dest_file in listed_keys

    file_info = next(f for f in listed if f.key == dest_file)
    assert file_info.size_bytes == 102400

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_merge_after_resize(setup_downloader_and_config, storage):
    setup = await setup_downloader_and_config(
        file_name="file_100k.bin", limit_speed=False
    )
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    # 1. Start a part and resize it mid-way (or after completion)
    r1_orig = ChunkRange(0, 50000)
    chunk_manager.start_chunk(r1_orig)

    # Wait for completion of original
    await chunk_manager.wait_for_completed_chunks()

    # Resize to something smaller
    r1_new = ChunkRange(0, 25000)
    chunk_manager.resize_chunk(r1_orig, r1_new)

    # Wait for resize (succession) to complete
    await chunk_manager.wait_for_completed_chunks()

    # 2. Start the rest of the file
    r2 = ChunkRange(25001, None)
    chunk_manager.start_chunk(r2)
    await chunk_manager.wait_for_completed_chunks()

    # 3. Merge
    dest_file = await chunk_manager.merge_chunks(cleanup=True)

    # 4. Verify
    assert dest_file == download_config.file_name
    listed = await storage.list_data()
    file_info = next(f for f in listed if f.key == dest_file)
    assert file_info.size_bytes == 102400

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_monitor_lifecycle_integration(
    setup_downloader_and_config, storage
):
    """Integration: Monitor should start when a chunk starts and stop when all finish"""
    setup = await setup_downloader_and_config(
        file_name="file_100k.bin", limit_speed=False
    )
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    chunk_manager = ChunkManager(download_config, downloader, storage)

    # 1. No monitor at start
    assert chunk_manager._monitor_task is None

    # 2. Monitor starts with first chunk
    r1 = ChunkRange(0, 1023)
    chunk_manager.start_chunk(r1)
    await asyncio.sleep(0.05)  # let it start
    assert chunk_manager._monitor_task is not None
    assert not chunk_manager._monitor_task.done()

    # 3. Monitor stops when chunk completes
    await chunk_manager.wait_for_completed_chunks()

    # Give the monitor a moment to detect completion and stop itself
    await asyncio.sleep(0.2)
    assert chunk_manager._monitor_task is None or chunk_manager._monitor_task.done()

    # If the monitor task is still there but done, _check_stop_monitor wasn't called
    # but the task logic should have exited. However, the requirement is that
    # ChunkManager manages it. In wait_for_completed_chunks, _check_stop_monitor is called.
    assert chunk_manager._monitor_task is None

    await chunk_manager.cleanup()


@pytest.mark.asyncio
async def test_chunk_manager_context_manager_lifecycle_integration(
    setup_downloader_and_config, storage
):
    """Integration: Full lifecycle using async context manager"""
    setup = await setup_downloader_and_config(
        file_name="file_100k.bin", limit_speed=False
    )
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    async with ChunkManager(download_config, downloader, storage) as manager:
        # 1. Start a few chunks
        r1 = ChunkRange(0, 1023)
        r2 = ChunkRange(1024, 2047)
        manager.start_chunk(r1)
        manager.start_chunk(r2)

        await manager.wait_for_completed_chunks()

    # After context exit, it should be cleaned up
    assert manager._cleaned_up is True
    # Files should be gone (cleanup_temp_files called)
    # Verify files on disk are gone for those chunks
    listed = await storage.list_data()
    chunk_files = [f.key for f in listed if "file_100k.bin.sdownload" in f.key]
    assert len(chunk_files) == 0


@pytest.mark.asyncio
async def test_chunk_manager_context_manager_external_cancel_integration(
    setup_downloader_and_config, storage
):
    """Integration: Verify cleanup runs even if the outer task is cancelled"""
    setup = await setup_downloader_and_config(
        file_name="file_100k.bin", limit_speed=False
    )
    download_config = setup["download_config"]
    downloader = setup["downloader"]

    inner_manager = None

    async def run_manager():
        nonlocal inner_manager
        async with ChunkManager(download_config, downloader, storage) as manager:
            inner_manager = manager
            manager.start_chunk(ChunkRange(0, 5000))
            await asyncio.sleep(10)  # Wait for long time, to be cancelled

    task = asyncio.create_task(run_manager())
    await asyncio.sleep(0.1)  # Let it start

    assert inner_manager is not None
    assert not inner_manager._cleaned_up

    # External cancel
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Cleanup MUST have run
    assert inner_manager._cleaned_up is True

    # Check if monitor and tasks are dead
    assert inner_manager._monitor_task is None or inner_manager._monitor_task.done()
    for ctx in inner_manager._chunks_tasks.values():
        assert ctx.task.done() or ctx.task.cancelled()
