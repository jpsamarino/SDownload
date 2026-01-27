import asyncio
import logging
from collections.abc import AsyncIterable
from unittest.mock import AsyncMock, MagicMock
import pytest
from datetime import datetime

from sDownload.file_system.local_storage import LocalStorage
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.interfaces.protocols.downloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.file_info_model import FileInfoModel
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel
from sDownload.services.downloader_manager.chunk_manager import ChunkManager
from sDownload.services.downloader_manager.download_task import DownloadConfig
from sDownload.services.downloader_manager.download_stats_models import EDownloadStatus


@pytest.fixture
def storage(tmp_path: str):
    return LocalStorage(storage_dir="./delet")


@pytest.mark.asyncio
async def test_chunk_manager_with_nginx(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/default/file_100k.bin"
    )
    result = result_list[0]
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,  # 1 MB/s limit
    )

    logger = logging.getLogger("chunk_manager_test")
    logger.setLevel(logging.INFO)

    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    # Start chunks
    chunk_manager.start_chunk(ChunkRange(0, 51199))
    chunk_manager.start_chunk(ChunkRange(51200, None))

    # Wait for completion
    completed = await chunk_manager.wait_for_completed_chunks()
    logger.info(f"Completed chunks: {completed}")

    # Verify downloaded bytes
    total_downloaded = chunk_manager.get_downloaded_bytes()
    assert total_downloaded == download_config.file_size

    # Cleanup
    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_chunk_manager_cancel_chunks(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("chunk_manager_cancel_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    chunk_manager.start_chunk(ChunkRange(0, 51200))
    chunk_manager.start_chunk(ChunkRange(51201, 102399))

    await asyncio.sleep(0.1)
    is_cancelled = await chunk_manager.cancel_chunk(ChunkRange(0, 51200))
    await chunk_manager.wait_for_completed_chunks(0.5)

    assert is_cancelled is True
    assert (
        chunk_manager.get_chunk_stats(ChunkRange(0, 51200)).status
        == EDownloadStatus.CANCELLED
    )
    assert chunk_manager.get_downloaded_bytes() < download_config.file_size

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_chunk_manager_wrong_partial_chunks_sizes(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("chunk_manager_cancel_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    chunk_manager.start_chunk(ChunkRange(102390, 511990))
    chunk_manager.start_chunk(ChunkRange(51200, 102381))

    await chunk_manager.wait_for_completed_chunks(0.5)

    assert (
        chunk_manager.get_chunk_stats(ChunkRange(102390, 511990)).status
        == EDownloadStatus.ERROR
    )
    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_chunk_manager_cleanup_temp_files(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("chunk_manager_cleanup_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    chunk_manager.start_chunk(ChunkRange(0, 51192))
    chunk_manager.start_chunk(ChunkRange(51203, 102399))

    await chunk_manager.wait_for_completed_chunks(1.0)

    temp_files = [
        stats.chunk_file_name for stats in chunk_manager.get_all_chunk_stats().values()
    ]
    listed_files = await storage.list_data()
    listed_keys = [f.key for f in listed_files]
    assert any(f in listed_keys for f in temp_files)

    await chunk_manager.cleanup_temp_files()

    listed_files_after = await storage.list_data()
    listed_keys_after = [f.key for f in listed_files_after]
    assert not any(f in listed_keys_after for f in temp_files)


@pytest.mark.asyncio
async def test_chunk_manager_stats_tracking(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("chunk_manager_stats_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    chunk_manager.start_chunk(ChunkRange(0, 51170))
    chunk_manager.start_chunk(ChunkRange(51171, 102399))

    await asyncio.sleep(0.1)
    active = chunk_manager.get_active_chunks()
    assert len(active) > 0
    stats = chunk_manager.get_chunk_stats(ChunkRange(0, 51170))
    assert stats is not None
    assert stats.status == EDownloadStatus.DOWNLOADING

    await chunk_manager.wait_for_completed_chunks(1.0)

    all_stats = chunk_manager.get_all_chunk_stats()
    assert len(all_stats) == 2
    for key, stat in all_stats.items():
        assert stat.status == EDownloadStatus.COMPLETED
        assert stat.bytes_downloaded == stat.file_size

    total_downloaded = chunk_manager.get_downloaded_bytes()
    assert total_downloaded == download_config.file_size

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_chunk_manager_cancel_all_chunks(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_1M.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024,
    )

    logger = logging.getLogger("chunk_manager_cancel_all_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    chunk_manager.start_chunk(ChunkRange(0, 511990))
    chunk_manager.start_chunk(ChunkRange(512000, 1023990))

    await asyncio.sleep(0.5)
    await chunk_manager.cancel_all_chunks()

    assert len(chunk_manager.get_active_chunks()) == 0

    all_stats = chunk_manager.get_all_chunk_stats()
    for key, stat in all_stats.items():
        assert stat.status in [EDownloadStatus.CANCELLED]

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_resize_chunk_success_with_finished_chunks(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("resize_chunk_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)
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
        chunk_manager.get_chunk_stats(new_range).status
        == EDownloadStatus.AWAITING_SUCCESSION
    )

    await chunk_manager.wait_for_completed_chunks(0.1)

    assert chunk_manager.get_chunk_stats(new_range).status == EDownloadStatus.COMPLETED
    assert (
        chunk_manager.get_chunk_stats(original_range).status
        == EDownloadStatus.DEPRECATED
    )

    assert chunk_manager.get_chunk_stats(new_range).bytes_downloaded == 25600
    assert chunk_manager.get_downloaded_bytes() == 25600

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_resize_chunk_success_with_downloading_chunks(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("resize_chunk_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)
    original_range = ChunkRange(0, 51199)
    chunk_manager.start_chunk(original_range)
    await asyncio.sleep(0.1)
    new_range = ChunkRange(0, 25599)
    chunk_manager.resize_chunk(original_range, new_range)

    assert (
        chunk_manager.get_chunk_stats(new_range).status
        == EDownloadStatus.AWAITING_SUCCESSION
    )

    await chunk_manager.wait_for_completed_chunks(1)

    assert chunk_manager.get_chunk_stats(new_range).status == EDownloadStatus.COMPLETED
    assert (
        chunk_manager.get_chunk_stats(original_range).status
        == EDownloadStatus.DEPRECATED
    )

    assert chunk_manager.get_chunk_stats(new_range).bytes_downloaded == 25600
    assert chunk_manager.get_downloaded_bytes() == 25600

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_resize_chunk_prefix_range(nginx_custom, storage):

    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    chunk_manager = ChunkManager(download_config, downloader, storage)

    original_range = ChunkRange(2, 30000)
    chunk_manager.start_chunk(original_range)
    await asyncio.sleep(0.1)

    new_range = ChunkRange(239, 10239)  # 10KB
    chunk_manager.resize_chunk(original_range, new_range)

    await chunk_manager.wait_for_completed_chunks(1)

    assert chunk_manager.get_chunk_stats(new_range).status == EDownloadStatus.COMPLETED
    assert (
        chunk_manager.get_chunk_stats(original_range).status
        == EDownloadStatus.DEPRECATED
    )

    assert chunk_manager.get_chunk_stats(new_range).bytes_downloaded == 10001
    assert chunk_manager.get_downloaded_bytes() == 10001

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_resize_chunk_head_cut(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("resize_chunk_head_cut_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    original_range = ChunkRange(0, 51123)
    chunk_manager.start_chunk(original_range)

    await asyncio.sleep(0.1)

    new_range = ChunkRange(10240, 51123)
    chunk_manager.resize_chunk(original_range, new_range)

    await chunk_manager.wait_for_completed_chunks(1.0)

    assert chunk_manager.get_chunk_stats(new_range).status == EDownloadStatus.COMPLETED
    assert (
        chunk_manager.get_chunk_stats(original_range).status
        == EDownloadStatus.DEPRECATED
    )
    assert (
        chunk_manager.get_chunk_stats(new_range).bytes_downloaded == 51123 - 10240 + 1
    )

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_resize_chunk_cancel_successor(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
    )
    result = result_list[0]

    download_config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
        max_speed_bytes_per_second=1024 * 1024,
    )

    logger = logging.getLogger("resize_chunk_cancel_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    original_range = ChunkRange(0, 52221)
    chunk_manager.start_chunk(original_range)

    await asyncio.sleep(0.01)

    new_range = ChunkRange(0, 25511)
    chunk_manager.resize_chunk(original_range, new_range)
    await chunk_manager.cancel_chunk(new_range)

    await asyncio.sleep(1)

    assert chunk_manager.get_chunk_stats(new_range).status == EDownloadStatus.CANCELLED
    assert (
        chunk_manager.get_chunk_stats(original_range).status
        == EDownloadStatus.CANCELLED
    )

    await chunk_manager.cleanup_temp_files()


@pytest.mark.asyncio
async def test_resize_chunk_validation_error():
    """Test that resize_chunk raises ValueError for invalid ranges."""
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

    # Try to resize a non-existent chunk
    with pytest.raises(KeyError):
        chunk_manager.resize_chunk(ChunkRange(0, 100), ChunkRange(0, 50))

    # Create a mock stats entry
    from sDownload.services.downloader_manager.download_stats_models import (
        ChunkDownloadStats,
    )

    chunk_manager._chunks_stats[ChunkRange(0, 100)] = ChunkDownloadStats(
        chunk_file_name="test.sdownload",
        range=ChunkRange(0, 100),
        file_size=101,
    )

    # Try to resize with a range that extends beyond the original
    with pytest.raises(ValueError):
        chunk_manager.resize_chunk(ChunkRange(0, 100), ChunkRange(0, 200))

    # Try to resize with a range that starts before the original
    with pytest.raises(ValueError):
        chunk_manager.resize_chunk(ChunkRange(50, 100), ChunkRange(40, 80))
