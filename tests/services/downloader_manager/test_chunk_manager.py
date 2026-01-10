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
        f"{nginx_custom['http']}/limited_speed/file_100k.bin"
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
    chunk_manager.start_chunk(ChunkRange(51200, 102399))

    # Wait for completion
    while chunk_manager.get_active_chunks():
        completed = await chunk_manager.wait_for_completed_chunks(1.0)
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

    # Start chunks
    chunk_manager.start_chunk(ChunkRange(0, 51199))
    chunk_manager.start_chunk(ChunkRange(51200, 102399))

    # Wait a bit then cancel one chunk
    await asyncio.sleep(0.1)
    is_cancelled = await chunk_manager.cancel_chunk(ChunkRange(0, 51199))
    # Wait for remaining chunks
    while chunk_manager.get_active_chunks():
        completed = await chunk_manager.wait_for_completed_chunks(1.0)
        logger.info(f"Completed after cancel: {completed}")

    # Verify only one chunk completed
    assert is_cancelled is True
    assert (
        chunk_manager.get_chunk_stats(ChunkRange(0, 51199)).status
        == EDownloadStatus.CANCELLED
    )
    assert (
        chunk_manager.get_downloaded_bytes() < download_config.file_size
    )  # Not full download

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

    # Start chunks
    chunk_manager.start_chunk(ChunkRange(102390, 511990))
    chunk_manager.start_chunk(ChunkRange(51200, 102399))

    # Wait for remaining chunks
    while chunk_manager.get_active_chunks():
        completed = await chunk_manager.wait_for_completed_chunks(1)
        logger.info(f"Completed after cancel: {completed}")

    assert (
        chunk_manager.get_chunk_stats(ChunkRange(102390, 511990)).status
        == EDownloadStatus.ERROR
    )
    # assert (
    #     chunk_manager.get_downloaded_bytes() < download_config.file_size
    # )  # Not full download

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

    # Start and complete chunks
    chunk_manager.start_chunk(ChunkRange(0, 51199))
    chunk_manager.start_chunk(ChunkRange(51200, 102399))

    while chunk_manager.get_active_chunks():
        await chunk_manager.wait_for_completed_chunks(1.0)

    # Check temp files exist before cleanup
    temp_files = [
        stats.chunk_file_name for stats in chunk_manager.get_all_chunk_stats().values()
    ]
    listed_files = await storage.list_data()
    listed_keys = [f.key for f in listed_files]
    assert any(f in listed_keys for f in temp_files)

    # Cleanup
    await chunk_manager.cleanup_temp_files()

    # Verify temp files are deleted
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

    # Start chunks
    chunk_manager.start_chunk(ChunkRange(0, 51199))
    chunk_manager.start_chunk(ChunkRange(51200, 102399))

    # Check stats during download
    await asyncio.sleep(0.5)
    active = chunk_manager.get_active_chunks()
    assert len(active) > 0
    stats = chunk_manager.get_chunk_stats(0, 51199)
    assert stats is not None
    assert stats.status == EDownloadStatus.DOWNLOADING

    # Wait for completion
    while chunk_manager.get_active_chunks():
        await chunk_manager.wait_for_completed_chunks(1.0)

    # Check final stats
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

    logger = logging.getLogger("chunk_manager_cancel_all_test")
    chunk_manager = ChunkManager(download_config, downloader, storage, logger)

    # Start chunks
    chunk_manager.start_chunk(ChunkRange(0, 51199))
    chunk_manager.start_chunk(ChunkRange(51200, 102399))

    # Wait a bit then cancel all
    await asyncio.sleep(0.5)
    await chunk_manager.cancel_all_chunks()

    # Verify no active chunks
    assert len(chunk_manager.get_active_chunks()) == 0

    # Check stats for cancelled
    all_stats = chunk_manager.get_all_chunk_stats()
    for key, stat in all_stats.items():
        assert stat.status in [
            EDownloadStatus.CANCELLED,
            EDownloadStatus.DOWNLOADING,
        ]  # Might be downloading if not yet cancelled

    await chunk_manager.cleanup_temp_files()
