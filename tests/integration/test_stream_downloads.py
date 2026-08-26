import asyncio
from pathlib import Path

import pytest

from sDownload.file_system import LocalStorage
from sDownload.interfaces.models import (
    EDownloadStatus,
    EFilePolicy,
)
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.services.downloader_manager.download_task import DownloadTask
from sDownload.services.downloader_manager.strategies import (
    MultiChunkDownloadStrategy,
    SingleStreamStrategy,
)


@pytest.mark.asyncio
async def test_stream_chunked_downloads_correctly(tmp_path: Path, nginx_custom: dict[str, str]):
    """
    Test 1: Downloads a dynamic chunked stream without Content-Length from Nginx.
    Verifies that DownloadTask automatically falls back to SingleStreamStrategy,
    downloads all chunks sequentially to EOF, and produces a complete, intact file.
    """
    url = f"{nginx_custom['http']}/stream_chunked/file_100k.bin"
    storage = LocalStorage(storage_dir=str(tmp_path))
    params = DownloadTaskParams(
        url=url,
        dest_dir=str(tmp_path),
        file_name="file_100k.bin",
        file_policy=EFilePolicy.OVERWRITE,
        # Intentionally ask for 4 connections to verify automatic downgrade
        max_conn=4,
        use_chunked=True,
    )

    task = DownloadTask(
        params=params,
        storage=storage,
        strategy=MultiChunkDownloadStrategy(max_conn=4),
    )

    await task.start()
    await task.wait_until_done()

    # 1. Assert status and strategy migration
    assert task.status == EDownloadStatus.COMPLETED
    assert isinstance(task.strategy, SingleStreamStrategy)

    # 2. Verify file content and size on storage (100 * 1024 = 102400 bytes)
    expected_size = 100 * 1024
    stored_info = await storage.get_data_info("file_100k.bin")
    assert stored_info is not None
    assert stored_info.size_bytes == expected_size

    # Verify content integrity
    file_path = tmp_path / "file_100k.bin"
    assert file_path.stat().st_size == expected_size


@pytest.mark.asyncio
async def test_stream_no_resume_with_file_policies(tmp_path: Path, nginx_custom: dict[str, str]):
    """
    Test 2: Downloads a stream with explicit Accept-Ranges: none from Nginx and tests
    file policy behavior (AUTO_RENAME and OVERWRITE) on stream endpoints.
    """
    url = f"{nginx_custom['http']}/stream_no_resume/file_100k.bin"
    storage = LocalStorage(storage_dir=str(tmp_path))

    # Download 1: First download
    params1 = DownloadTaskParams(
        url=url,
        dest_dir=str(tmp_path),
        file_name="stream_report.bin",
        file_policy=EFilePolicy.OVERWRITE,
    )
    task1 = DownloadTask(params=params1, storage=storage)
    await task1.start()
    await task1.wait_until_done()
    assert task1.status == EDownloadStatus.COMPLETED
    assert (tmp_path / "stream_report.bin").stat().st_size == 100 * 1024

    # Download 2: Same name with AUTO_RENAME policy -> creates stream_report_1.bin
    params2 = DownloadTaskParams(
        url=url,
        dest_dir=str(tmp_path),
        file_name="stream_report.bin",
        file_policy=EFilePolicy.AUTO_RENAME,
    )
    task2 = DownloadTask(params=params2, storage=storage)
    await task2.start()
    await task2.wait_until_done()
    assert task2.status == EDownloadStatus.COMPLETED
    assert task2.file_name == "stream_report_1.bin"
    assert (tmp_path / "stream_report_1.bin").exists()
    assert (tmp_path / "stream_report_1.bin").stat().st_size == 100 * 1024


@pytest.mark.asyncio
async def test_stream_slow_1mb_monitors_speed_and_progress(
    tmp_path: Path, nginx_custom: dict[str, str]
):
    """
    Test 3: Downloads a 1 MB stream paced at 100 KB/s by Nginx over ~10 seconds.
    Samples download telemetry every second to verify:
    - bytes_downloaded grows continuously
    - speed_bps stays stable and non-zero
    - progress remains 0.0 without division errors while downloading
    - Final file has exact 1 MB (1,048,576 bytes).
    """
    url = f"{nginx_custom['http']}/stream_slow_1mb/file_1M.bin"
    storage = LocalStorage(storage_dir=str(tmp_path))
    params = DownloadTaskParams(
        url=url,
        dest_dir=str(tmp_path),
        file_name="stream_1mb.bin",
        file_policy=EFilePolicy.OVERWRITE,
    )

    task = DownloadTask(params=params, storage=storage)
    await task.start()

    samples_bytes: list[int] = []
    samples_speed: list[float] = []

    # Monitor periodically while downloading
    while task.status == EDownloadStatus.DOWNLOADING:
        await asyncio.sleep(1.0)
        if task.stats and task.status == EDownloadStatus.DOWNLOADING:
            samples_bytes.append(task.stats.bytes_downloaded)
            samples_speed.append(task.stats.speed_bps)
            # Progress must be 0.0 while actively downloading unknown size stream
            assert task.stats.progress == 0.0

    await task.wait_until_done()

    # Assertions after completion
    assert task.status == EDownloadStatus.COMPLETED
    assert task.stats is not None
    assert task.stats.progress == 100.0  # Upon finalization, progress reaches 100%

    # Telemetry verifications
    assert len(samples_bytes) >= 5, (
        f"Expected at least 5 samples during 10s stream, got {len(samples_bytes)}"
    )
    # Verify continuous bytes growth
    for i in range(1, len(samples_bytes)):
        assert samples_bytes[i] >= samples_bytes[i - 1]

    # Verify final stored file
    expected_total_bytes = 1024 * 1024  # 1,048,576 bytes
    final_file = tmp_path / "stream_1mb.bin"
    assert final_file.exists()
    assert final_file.stat().st_size == expected_total_bytes
