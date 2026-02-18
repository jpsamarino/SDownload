import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from sDownload.services.downloader_manager.chunk_utils.downloader import (
    download_chunk_supervised,
)
from sDownload.services.downloader_manager.throttling import get_default_throttler
from sDownload.interfaces.models import (
    ChunkRange,
    ChunkDownloadStats,
    EDownloadStatus,
)


@pytest.fixture
def mock_downloader():
    return MagicMock()


@pytest.fixture
def mock_storage():
    storage = MagicMock()

    async def fake_save(name, it):
        async for _ in it:
            pass

    storage.save_binary_data = AsyncMock(side_effect=fake_save)
    return storage


@pytest.fixture
def chunk_range():
    return ChunkRange(0, 99)


@pytest.fixture
def stats(chunk_range):
    return ChunkDownloadStats(
        chunk_file_name="test_chunk.bin", range=chunk_range, file_size=None
    )


@pytest.fixture
def throttler():
    return get_default_throttler()


async def async_gen(data_chunks):
    for chunk in data_chunks:
        yield chunk
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_download_chunk_success(mock_downloader, mock_storage, stats, throttler):
    """Scenario 1: Successful Download"""
    download_url = "http://example.com/file"
    data = [b"chunk1", b"chunk2"]
    mock_downloader.download_chunk.return_value = async_gen(data)

    result = await download_chunk_supervised(
        mock_downloader, mock_storage, stats, download_url, throttler
    )

    assert result == stats.range
    assert stats.status == EDownloadStatus.COMPLETED
    assert stats.bytes_downloaded == 12
    mock_storage.save_binary_data.assert_called_once()


@pytest.mark.asyncio
async def test_download_chunk_size_mismatch(
    mock_downloader, mock_storage, stats, throttler
):
    """Scenario 2: Partial Download Error (Size Mismatch)"""
    download_url = "http://example.com/file"
    # Set expected size to 100
    stats.file_size = 100
    # But we only provide 10
    data = [b"1234567890"]
    mock_downloader.download_chunk.return_value = async_gen(data)

    with pytest.raises(IOError, match="Chunk size error"):
        await download_chunk_supervised(
            mock_downloader, mock_storage, stats, download_url, throttler
        )

    assert stats.status == EDownloadStatus.ERROR


@pytest.mark.asyncio
async def test_download_chunk_cancellation_simple(
    mock_downloader, mock_storage, stats, throttler
):
    """Scenario 3: Simple Cancellation"""
    download_url = "http://example.com/file"

    async def cancelling_gen():
        yield b"some data"
        raise asyncio.CancelledError()

    mock_downloader.download_chunk.return_value = cancelling_gen()

    with pytest.raises(asyncio.CancelledError):
        await download_chunk_supervised(
            mock_downloader, mock_storage, stats, download_url, throttler
        )

    assert stats.status == EDownloadStatus.CANCELLED


@pytest.mark.asyncio
async def test_download_chunk_cancellation_goal_reached(
    mock_downloader, mock_storage, stats, throttler
):
    """Scenario 4: Cancellation - Goal Reached (Succession)"""
    download_url = "http://example.com/file"
    stats.limit_qt_bytes = 5

    async def cancelling_gen():
        yield b"123456"  # More than limit
        raise asyncio.CancelledError()

    mock_downloader.download_chunk.return_value = cancelling_gen()

    with pytest.raises(asyncio.CancelledError):
        await download_chunk_supervised(
            mock_downloader, mock_storage, stats, download_url, throttler
        )

    assert stats.status == EDownloadStatus.DEPRECATED


@pytest.mark.asyncio
async def test_download_chunk_generic_error(
    mock_downloader, mock_storage, stats, throttler
):
    """Scenario 5: Network/Generic Error"""
    download_url = "http://example.com/file"

    async def error_gen():
        yield b"data"
        raise ValueError("Network timeout")

    mock_downloader.download_chunk.return_value = error_gen()

    with pytest.raises(ValueError, match="Network timeout"):
        await download_chunk_supervised(
            mock_downloader, mock_storage, stats, download_url, throttler
        )

    assert stats.status == EDownloadStatus.ERROR


@pytest.mark.asyncio
async def test_download_chunk_storage_full(
    mock_downloader, mock_storage, stats, throttler
):
    """Scenario 6: Storage Error (Disk Full)"""
    download_url = "http://example.com/file"
    mock_downloader.download_chunk.return_value = async_gen([b"data"])

    # Simulate storage error
    mock_storage.save_binary_data.side_effect = IOError("No space left on device")

    with pytest.raises(IOError, match="No space left on device"):
        await download_chunk_supervised(
            mock_downloader, mock_storage, stats, download_url, throttler
        )

    assert stats.status == EDownloadStatus.ERROR
