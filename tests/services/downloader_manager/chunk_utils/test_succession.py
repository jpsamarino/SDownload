import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from sDownload.services.downloader_manager.chunk_utils.succession import (
    run_chunk_succession,
)
from sDownload.interfaces.models import (
    ChunkRange,
    ChunkDownloadStats,
    EDownloadStatus,
)


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.crop_file = AsyncMock()
    storage.move_data = AsyncMock()
    return storage


@pytest.fixture
def stats_pre():
    stats = ChunkDownloadStats(
        chunk_file_name="chunk_A.bin", range=ChunkRange(0, 100), file_size=101
    )
    # bytes_downloaded is set in tests
    return stats


@pytest.fixture
def stats_succ():
    return ChunkDownloadStats(
        chunk_file_name="chunk_B.bin",
        range=ChunkRange(50, 100),
        file_size=51,
        status=EDownloadStatus.AWAITING_SUCCESSION,
    )


@pytest.mark.asyncio
async def test_succession_success(mock_storage, stats_pre, stats_succ):
    """Scenario 1: Successful Succession"""
    range_succ = stats_succ.range

    # Predecessor reached goal and was cancelled
    stats_pre.limit_qt_bytes = 51
    stats_pre.bytes_downloaded = 55

    # Use a real Future to avoid issues with asyncio.wait
    pre_task = asyncio.Future()
    pre_task.set_exception(asyncio.CancelledError())

    result = await run_chunk_succession(mock_storage, stats_pre, stats_succ, pre_task)

    assert result == range_succ
    assert stats_succ.status == EDownloadStatus.COMPLETED
    assert stats_pre.status == EDownloadStatus.DEPRECATED
    assert stats_succ.bytes_downloaded == 51  # 100 - 50 + 1

    mock_storage.crop_file.assert_called_once_with("chunk_A.bin", 50, 100)
    mock_storage.move_data.assert_called_once_with("chunk_A.bin", "chunk_B.bin")


@pytest.mark.asyncio
async def test_succession_predecessor_failed(mock_storage, stats_pre, stats_succ):
    """Scenario 2: Predecessor Task Failed"""
    pre_task = asyncio.Future()
    pre_task.set_exception(ValueError("Network Error"))

    with pytest.raises(RuntimeError, match="Predecessor failed: Network Error"):
        await run_chunk_succession(
            mock_storage,
            stats_pre,
            stats_succ,
            pre_task,
        )

    assert stats_succ.status == EDownloadStatus.ERROR


@pytest.mark.asyncio
async def test_succession_insufficient_data(mock_storage, stats_pre, stats_succ):
    """Scenario 3: Insufficient Data"""
    # Predecessor task finished but didn't reach limit
    stats_pre.limit_qt_bytes = 60
    stats_pre.bytes_downloaded = 50

    pre_task = asyncio.Future()
    pre_task.set_result(None)

    with pytest.raises(RuntimeError, match="Insufficient data"):
        await run_chunk_succession(
            mock_storage,
            stats_pre,
            stats_succ,
            pre_task,
        )

    assert stats_succ.status == EDownloadStatus.ERROR


@pytest.mark.asyncio
async def test_succession_cancelled(mock_storage, stats_pre, stats_succ):
    """Scenario 4: Succession Cancellation"""
    # Predecessor must have downloaded bytes so crop_file is attempted
    stats_pre.bytes_downloaded = 60

    # Mocking wait to raise cancellation for the succession itself
    with pytest.raises(asyncio.CancelledError):
        # We need a way to trigger cancellation mid-succession.
        # Let's mock storage.crop_file to raise it.
        mock_storage.crop_file.side_effect = asyncio.CancelledError()

        await run_chunk_succession(mock_storage, stats_pre, stats_succ, None)

    assert stats_succ.status == EDownloadStatus.CANCELLED


@pytest.mark.asyncio
async def test_succession_storage_error(mock_storage, stats_pre, stats_succ):
    """Scenario 5: Storage Error"""
    stats_pre.limit_qt_bytes = 51
    stats_pre.bytes_downloaded = 60

    mock_storage.move_data.side_effect = IOError("Disk full")

    with pytest.raises(IOError, match="Disk full"):
        await run_chunk_succession(mock_storage, stats_pre, stats_succ, None)

    assert stats_succ.status == EDownloadStatus.ERROR


@pytest.mark.asyncio
async def test_succession_init_signal(mock_storage, stats_pre, stats_succ):
    """Scenario 6: Verify init_signal is set immediately"""
    init_signal = asyncio.Event()
    stats_pre.bytes_downloaded = 55

    pre_task = asyncio.Future()
    pre_task.set_result(None)

    await run_chunk_succession(
        mock_storage, stats_pre, stats_succ, pre_task, init_signal=init_signal
    )

    assert init_signal.is_set()


@pytest.mark.asyncio
async def test_succession_invalid_successor_state(mock_storage, stats_pre, stats_succ):
    """Scenario 7: Successor in invalid state raises RuntimeError"""
    stats_succ.set_status(EDownloadStatus.PENDING)

    with pytest.raises(
        RuntimeError, match="Successor is not in AWAITING_SUCCESSION state"
    ):
        await run_chunk_succession(mock_storage, stats_pre, stats_succ, None)


@pytest.mark.asyncio
async def test_succession_predecessor_zero_bytes(mock_storage, stats_pre, stats_succ):
    """Scenario 8: Predecessor has 0 bytes -> Raises RuntimeError"""
    stats_pre.bytes_downloaded = 0
    stats_pre.limit_qt_bytes = 0

    pre_task = asyncio.Future()
    pre_task.set_exception(asyncio.CancelledError())

    with pytest.raises(RuntimeError, match="provided no data"):
        await run_chunk_succession(mock_storage, stats_pre, stats_succ, pre_task)

    assert stats_succ.status == EDownloadStatus.ERROR
    # No file operations should have been called
    mock_storage.crop_file.assert_not_called()
    mock_storage.move_data.assert_not_called()


@pytest.mark.asyncio
async def test_succession_predecessor_file_missing(mock_storage, stats_pre, stats_succ):
    """Scenario 9: Predecessor reports bytes but file is missing -> Raises FileNotFoundError -> status ERROR"""
    stats_pre.bytes_downloaded = 60
    stats_pre.limit_qt_bytes = 51

    mock_storage.crop_file.side_effect = FileNotFoundError("file not found")

    pre_task = asyncio.Future()
    pre_task.set_exception(asyncio.CancelledError())

    with pytest.raises(FileNotFoundError):
        await run_chunk_succession(mock_storage, stats_pre, stats_succ, pre_task)

    assert stats_succ.status == EDownloadStatus.ERROR
