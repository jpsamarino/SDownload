import asyncio
import logging
import pytest
from unittest.mock import MagicMock, patch
from sDownload.services.downloader_manager.chunk_utils.monitor import (
    monitor_download_progress,
)
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
    EDownloadStatus,
)
from sDownload.interfaces.protocols.chunk_models import ChunkRange


@pytest.fixture
def stats_factory():
    def _create(start, end, status=EDownloadStatus.DOWNLOADING):
        s = ChunkDownloadStats(
            chunk_file_name=f"chunk_{start}_{end}.bin",
            range=ChunkRange(start, end),
            file_size=end - start + 1 if end is not None else None,
        )
        s.set_status(status)
        s.speed_bps = 1024 * 1024  # 1 MB/s default for test
        s.update = (
            MagicMock()
        )  # Mock update to prevent it from resetting speed_bps in tight loops
        return s

    return _create


@pytest.mark.asyncio
async def test_monitor_basic_loop(stats_factory):
    """Scenario 1: Basic Monitoring with one chunk"""
    s1 = stats_factory(0, 100)
    chunks_stats = {s1.range: s1}

    with patch(
        "sDownload.services.downloader_manager.chunk_utils.monitor.logger"
    ) as mock_logger:
        monitor_task = asyncio.create_task(
            monitor_download_progress(chunks_stats, "test_file.bin", interval=0.01)
        )

        await asyncio.sleep(0.05)
        monitor_task.cancel()

        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        assert mock_logger.info.called
        # Verify it logged speed
        args, _ = mock_logger.info.call_args
        # args[0] is the format string, args[1] is file_name, args[2] is speed, args[3] is active_count
        assert args[2] == 1.0  # 1.0 MB/s
        assert args[3] == 1


@pytest.mark.asyncio
async def test_monitor_multiple_chunks(stats_factory):
    """Scenario 2: Total speed calculation with multiple chunks"""
    s1 = stats_factory(0, 100)
    s2 = stats_factory(101, 200)
    s1.speed_bps = 1024 * 1024  # 1 MB/s
    s2.speed_bps = 512 * 1024  # 0.5 MB/s

    chunks_stats = {s1.range: s1, s2.range: s2}

    with patch(
        "sDownload.services.downloader_manager.chunk_utils.monitor.logger"
    ) as mock_logger:
        monitor_task = asyncio.create_task(
            monitor_download_progress(chunks_stats, "test_file.bin", interval=0.01)
        )

        await asyncio.sleep(0.05)
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)

        assert mock_logger.info.called
        args, _ = mock_logger.info.call_args
        # Total should be 1.5 MB/s
        assert args[2] == 1.5
        assert args[3] == 2


@pytest.mark.asyncio
async def test_monitor_no_active_chunks(stats_factory):
    """Scenario 3: No active chunks means no speed log"""
    s1 = stats_factory(0, 100, status=EDownloadStatus.COMPLETED)
    chunks_stats = {s1.range: s1}

    with patch(
        "sDownload.services.downloader_manager.chunk_utils.monitor.logger"
    ) as mock_logger:
        monitor_task = asyncio.create_task(
            monitor_download_progress(chunks_stats, "test_file.bin", interval=0.01)
        )

        await asyncio.sleep(0.05)
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)

        # info should NOT be called because active_count == 0
        assert not mock_logger.info.called


@pytest.mark.asyncio
async def test_monitor_debug_logs(stats_factory):
    """Scenario 4: Debug logs are triggered when level is DEBUG"""
    s1 = stats_factory(0, 100)
    chunks_stats = {s1.range: s1}

    with patch(
        "sDownload.services.downloader_manager.chunk_utils.monitor.logger"
    ) as mock_logger:
        mock_logger.isEnabledFor.return_value = True  # Simulate DEBUG level

        monitor_task = asyncio.create_task(
            monitor_download_progress(chunks_stats, "test_file.bin", interval=0.01)
        )

        await asyncio.sleep(0.05)
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)

        assert mock_logger.debug.called
        # Check if it logged the chunk detail
        debug_args, _ = mock_logger.debug.call_args
        # debug_args[0] is format, [1] is start, [2] is end, [3] is progress, [4] is speed
        assert debug_args[1] == 0
        assert debug_args[2] == 100
