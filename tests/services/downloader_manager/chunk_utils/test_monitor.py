import asyncio
import contextlib
from unittest.mock import patch

import pytest

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
    EDownloadStatus,
)
from sDownload.services.downloader_manager.chunk_utils.monitor import (
    monitor_download_progress,
)


class MockStats(ChunkDownloadStats):
    __slots__ = ()

    def update(self):
        """No-op update for tests to prevent speed_bps reset."""
        pass


@pytest.fixture
def stats_factory():
    def _create(start, end, status=EDownloadStatus.DOWNLOADING):
        s = MockStats(
            chunk_file_name=f"chunk_{start}_{end}.bin",
            range=ChunkRange(start, end),
            file_size=end - start + 1 if end is not None else None,
        )
        s.set_status(status)
        s.speed_bps = 1024 * 1024  # 1 MB/s default for test
        return s

    return _create


@pytest.mark.asyncio
async def test_monitor_basic_loop(stats_factory):
    """Scenario 1: Basic Monitoring with one chunk"""
    s1 = stats_factory(0, 100)
    chunks_stats = {s1.range: s1}

    with patch("sDownload.services.downloader_manager.chunk_utils.monitor.logger") as mock_logger:
        monitor_task = asyncio.create_task(
            monitor_download_progress(chunks_stats, "test_file.bin", interval=0.01)
        )

        await asyncio.sleep(0.05)
        monitor_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task

        assert mock_logger.info.called
        # Verify it logged speed
        args, _ = mock_logger.info.call_args
        # args[0] is the format string, args[1] is file_name, args[2] is speed, args[3] is downloading_count, args[4] is pending_count
        assert args[2] == 1.0  # 1.0 MB/s
        assert args[3] == 1
        assert args[4] == 0


@pytest.mark.asyncio
async def test_monitor_multiple_chunks(stats_factory):
    """Scenario 2: Total speed calculation with multiple chunks"""
    s1 = stats_factory(0, 100)
    s2 = stats_factory(101, 200)
    s1.speed_bps = 1024 * 1024  # 1 MB/s
    s2.speed_bps = 512 * 1024  # 0.5 MB/s

    chunks_stats = {s1.range: s1, s2.range: s2}

    with patch("sDownload.services.downloader_manager.chunk_utils.monitor.logger") as mock_logger:
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
        assert args[4] == 0


@pytest.mark.asyncio
async def test_monitor_no_active_chunks(stats_factory):
    """Scenario 3: No active chunks means no speed log"""
    s1 = stats_factory(0, 100, status=EDownloadStatus.COMPLETED)
    chunks_stats = {s1.range: s1}

    with patch("sDownload.services.downloader_manager.chunk_utils.monitor.logger") as mock_logger:
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

    with patch("sDownload.services.downloader_manager.chunk_utils.monitor.logger") as mock_logger:
        mock_logger.isEnabledFor.return_value = True  # Simulate DEBUG level

        monitor_task = asyncio.create_task(
            monitor_download_progress(chunks_stats, "test_file.bin", interval=0.01)
        )

        await asyncio.sleep(0.05)
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)

        assert mock_logger.debug.called
        # Check if it logged the chunk detail in any of the debug calls
        debug_calls = [call.args for call in mock_logger.debug.call_args_list]
        chunk_detail_logged = any(len(args) > 1 and args[1] == 0 for args in debug_calls)
        assert chunk_detail_logged


@pytest.mark.asyncio
async def test_monitor_auto_stop_on_completion(stats_factory):
    """Scenario 5: Monitor should stop itself when all chunks are COMPLETED"""
    s1 = stats_factory(0, 100, status=EDownloadStatus.DOWNLOADING)
    chunks_stats = {s1.range: s1}

    # Start monitor
    monitor_task = asyncio.create_task(
        monitor_download_progress(chunks_stats, "auto_stop_test", interval=0.02)
    )

    await asyncio.sleep(0.1)
    assert not monitor_task.done()

    # Mark as completed
    s1.set_status(EDownloadStatus.COMPLETED)

    # Wait for monitor to detect and stop
    await asyncio.wait_for(monitor_task, timeout=0.1)
    assert monitor_task.done()


@pytest.mark.asyncio
async def test_monitor_auto_stop_on_empty_stats():
    """Scenario 6: Monitor should stop itself when chunks_stats is empty"""
    chunks_stats = {}

    # Start monitor
    monitor_task = asyncio.create_task(
        monitor_download_progress(chunks_stats, "empty_stop_test", interval=0.01)
    )

    # Should stop almost immediately
    await asyncio.wait_for(monitor_task, timeout=0.1)
    assert monitor_task.done()


@pytest.mark.asyncio
async def test_monitor_stays_alive_on_pending_only(stats_factory):
    """Scenario 7: Monitor should stay active but NOT log when only PENDING chunks exist"""
    s1 = stats_factory(0, 100, status=EDownloadStatus.PENDING)
    chunks_stats = {s1.range: s1}

    with patch("sDownload.services.downloader_manager.chunk_utils.monitor.logger") as mock_logger:
        monitor_task = asyncio.create_task(
            monitor_download_progress(chunks_stats, "pending_test", interval=0.01)
        )

        await asyncio.sleep(0.05)

        # Should still be running
        assert not monitor_task.done()

        # info should NOT be called because active_count > 0 but downloading_count == 0
        assert not mock_logger.info.called

        # Now switch to downloading
        s1.set_status(EDownloadStatus.DOWNLOADING)
        await asyncio.sleep(0.05)

        # Now it should have logged
        assert mock_logger.info.called

        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)
