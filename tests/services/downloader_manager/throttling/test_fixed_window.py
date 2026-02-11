import asyncio
import time
import pytest
from sDownload.services.downloader_manager.throttling.fixed_window import (
    FixedWindowThrottler,
)
from sDownload.services.downloader_manager.download_stats_models import (
    ChunkDownloadStats,
)
from sDownload.interfaces.protocols.chunk_models import ChunkRange


@pytest.fixture
def stats():
    return ChunkDownloadStats(
        chunk_file_name="test.bin", range=ChunkRange(0, 1000), file_size=1001
    )


async def async_gen(data_list):
    for item in data_list:
        yield item
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_fixed_window_tracking(stats):
    """Verify that it correctly tracks downloaded bytes."""
    throttler = FixedWindowThrottler()
    data = [b"12345", b"67890"]

    gen = throttler.wrap(async_gen(data), stats)

    results = []
    async for d in gen:
        results.append(d)

    assert results == data
    assert stats.bytes_downloaded == 10


@pytest.mark.asyncio
async def test_fixed_window_throttling(stats):
    """Verify that it sleeps when the limit is exceeded."""
    stats.target_speed_bps = 5  # 5 bytes per second
    throttler = FixedWindowThrottler(min_chunk_size=1)

    # Send 10 bytes immediately
    data = [b"1234567890"]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    # Expected sleep: 10 bytes / 5 bps = 2 seconds.
    # Since it checks AFTER yielding, it should sleep after the 10th byte.
    assert (end - start) >= 1.5  # Allow for some overhead/jitter


@pytest.mark.asyncio
async def test_fixed_window_no_limit(stats):
    """Verify that it doesn't sleep when the limit is infinity."""
    stats.target_speed_bps = float("inf")
    throttler = FixedWindowThrottler()

    data = [b"A" * 100]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    assert (end - start) < 0.5  # Should be very fast
