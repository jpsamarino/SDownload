import asyncio
import time

import pytest

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
)
from sDownload.services.downloader_manager.throttling.fixed_window import (
    FixedWindowThrottler,
)


@pytest.fixture
def stats():
    return ChunkDownloadStats(chunk_file_name="test.bin", range=ChunkRange(0, 1000), file_size=1001)


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

    data = [b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9", b"0"]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    # Expected sleep: 10 bytes / 5 bps = 2 seconds.
    total_time = end - start
    assert total_time >= 1.9
    assert total_time < 2.1


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


@pytest.mark.asyncio
async def test_fixed_window_max_sleep_cap(stats):
    """
    Verify that sleep is capped at max_sleep_seconds even if the debt is huge.
    Simulates a massive buffer dump followed by a slow connection.
    """
    stats.target_speed_bps = 100  # 100 bytes/s
    max_sleep = 0.5
    throttler = FixedWindowThrottler(max_sleep_seconds=max_sleep, min_chunk_size=1)

    # Send 1000 bytes. Expected sleep = 10s. Max sleep = 0.5s.
    data = [b"A" * 1000]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()
    elapsed = end - start

    # Validation: It should have slept at least max_sleep (0.5s)
    # but NOT the full expected 10s.
    assert elapsed >= max_sleep
    assert elapsed < 2.0  # Should be close to 0.5s, definitely not 10s.


@pytest.mark.asyncio
async def test_fixed_window_custom_sleep_cap(stats):
    """Verify custom max_sleep_seconds configuration. Using small cap to test precision."""
    stats.target_speed_bps = 100
    custom_cap = 0.2
    throttler = FixedWindowThrottler(max_sleep_seconds=custom_cap, min_chunk_size=1)

    # Huge chunk: 500 bytes -> 5s sleep expected
    data = [b"A" * 500]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    # Should sleep around 0.2s (+overhead), but definitely > 0.15s and < 1s
    assert (end - start) >= 0.15
    assert (end - start) < 0.5
