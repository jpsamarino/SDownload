import asyncio
import time

import pytest

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
)
from sDownload.services.downloader_manager.throttling.token_bucket import (
    TokenBucketThrottler,
)


@pytest.fixture
def stats():
    return ChunkDownloadStats(chunk_file_name="test.bin", range=ChunkRange(0, 1000), file_size=1001)


async def async_gen(data_list):
    for item in data_list:
        yield item
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_token_bucket_tracking(stats):
    """Verify that bytes are correctly tracked regardless of throttling."""
    throttler = TokenBucketThrottler()
    data = [b"12345", b"67890"]

    gen = throttler.wrap(async_gen(data), stats)

    results = []
    async for d in gen:
        results.append(d)

    assert results == data
    assert stats.bytes_downloaded == 10


@pytest.mark.asyncio
async def test_token_bucket_throttling_precision(stats):
    """
    Verify throttling precision with byte-by-byte data.
    10 bytes at 5 bps = 2 seconds total.
    Each byte consumes tokens; once depleted, sleep is triggered.
    """
    stats.target_speed_bps = 5
    throttler = TokenBucketThrottler(burst_seconds=0.0, min_chunk_size=1)

    # Byte-by-byte to force granular token consumption
    data = [b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9", b"0"]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    total_time = end - start
    # 10 bytes / 5 bps = 2 seconds. Allow some tolerance.
    assert total_time >= 1.8
    assert total_time < 2.5


@pytest.mark.asyncio
async def test_token_bucket_no_limit(stats):
    """Verify zero overhead when target_speed_bps is None."""
    stats.target_speed_bps = None
    throttler = TokenBucketThrottler()

    data = [b"A" * 100000]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    # Should be nearly instant (no throttling, no clock calls)
    assert (end - start) < 0.1


@pytest.mark.asyncio
async def test_token_bucket_max_sleep_cap(stats):
    """
    Verify that sleep is capped at max_sleep_seconds.
    Huge data dump with low speed should NOT cause a long sleep.
    """
    stats.target_speed_bps = 100
    max_sleep = 0.3
    throttler = TokenBucketThrottler(
        max_sleep_seconds=max_sleep, min_chunk_size=1, burst_seconds=0.0
    )

    # 1000 bytes at 100 bps = 10s expected sleep. Cap = 0.3s.
    data = [b"A" * 1000]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    elapsed = end - start
    # Should sleep at most max_sleep, definitely NOT 10s
    assert elapsed >= 0.2
    assert elapsed < 1.0


@pytest.mark.asyncio
async def test_token_bucket_dynamic_speed_change(stats):
    """
    Verify that the throttler adapts when target_speed changes mid-stream.
    Start unlimited, then switch to a limit mid-download.
    """
    stats.target_speed_bps = None  # Start unlimited
    throttler = TokenBucketThrottler(min_chunk_size=1, burst_seconds=0.0)

    received = []

    async def changing_gen():
        # Phase 1: 5 chunks while unlimited
        for _i in range(5):
            yield b"A"
            await asyncio.sleep(0)
        # Switch to limited mid-stream
        stats.target_speed_bps = 5  # 5 bps
        # Phase 2: 5 more chunks while limited
        for _i in range(5):
            yield b"B"
            await asyncio.sleep(0)

    start = time.monotonic()
    gen = throttler.wrap(changing_gen(), stats)
    async for chunk in gen:
        received.append(chunk)
    end = time.monotonic()

    total_time = end - start

    # Phase 1 (5 bytes unlimited) should be instant.
    # Phase 2 (5 bytes at 5 bps) should take ~1 second.
    # Total should be around 1 second, not 0 and not 2.
    assert len(received) == 10
    assert total_time >= 0.8
    assert total_time < 2.0


@pytest.mark.asyncio
async def test_token_bucket_burst_allows_initial_speed(stats):
    """
    Verify that burst_seconds gives initial token capacity,
    allowing data to flow without throttling up to the burst limit.
    """
    stats.target_speed_bps = 100
    # burst_seconds=1.0 means capacity = 100 tokens initially.
    # We refill tokens based on elapsed time, starting at 0.
    # But since we start with 0 tokens and the first check
    # adds elapsed*target tokens, a small burst should pass quickly.
    throttler = TokenBucketThrottler(burst_seconds=1.0, min_chunk_size=1)

    # Send 50 bytes. With burst capacity, should be fast.
    data = [b"A" * 50]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    # Should be very fast since we start under burst capacity
    assert (end - start) < 0.5
