import asyncio
import time
import pytest
from sDownload.services.downloader_manager.throttling.token_bucket import (
    TokenBucketThrottler,
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
async def test_token_bucket_tracking(stats):
    """Verify that it correctly tracks downloaded bytes."""
    throttler = TokenBucketThrottler()
    data = [b"12345", b"67890"]

    gen = throttler.wrap(async_gen(data), stats)

    results = []
    async for d in gen:
        results.append(d)

    assert results == data
    assert stats.bytes_downloaded == 10


@pytest.mark.asyncio
async def test_token_bucket_burst(stats):
    """Verify that it allows bursts if tokens are available."""
    stats.target_speed_bps = 100
    throttler = TokenBucketThrottler(burst_seconds=1.0)  # Starts with 100 tokens

    # Send 50 bytes. Since we start with 100 tokens, it should be instant.
    data = [b"A" * 50]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    assert (end - start) < 0.2  # No throttling for burst within capacity


@pytest.mark.asyncio
async def test_token_bucket_throttling(stats):
    """Verify that it throttles after tokens are exhausted."""
    stats.target_speed_bps = 10
    throttler = TokenBucketThrottler(burst_seconds=0.1)  # Starts with 1 token

    # Send 11 bytes.
    # 1 token used immediately. 10 more needed -> 1 second wait.
    data = [b"12345678901"]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    assert (end - start) >= 0.9


@pytest.mark.asyncio
async def test_token_bucket_no_limit(stats):
    """Verify that it doesn't sleep when the limit is infinity."""
    stats.target_speed_bps = float("inf")
    throttler = TokenBucketThrottler()

    data = [b"A" * 100]

    start = time.monotonic()
    gen = throttler.wrap(async_gen(data), stats)
    async for _ in gen:
        pass
    end = time.monotonic()

    assert (end - start) < 0.5
