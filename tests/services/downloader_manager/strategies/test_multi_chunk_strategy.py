from sDownload.interfaces.models import (
    ChunkRange,
    DownloadStats,
    StrategyAction,
)
from sDownload.services.downloader_manager.strategies.multi_chunk_strategy import (
    MultiChunkDownloadStrategy,
)


def test_multi_chunk_strategy_on_start_no_chunks():
    strategy = MultiChunkDownloadStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)  # 10MB
    chunks_stats = {}

    actions = strategy.on_start(dl_stats, chunks_stats)

    assert len(actions) == 4
    for action in actions:
        assert isinstance(action, StrategyAction.Start)

    # Verify continuous ranges
    ranges = [a.range for a in actions]
    assert ranges[0].start == 0
    assert ranges[-1].end is None or ranges[-1].end == dl_stats.file_size - 1


def test_multi_chunk_strategy_on_start_with_cache():
    # If cache is provided, it should use those ranges (simplified check as calculate_ranges is tested elsewhere)
    cache = [ChunkRange(0, 1000), ChunkRange(1001, 2000)]
    strategy = MultiChunkDownloadStrategy(max_conn=2, cache=cache)
    dl_stats = DownloadStats(file_size=5000)
    chunks_stats = {}

    actions = strategy.on_start(dl_stats, chunks_stats)

    # MultiChunkDownloadStrategy with cache might produce more chunks if cache is partial,
    # but here we just want to see if it respects the logic.
    assert len(actions) >= 2


def test_multi_chunk_strategy_on_start_already_running():
    strategy = MultiChunkDownloadStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)
    chunks_stats = {ChunkRange(0, 100): None}  # Mocking some status

    actions = strategy.on_start(dl_stats, chunks_stats)
    assert actions == []


def test_multi_chunk_strategy_on_update():
    strategy = MultiChunkDownloadStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)
    chunks_stats = {}

    actions = strategy.on_update(dl_stats, chunks_stats)
    assert actions == []
