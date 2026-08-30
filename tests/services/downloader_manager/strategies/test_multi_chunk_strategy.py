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

    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=10)

    assert len(actions) == 4
    for action in actions:
        assert isinstance(action, StrategyAction.Start)

    # Verify continuous ranges
    ranges = [a.range for a in actions]
    assert ranges[0].start == 0
    assert ranges[-1].end is None or ranges[-1].end == dl_stats.file_size - 1


def test_multi_chunk_strategy_on_start_limited_slots():
    strategy = MultiChunkDownloadStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)
    chunks_stats = {}

    # The strategy wants 4 connections, but the manager only allows 2
    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=2)

    assert len(actions) == 2
    for action in actions:
        assert isinstance(action, StrategyAction.Start)


def test_multi_chunk_strategy_on_start_with_cache():
    # If cache is provided, it should use those ranges (simplified check as calculate_ranges is tested elsewhere)
    cache = [ChunkRange(0, 1000), ChunkRange(1001, 2000)]
    strategy = MultiChunkDownloadStrategy(max_conn=2, cache=cache)
    dl_stats = DownloadStats(file_size=5000)
    chunks_stats = {}

    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=2)

    # MultiChunkDownloadStrategy with cache might produce more chunks if cache is partial,
    # but here we just want to see if it respects the logic.
    assert len(actions) >= 2


def test_multi_chunk_strategy_on_start_already_running():
    strategy = MultiChunkDownloadStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)
    # Fully completed cache: all ranges already covered
    full_cache_stats = {ChunkRange(0, 10 * 1024 * 1024 - 1): None}

    actions = strategy.on_start(dl_stats, full_cache_stats, available_slots=4)
    assert actions == []

    # Partial cache: missing gap from 101 to EOF is started
    partial_cache_stats = {ChunkRange(0, 100): None}
    partial_actions = strategy.on_start(dl_stats, partial_cache_stats, available_slots=4)
    assert len(partial_actions) > 0
    assert partial_actions[0].range.start == 101


def test_multi_chunk_strategy_on_update():
    strategy = MultiChunkDownloadStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)
    chunks_stats = {}

    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=4)
    assert actions == []
