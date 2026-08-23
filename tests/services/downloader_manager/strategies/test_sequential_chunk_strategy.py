from sDownload.interfaces.models import (
    DownloadStats,
    StrategyAction,
)
from sDownload.services.downloader_manager.strategies.sequential_chunk_strategy import (
    SequentialChunkStrategy,
)


def test_sequential_chunk_strategy_lifecycle():
    strategy = SequentialChunkStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)  # 10MB
    chunks_stats = {}

    # 1. on_start with limited slots
    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=2)
    assert len(actions) == 2
    for action in actions:
        assert isinstance(action, StrategyAction.Start)

    assert len(strategy._pending_ranges) == 2

    # 2. on_update right after, slots exhausted
    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=0)
    assert len(actions) == 0
    assert len(strategy._pending_ranges) == 2

    # 3. on_update slots freed up
    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=1)
    assert len(actions) == 1
    assert len(strategy._pending_ranges) == 1

    # 4. on_update remaining slots freed up, more slots than ranges
    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=5)
    assert len(actions) == 1
    assert len(strategy._pending_ranges) == 0

    # 5. on_update no pending ranges
    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=5)
    assert len(actions) == 0
