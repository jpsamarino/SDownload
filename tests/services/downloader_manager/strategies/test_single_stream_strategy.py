from sDownload.interfaces.models import (
    ChunkRange,
    DownloadStats,
    StrategyAction,
)
from sDownload.services.downloader_manager.strategies.single_stream_strategy import (
    SingleStreamStrategy,
)


def test_single_stream_strategy_on_start():
    strategy = SingleStreamStrategy()
    dl_stats = DownloadStats(file_size=None)
    chunks_stats = {}

    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=4)

    assert len(actions) == 1
    action = actions[0]
    assert isinstance(action, StrategyAction.Start)
    assert action.range == ChunkRange(0, None)


def test_single_stream_strategy_on_start_already_started():
    strategy = SingleStreamStrategy()
    dl_stats = DownloadStats(file_size=0)
    chunks_stats = {}

    actions1 = strategy.on_start(dl_stats, chunks_stats, available_slots=1)
    assert len(actions1) == 1

    # Second call should not start another chunk
    actions2 = strategy.on_start(dl_stats, chunks_stats, available_slots=1)
    assert len(actions2) == 0


def test_single_stream_strategy_on_start_no_slots():
    strategy = SingleStreamStrategy()
    dl_stats = DownloadStats(file_size=1000)
    chunks_stats = {}

    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=0)
    assert len(actions) == 0


def test_single_stream_strategy_on_update_and_on_end():
    strategy = SingleStreamStrategy()
    dl_stats = DownloadStats(file_size=None)
    chunks_stats = {}

    assert strategy.on_update(dl_stats, chunks_stats, available_slots=1) == []
    # on_end is no-op
    strategy.on_end(dl_stats, chunks_stats)
