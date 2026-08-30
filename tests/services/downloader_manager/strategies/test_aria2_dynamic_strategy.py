from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
    DownloadStats,
    EDownloadStatus,
    StrategyAction,
)
from sDownload.services.downloader_manager.strategies.aria2_dynamic_strategy import (
    Aria2DynamicStrategy,
)


def make_stat(
    start: int,
    end: int | None,
    bytes_downloaded: int,
    status: EDownloadStatus = EDownloadStatus.DOWNLOADING,
) -> ChunkDownloadStats:
    total_bytes = (end - start + 1) if end is not None else None
    return ChunkDownloadStats(
        chunk_file_name=f"{start}_{end or 'EOF'}.sdownload",
        range=ChunkRange(start=start, end=end),
        file_size=total_bytes,
        bytes_downloaded=bytes_downloaded,
        status=status,
    )


def test_aria2_strategy_starts_with_single_connection():
    """Validates that aria2 strategy starts with a single connection covering the entire file."""
    strategy = Aria2DynamicStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=100 * 1024 * 1024)  # 100MB
    chunks_stats = {}

    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=4)

    assert len(actions) == 1
    assert isinstance(actions[0], StrategyAction.Start)
    assert actions[0].range == ChunkRange(0, 100 * 1024 * 1024 - 1)


def test_aria2_strategy_splits_when_slots_available():
    """
    Scenario:
    File: 100 MB.
    Chunk 0: [0, 99_999_999], currently downloaded 20 MB (starts at 0, has downloaded 20MB -> pos=20MB).
    Remaining: 80 MB.
    Available slots: 1.
    Min split size: 2 MB.
    Since 80MB >= 2 * 2MB, strategy splits the unwritten 80MB in half:
    - 40MB for first half -> new range [0, 59_999_999].
    - 40MB for second half -> new chunk [60_000_000, 99_999_999].
    """
    min_split = 2 * 1024 * 1024  # 2MB
    strategy = Aria2DynamicStrategy(max_conn=4, min_split_size=min_split)
    dl_stats = DownloadStats(file_size=100 * 1024 * 1024)

    total_bytes = 100 * 1024 * 1024
    downloaded_bytes = 20 * 1024 * 1024
    chunk_0 = make_stat(
        0,
        total_bytes - 1,
        downloaded_bytes,
        status=EDownloadStatus.DOWNLOADING,
    )
    chunks_stats = {chunk_0.range: chunk_0}

    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=1)

    assert len(actions) == 2
    resize_action, start_action = actions[0], actions[1]

    assert isinstance(resize_action, StrategyAction.Resize)
    assert resize_action.current_range == ChunkRange(0, total_bytes - 1)
    assert resize_action.new_range.start == 0

    assert isinstance(start_action, StrategyAction.Start)
    assert start_action.range.start == resize_action.new_range.end + 1
    assert start_action.range.end == total_bytes - 1


def test_aria2_strategy_respects_min_split_size_rule():
    """
    Scenario:
    File: 10 MB.
    Chunk 0: [0, 9_999_999], downloaded 7 MB (3 MB remaining).
    Min split size: 2 MB.
    Rule: remaining (3MB) < 2 * min_split_size (4MB) -> NO split!
    """
    min_split = 2 * 1024 * 1024  # 2MB
    strategy = Aria2DynamicStrategy(max_conn=4, min_split_size=min_split)
    dl_stats = DownloadStats(file_size=10 * 1024 * 1024)

    chunk_0 = make_stat(
        0,
        10 * 1024 * 1024 - 1,
        7 * 1024 * 1024,
        status=EDownloadStatus.DOWNLOADING,
    )
    chunks_stats = {chunk_0.range: chunk_0}

    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=1)

    # Must NOT split because remaining 3MB < 4MB (2 * min_split_size)
    assert actions == []


def test_aria2_strategy_picks_largest_remaining_chunk():
    """
    Scenario:
    Two chunks downloading concurrently:
    Chunk A: [0, 49_999_999], downloaded 10 MB -> remaining 40 MB.
    Chunk B: [50_000_000, 99_999_999], downloaded 40 MB -> remaining 10 MB.
    Available slots: 1.
    Min split size: 2 MB.
    Expected: Splits Chunk A (the largest remaining), leaves Chunk B untouched.
    """
    min_split = 2 * 1024 * 1024
    strategy = Aria2DynamicStrategy(max_conn=4, min_split_size=min_split)
    dl_stats = DownloadStats(file_size=100 * 1024 * 1024)

    chunk_a = make_stat(0, 49_999_999, 10 * 1024 * 1024, status=EDownloadStatus.DOWNLOADING)
    chunk_b = make_stat(
        50_000_000,
        99_999_999,
        40 * 1024 * 1024,
        status=EDownloadStatus.DOWNLOADING,
    )
    chunks_stats = {chunk_a.range: chunk_a, chunk_b.range: chunk_b}

    actions = strategy.on_update(dl_stats, chunks_stats, available_slots=1)

    assert len(actions) == 2
    resize_action = actions[0]
    assert resize_action.current_range == ChunkRange(0, 49_999_999)


def test_aria2_strategy_streaming_mode():
    """Validates that for unknown file size (streaming), starts with [0, None] and never splits."""
    strategy = Aria2DynamicStrategy(max_conn=4)
    dl_stats = DownloadStats(file_size=None)
    chunks_stats = {}

    actions = strategy.on_start(dl_stats, chunks_stats, available_slots=4)
    assert len(actions) == 1
    assert actions[0].range == ChunkRange(0, None)

    stream_stat = make_stat(0, None, 50_000_000, status=EDownloadStatus.DOWNLOADING)
    update_actions = strategy.on_update(
        dl_stats, {stream_stat.range: stream_stat}, available_slots=2
    )
    assert update_actions == []
