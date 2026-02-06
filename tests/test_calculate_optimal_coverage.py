import pytest
from sDownload.utils.range_operations import calculate_optimal_coverage, ChunkFragment
from sDownload.interfaces.protocols.chunk_models import ChunkRange


def test_resolve_perfect_sequential_no_overlaps():
    # C1: 0-99, C2: 100-199
    chunks = [
        ChunkRange(0, 99),
        ChunkRange(100, 199),
    ]
    result = calculate_optimal_coverage(chunks, file_size=200)

    assert len(result) == 2
    # C1 used fully, so read_limit_qt_bytes = None
    assert result[0] == ChunkFragment(range=chunks[0], read_limit_qt_bytes=None)
    # C2 used fully, so read_limit_qt_bytes = None
    assert result[1] == ChunkFragment(range=chunks[1], read_limit_qt_bytes=None)


def test_resolve_with_overlap_chooses_shortest_path():
    # C1: 0-100
    # C2: 50-150
    # C3: 101-200
    c1, c2, c3 = ChunkRange(0, 100), ChunkRange(50, 150), ChunkRange(101, 200)
    chunks = [c1, c2, c3]

    # Target 201 bytes (0-200)
    result = calculate_optimal_coverage(chunks, file_size=201)

    # Shortest path is C1 -> C3 (2 chunks)
    assert len(result) == 2
    assert result[0] == ChunkFragment(range=c1, read_limit_qt_bytes=None)
    assert result[1] == ChunkFragment(range=c3, read_limit_qt_bytes=None)


def test_resolve_forces_tail_cut_to_avoid_head_cut():
    # C1: 0-100
    # C2: 80-200
    # Since C2 starts at 80, we MUST cut C1 at index 79 (80 bytes)
    c1, c2 = ChunkRange(0, 100), ChunkRange(80, None)
    result = calculate_optimal_coverage([c1, c2], file_size=201)

    assert len(result) == 2
    # To use C2 from its offset 0 (global byte 80), we take exactly 80 bytes from C1
    assert result[0] == ChunkFragment(range=c1, read_limit_qt_bytes=80)
    # C2 covers the rest of the file, no cut needed at the end because it is open-ended
    assert result[1] == ChunkFragment(range=c2, read_limit_qt_bytes=None)


def test_resolve_complex_shortest_path():
    # Path B: C2 -> C4 (2 chunks) - WINNER
    c2 = ChunkRange(0, 100)
    c4 = ChunkRange(101, 200)
    # Chunks: [0-50], [0-100], [40-150], [101-200]
    chunks = [ChunkRange(0, 50), c2, ChunkRange(40, 150), c4]

    result = calculate_optimal_coverage(chunks, file_size=201)

    assert len(result) == 2
    assert result[0] == ChunkFragment(range=c2, read_limit_qt_bytes=None)
    assert result[1] == ChunkFragment(range=c4, read_limit_qt_bytes=None)


def test_resolve_gap_raises_error():
    # C1: 0-10, C2: 12-20. Gap at 11.
    chunks = [ChunkRange(0, 10), ChunkRange(12, 20)]
    with pytest.raises(RuntimeError, match="Gap detected"):
        calculate_optimal_coverage(chunks, file_size=21)


def test_resolve_no_file_size_with_infinite_reach():
    # If file_size is None, it finds path to chunk with end=None
    c1 = ChunkRange(0, 100)
    c2 = ChunkRange(101, None)
    result = calculate_optimal_coverage([c1, c2])

    assert len(result) == 2
    assert result[0] == ChunkFragment(range=c1, read_limit_qt_bytes=None)
    assert result[1] == ChunkFragment(range=c2, read_limit_qt_bytes=None)


def test_resolve_incomplete_coverage_raises_error():
    chunks = [ChunkRange(0, 100)]
    with pytest.raises(RuntimeError, match="Gap detected"):
        calculate_optimal_coverage(chunks, file_size=200)


def test_resolve_first_chunk_not_at_zero():
    chunks = [ChunkRange(10, 100)]
    with pytest.raises(RuntimeError, match="Gap at the beginning"):
        calculate_optimal_coverage(chunks, file_size=101)


def test_resolve_complex_many_chunks_staircase():
    """
    Complex scenario with 16 chunks.
    Target: 1000 bytes.
    The 'best' path will take 10 chunks of 100 bytes each.
    We add 6 'distraction' chunks that are smaller or start at awkward places.
    """
    essential = [ChunkRange(i * 100, (i + 1) * 100 - 1) for i in range(10)]
    # Essential path: [0-99], [100-199], [200-299] ... [900-999]

    distractions = [
        ChunkRange(0, 50),  # Shadowed by C1
        ChunkRange(50, 150),  # Overlaps C1 and C2, but doesn't help shorten the path
        ChunkRange(250, 350),  # Overlaps C3 and C4
        ChunkRange(500, 550),  # Redundant for C6
        ChunkRange(850, 950),  # Overlaps C9 and C10
        ChunkRange(950, 1100),  # Goes beyond target
    ]

    all_chunks = essential + distractions
    result = calculate_optimal_coverage(all_chunks, file_size=1000)

    # BFS should find the 10 chunks of 100 bytes each as it's the most direct path
    assert len(result) == 10
    for i in range(10):
        assert result[i].range == essential[i]
        assert result[i].read_limit_qt_bytes is None
