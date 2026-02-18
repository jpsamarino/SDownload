from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_ranges


def test_no_cache():
    ranges = calculate_ranges(100, 4)
    expected = [
        ChunkRange(0, 24),
        ChunkRange(25, 49),
        ChunkRange(50, 74),
        ChunkRange(75, None),
    ]
    assert ranges == expected


def test_full_cache():
    ranges = calculate_ranges(100, 4, [ChunkRange(0, None)])
    expected = [ChunkRange(0, None)]
    assert ranges == expected


def test_partial_cache_fill_gaps():
    total_bytes = 200
    ranges = calculate_ranges(
        total_bytes, 10, [ChunkRange(0, 10), ChunkRange(20, 45), ChunkRange(163, None)]
    )

    # Check specific known chunks
    assert ChunkRange(0, 10) in ranges
    assert ChunkRange(20, 45) in ranges
    assert ChunkRange(163, None) in ranges
    total_bytes = sum(
        (
            (range.end - range.start + 1)
            if range.end is not None
            else total_bytes - range.start
        )
        for range in ranges
    )
    assert total_bytes == 200
    assert len(ranges) == 10


def test_cache_unsorted():
    ranges = calculate_ranges(100, 4, [ChunkRange(50, 74), ChunkRange(0, 24)])
    expected = [
        ChunkRange(0, 24),
        ChunkRange(25, 49),
        ChunkRange(50, 74),
        ChunkRange(75, None),
    ]
    assert ranges == expected


def test_small_gap_splitting():
    ranges = calculate_ranges(100, 10, [ChunkRange(0, 5)])
    assert ranges[0] == ChunkRange(0, 5)
    assert ranges[1] == ChunkRange(6, 15)


def test_common_case():
    ranges = calculate_ranges(108166270, 5, [])
    expected = [
        ChunkRange(0, 21633253),
        ChunkRange(21633254, 43266507),
        ChunkRange(43266508, 64899761),
        ChunkRange(64899762, 86533015),
        ChunkRange(86533016, None),
    ]
    total_bytes = sum(
        (
            (range.end - range.start + 1)
            if range.end is not None
            else 108166270 - range.start
        )
        for range in ranges
    )
    assert total_bytes == 108166270
    assert ranges == expected
