import random

from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
    EDownloadStatus,
)
from sDownload.utils.range_operations import (
    calculate_downloaded_bytes,
    calculate_optimal_coverage,
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


# ==============================================================================
# 1. Defesa contra Dados Nulos e Malformados
# ==============================================================================
def test_defensive_against_none_and_malformed_stats():
    """Validates that calculate_downloaded_bytes handles None items and malformed stats gracefully."""
    # Empty stats
    assert calculate_downloaded_bytes([]) == 0

    # None and corrupted range items in collection
    valid_stat = make_stat(0, 499, 500, EDownloadStatus.COMPLETED)
    malformed_stat = make_stat(500, 999, 500, EDownloadStatus.COMPLETED)
    malformed_stat.range = None  # Simulate corrupted range

    stats = [None, valid_stat, None, malformed_stat, None]
    assert calculate_downloaded_bytes(stats) == 500


# ==============================================================================
# 2. Contrato de Limites: file_size Fixo vs Stream Infinito
# ==============================================================================
def test_file_size_and_streaming_bounds():
    """
    Validates bounds behavior:
    - With file_size: clamps upper bound to file_size (never reports > 100%).
    - Without file_size (None): acts as unbounded stream accumulator.
    """
    stats = [
        make_stat(0, 1500, 1200, EDownloadStatus.DOWNLOADING),
    ]
    # Clamped to file_size=1000
    assert calculate_downloaded_bytes(stats, file_size=1000) == 1000

    # Unbounded streaming when file_size=None
    assert calculate_downloaded_bytes(stats, file_size=None) == 1200


# ==============================================================================
# 3. Proteção de Borda do Próprio Chunk (Socket Overshoot)
# ==============================================================================
def test_chunk_boundary_reach_clamping():
    """
    Validates that a chunk's bytes cannot overshoot past its own s.range.end
    if the socket reads more bytes than assigned.
    """
    stats = [
        make_stat(0, 99, 150, EDownloadStatus.COMPLETED),  # Target was 100 bytes (0..99)
    ]
    # Clamped to range.end (99) -> 100 bytes
    assert calculate_downloaded_bytes(stats) == 100


# ==============================================================================
# 4. Mosaico Complexo Determinístico (24 Chunks, Micro-Gaps, Sucessão e Subconjuntos)
# ==============================================================================
def test_deterministic_complex_mosaic():
    """
    Deterministic stress test with 24 handcrafted chunks across 5 regions:
    - Region 1 [0..300]: 5 overlapping & nested chunks (301 bytes).
    - 1-byte Micro-Gap at byte 301.
    - Region 2 [302..500]: 4 overlapping chunks (199 bytes).
    - 1-byte Micro-Gap at byte 501.
    - Region 3 [502..700]: 3 chunks including 1-byte single chunk [502, 502] (199 bytes).
    - 100-byte Gap at [701..800].
    - Region 4 [801..1300]: 8 chunks with duplicates, subsets, and edge touches (500 bytes).
    - 99-byte Gap at [1301..1399].
    - Region 5 [1400..1479]: 4 chunks mixed with DEPRECATED, CANCELLED, and partial DOWNLOADING (80 bytes).

    Total Expected: 301 + 199 + 199 + 500 + 80 = 1279 bytes.
    """
    stats = [
        # Region 1 (0..300 -> 301 bytes)
        make_stat(0, 100, 101, EDownloadStatus.COMPLETED),
        make_stat(50, 150, 101, EDownloadStatus.COMPLETED),
        make_stat(80, 200, 121, EDownloadStatus.COMPLETED),
        make_stat(20, 70, 51, EDownloadStatus.COMPLETED),
        make_stat(190, 300, 111, EDownloadStatus.COMPLETED),
        # Region 2 (302..500 -> 199 bytes) [Gap at 301]
        make_stat(302, 400, 99, EDownloadStatus.COMPLETED),
        make_stat(350, 450, 101, EDownloadStatus.COMPLETED),
        make_stat(400, 500, 101, EDownloadStatus.COMPLETED),
        make_stat(302, 310, 9, EDownloadStatus.COMPLETED),
        # Region 3 (502..700 -> 199 bytes) [Gap at 501]
        make_stat(502, 502, 1, EDownloadStatus.COMPLETED),
        make_stat(502, 600, 99, EDownloadStatus.COMPLETED),
        make_stat(550, 700, 151, EDownloadStatus.COMPLETED),
        # Region 4 (801..1300 -> 500 bytes) [Gap 701..800]
        make_stat(801, 1000, 200, EDownloadStatus.COMPLETED),
        make_stat(801, 1000, 200, EDownloadStatus.COMPLETED),  # Exact duplicate
        make_stat(850, 950, 101, EDownloadStatus.COMPLETED),
        make_stat(820, 880, 61, EDownloadStatus.COMPLETED),
        make_stat(900, 1100, 201, EDownloadStatus.COMPLETED),
        make_stat(1050, 1200, 151, EDownloadStatus.COMPLETED),
        make_stat(1199, 1200, 2, EDownloadStatus.COMPLETED),
        make_stat(1200, 1300, 101, EDownloadStatus.COMPLETED),
        # Region 5 (1400..1479 -> 80 bytes) [Gap 1301..1399]
        make_stat(1400, 1600, 50, EDownloadStatus.DOWNLOADING),
        make_stat(1420, 1700, 200, EDownloadStatus.DEPRECATED),  # Ignored
        make_stat(1430, 1800, 100, EDownloadStatus.CANCELLED),  # Ignored
        make_stat(1440, 1500, 40, EDownloadStatus.DOWNLOADING),
    ]

    random.seed(42)
    random.shuffle(stats)

    assert len(stats) == 24
    assert calculate_downloaded_bytes(stats) == 1279


# ==============================================================================
# 5. Validação Cruzada Probabilística (10 Loops com Dijkstra / BFS)
# ==============================================================================
def test_cross_validation_dijkstra_10_loops():
    """
    Performs 10 consecutive randomized stress loops with 100-300 chunks each.
    Cross-validates:
    1. calculate_downloaded_bytes matches the bitset ground-truth.
    2. When complete: calculate_optimal_coverage (Dijkstra) finds the exact path summing to file_size.
    3. When incomplete: calculate_optimal_coverage detects the gap and raises ValueError.
    """
    random.seed(42)

    for iteration in range(1, 11):
        file_size = random.randint(5_000, 20_000)
        num_chunks = random.randint(100, 300)
        bitset = [False] * file_size

        stats = []
        effective_ranges: list[ChunkRange] = []

        for _ in range(num_chunks):
            start = random.randint(0, file_size + 500)
            max_len = random.randint(5, 500)
            end = start + max_len - 1

            status = random.choice(
                [
                    EDownloadStatus.COMPLETED,
                    EDownloadStatus.DOWNLOADING,
                    EDownloadStatus.DEPRECATED,
                    EDownloadStatus.CANCELLED,
                    EDownloadStatus.PENDING,
                ]
            )

            if status == EDownloadStatus.COMPLETED:
                downloaded = max_len
            elif status == EDownloadStatus.DOWNLOADING:
                downloaded = random.randint(1, max_len)
            elif status == EDownloadStatus.DEPRECATED:
                downloaded = max_len
            elif status == EDownloadStatus.CANCELLED:
                downloaded = random.randint(0, max_len)
            else:
                downloaded = 0

            stat = make_stat(start, end, downloaded, status)
            stats.append(stat)

            # Track valid downloading / completed chunks
            if (
                status in (EDownloadStatus.COMPLETED, EDownloadStatus.DOWNLOADING)
                and downloaded > 0
            ):
                chunk_reach = min(start + downloaded - 1, end)
                if start < file_size and chunk_reach >= start:
                    effective_reach = min(chunk_reach, file_size - 1)
                    effective_ranges.append(ChunkRange(start, effective_reach))
                    for b in range(start, effective_reach + 1):
                        bitset[b] = True

        computed_bytes = calculate_downloaded_bytes(stats, file_size=file_size)
        expected_bitset_bytes = sum(1 for b in bitset if b)

        # Cross-validation 1: Matches bitset
        assert computed_bytes == expected_bitset_bytes, (
            f"Iteration {iteration}: computed {computed_bytes} != bitset {expected_bitset_bytes}"
        )

        # Cross-validation 2: Matches Dijkstra / BFS optimal coverage
        if computed_bytes == file_size and effective_ranges:
            optimal_fragments = calculate_optimal_coverage(effective_ranges, file_size=file_size)
            assert len(optimal_fragments) > 0

            dijkstra_total_bytes = sum(
                frag.read_limit_qt_bytes
                if frag.read_limit_qt_bytes is not None
                else (frag.range.end - frag.range.start + 1)
                for frag in optimal_fragments
            )
            assert dijkstra_total_bytes == file_size
        else:
            gap_detected = False
            try:
                calculate_optimal_coverage(effective_ranges, file_size=file_size)
            except ValueError:
                gap_detected = True

            assert gap_detected
