import logging
import pytest
from sDownload.services.downloader_manager.chunk_utils import reconstruct_file
from sDownload.file_system.local_storage import LocalStorage
from sDownload.interfaces.models import (
    ChunkDownloadStats,
    EDownloadStatus,
    ChunkRange,
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_reconstructor_integration")


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(storage_dir=tmp_path)


def create_stats(
    name: str,
    range_start: int,
    range_end: int | None,
    status: EDownloadStatus,
) -> ChunkDownloadStats:
    return ChunkDownloadStats(
        chunk_file_name=name,
        range=ChunkRange(range_start, range_end),
        file_size=range_end - range_start + 1 if range_end is not None else None,
        status=status,
    )


@pytest.mark.asyncio
async def test_reconstructor_merge_real_files(storage, tmp_path):

    chunk1_name = "chunk_0_4.bin"
    chunk2_name = "chunk_5_9.bin"
    chunk3_name = "chunk_10_14.bin"

    content1 = b"01234"
    content2 = b"56789"
    content3 = b"abcde"

    (tmp_path / chunk1_name).write_bytes(content1)
    (tmp_path / chunk2_name).write_bytes(content2)
    (tmp_path / chunk3_name).write_bytes(content3)

    stats = [
        create_stats(chunk1_name, 0, 4, EDownloadStatus.COMPLETED),
        create_stats(chunk2_name, 5, 9, EDownloadStatus.COMPLETED),
        create_stats(chunk3_name, 10, 14, EDownloadStatus.COMPLETED),
    ]

    final_name = "merged_final.bin"

    result_key = await reconstruct_file(
        storage=storage, stats_list=stats, final_filename=final_name, total_file_size=15
    )

    assert result_key == final_name
    final_file = tmp_path / final_name

    assert final_file.exists(), "Merged file should exist"
    assert final_file.stat().st_size == 15
    assert final_file.read_bytes() == b"0123456789abcde"


@pytest.mark.asyncio
async def test_reconstructor_merge_complex_overlaps(storage, tmp_path):

    total_size = 100
    ideal_content = bytes([i % 256 for i in range(total_size)])

    ranges = [(0, 20), (10, 15), (15, 40), (35, 70), (50, 60), (65, 99)]

    stats = []
    for i, (start, end) in enumerate(ranges):
        name = f"chunk_{i}.bin"
        content = ideal_content[start : end + 1]
        (tmp_path / name).write_bytes(content)
        stats.append(create_stats(name, start, end, EDownloadStatus.COMPLETED))

    final_name = "complex_reconstruction.bin"
    await reconstruct_file(
        storage=storage,
        stats_list=stats,
        final_filename=final_name,
        total_file_size=total_size,
    )

    final_file = tmp_path / final_name
    assert final_file.exists()
    assert final_file.stat().st_size == total_size
    assert final_file.read_bytes() == ideal_content


@pytest.mark.asyncio
async def test_reconstructor_merge_with_redundant_chunks(storage, tmp_path):

    (tmp_path / "cA.bin").write_bytes(b"A" * 51)
    (tmp_path / "cB.bin").write_bytes(b"B" * 11)
    (tmp_path / "cC.bin").write_bytes(b"C" * 50)

    stats = [
        create_stats("cA.bin", 0, 50, EDownloadStatus.COMPLETED),
        create_stats("cB.bin", 10, 20, EDownloadStatus.COMPLETED),
        create_stats("cC.bin", 51, 100, EDownloadStatus.COMPLETED),
    ]

    final_name = "redundant.bin"
    await reconstruct_file(
        storage=storage,
        stats_list=stats,
        final_filename=final_name,
        total_file_size=101,
    )

    final_file = tmp_path / final_name
    assert final_file.read_bytes() == (b"A" * 51 + b"C" * 50)
