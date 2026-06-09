import pytest
import asyncio
import json
import os
import shutil
from pathlib import Path
from datetime import datetime
from sDownload.services.downloader_manager.recovery_download import (
    RecoveryDownload,
)
from sDownload.file_system.local_storage import LocalStorage
from sDownload.interfaces.models import (
    ChunkDownloadStats,
    ChunkRange,
    EDownloadStatus,
)


@pytest.fixture
def temp_storage_dir():
    path = Path("temp_test_storage")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    yield path
    if path.exists():
        shutil.rmtree(path)


@pytest.fixture
def local_storage(temp_storage_dir):
    return LocalStorage(storage_dir=temp_storage_dir)


@pytest.fixture
def recovery_service(local_storage):
    return RecoveryDownload(local_storage)


async def create_dummy_file(storage, name, size):
    async def data_gen():
        yield b"0" * size

    await storage.save_binary_data(name, data_gen())


@pytest.mark.asyncio
async def test_recovery_flow_full_integration(
    recovery_service, local_storage, temp_storage_dir
):
    file_id = "test_file_123"
    total_size = 5000

    # 1. Setup real files on "disk"
    # Chunk 1: Fully completed (1000 bytes)
    await create_dummy_file(local_storage, "chunk_1.tmp", 1000)
    # Chunk 2: Partial (2000 bytes on disk)
    await create_dummy_file(local_storage, "chunk_2.tmp", 2000)

    stats1 = ChunkDownloadStats(
        chunk_file_name="chunk_1.tmp",
        range=ChunkRange(0, 999),
        file_size=1000,
        status=EDownloadStatus.COMPLETED,
        bytes_downloaded=1000,
        progress=100.0,
    )

    stats2 = ChunkDownloadStats(
        chunk_file_name="chunk_2.tmp",
        range=ChunkRange(1000, 3999),  # Planned 3000 bytes
        file_size=3000,
        status=EDownloadStatus.DOWNLOADING,
        bytes_downloaded=2000,
        progress=66.6,
    )

    # 2. Save recovery info (lowering min_chunk_size to 100 to keep chunk_2)
    await recovery_service.save_info(
        file_id, total_size, [stats1, stats2], min_chunk_size=100
    )

    # 3. Verify JSON file exists and content is correct
    json_path = temp_storage_dir / f".sdown_resume_{file_id}.json"
    assert json_path.exists()

    with open(json_path, "r") as f:
        data = json.load(f)

    assert data["file_id"] == file_id
    assert len(data["chunks"]) == 2
    # Chunk 2 should be reduced to its disk size (2000) and marked as "complete" in recovery state
    assert data["chunks"][1]["bytes"] == 2000
    assert data["chunks"][1]["start"] == 1000
    assert data["chunks"][1]["end"] == 2999

    # 4. Load info and verify reconstruction
    loaded_info = await recovery_service.load_info(file_id)
    assert loaded_info is not None
    assert loaded_info.file_id == file_id
    assert isinstance(loaded_info.updated_at, datetime)
    assert len(loaded_info.chunks_finished) == 2

    c1 = loaded_info.chunks_finished[0]
    assert c1.chunk_file_name == "chunk_1.tmp"
    assert c1.status == EDownloadStatus.COMPLETED
    assert c1.range.end == 999

    c2 = loaded_info.chunks_finished[1]
    assert c2.chunk_file_name == "chunk_2.tmp"
    assert c2.status == EDownloadStatus.COMPLETED
    assert c2.range.start == 1000
    assert c2.range.end == 2999
    assert c2.bytes_downloaded == 2000


@pytest.mark.asyncio
async def test_recovery_cleanup_and_corruption_integration(
    recovery_service, local_storage, temp_storage_dir
):
    file_id = "test_cleanup_456"

    # 1. Setup files
    # Chunk 1: Valid (1MB)
    await create_dummy_file(local_storage, "valid.tmp", 1024 * 1024)
    # Chunk 2: Too small (under 1MB)
    await create_dummy_file(local_storage, "small.tmp", 500)
    # Chunk 3: Corrupted (Size mismatch: stats says 1000, disk has 500)
    await create_dummy_file(local_storage, "corrupt.tmp", 500)

    stats_valid = ChunkDownloadStats(
        chunk_file_name="valid.tmp",
        range=ChunkRange(0, 1048575),
        file_size=1024 * 1024,
        status=EDownloadStatus.COMPLETED,
        bytes_downloaded=1024 * 1024,
    )

    stats_small = ChunkDownloadStats(
        chunk_file_name="small.tmp",
        range=ChunkRange(1048576, 1049075),
        file_size=500,
        status=EDownloadStatus.DOWNLOADING,
        bytes_downloaded=500,
    )

    stats_corrupt = ChunkDownloadStats(
        chunk_file_name="corrupt.tmp",
        range=ChunkRange(1050000, 1051000),
        file_size=1000,
        status=EDownloadStatus.DOWNLOADING,
        bytes_downloaded=1000,  # Mismatch with disk (500)
    )

    # 2. Save (should trigger parallel cleanup)
    await recovery_service.save_info(
        file_id,
        5000000,
        [stats_valid, stats_small, stats_corrupt],
        min_chunk_size=1024 * 1024,
    )

    # 3. Verify disk state
    assert (temp_storage_dir / "valid.tmp").exists()
    assert not (temp_storage_dir / "small.tmp").exists()  # Should be deleted as useless
    assert not (
        temp_storage_dir / "corrupt.tmp"
    ).exists()  # Should be deleted as corrupted

    # 4. Verify JSON content (only 'valid' should be there)
    loaded = await recovery_service.load_info(file_id)
    assert len(loaded.chunks_finished) == 1
    assert loaded.chunks_finished[0].chunk_file_name == "valid.tmp"


@pytest.mark.asyncio
async def test_recovery_disk_resilience_integration(
    recovery_service, local_storage, temp_storage_dir
):
    file_id = "resilience_789"

    # 1. Save valid state
    await create_dummy_file(local_storage, "chunk_v1.tmp", 5000)
    await create_dummy_file(local_storage, "chunk_v2.tmp", 5000)

    stats_v1 = ChunkDownloadStats(
        chunk_file_name="chunk_v1.tmp",
        range=ChunkRange(0, 4999),
        file_size=5000,
        status=EDownloadStatus.COMPLETED,
        bytes_downloaded=5000,
    )
    stats_v2 = ChunkDownloadStats(
        chunk_file_name="chunk_v2.tmp",
        range=ChunkRange(5000, 9999),
        file_size=5000,
        status=EDownloadStatus.COMPLETED,
        bytes_downloaded=5000,
    )

    await recovery_service.save_info(file_id, 10000, [stats_v1, stats_v2])

    # 2. Simulate disk event (v1 deleted, v2 truncated)
    os.remove(temp_storage_dir / "chunk_v1.tmp")
    with open(temp_storage_dir / "chunk_v2.tmp", "wb") as f:
        f.write(b"0" * 1000)  # Now it's 1000, was 5000

    # 3. Load info (should validate against disk and exclude both)
    loaded = await recovery_service.load_info(file_id)
    assert loaded is not None
    assert len(loaded.chunks_finished) == 0  # Both failed validation


@pytest.mark.asyncio
async def test_purge_all_integration(recovery_service, local_storage, temp_storage_dir):
    file_id = "purge_test_999"
    json_path = temp_storage_dir / f".sdown_resume_{file_id}.json"

    # 1. Create a state with some files
    await create_dummy_file(local_storage, "p1.tmp", 100)
    await create_dummy_file(local_storage, "p2.tmp", 100)

    stats1 = ChunkDownloadStats(
        chunk_file_name="p1.tmp",
        range=ChunkRange(0, 99),
        file_size=100,
        status=EDownloadStatus.COMPLETED,
        bytes_downloaded=100,
    )
    stats2 = ChunkDownloadStats(
        chunk_file_name="p2.tmp",
        range=ChunkRange(100, 199),
        file_size=100,
        status=EDownloadStatus.COMPLETED,
        bytes_downloaded=100,
    )

    await recovery_service.save_info(file_id, 200, [stats1, stats2])
    assert json_path.exists()
    assert (temp_storage_dir / "p1.tmp").exists()
    assert (temp_storage_dir / "p2.tmp").exists()

    # 2. Purge everything
    await recovery_service.purge_all(file_id)

    # 3. Verify all gone
    assert not json_path.exists()
    assert not (temp_storage_dir / "p1.tmp").exists()
    assert not (temp_storage_dir / "p2.tmp").exists()
