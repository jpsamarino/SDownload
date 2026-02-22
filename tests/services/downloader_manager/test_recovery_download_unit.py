import pytest
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime
from sDownload.services.downloader_manager.recovery_download import (
    RecoveryDownload,
    DownloadInfo,
)
from sDownload.interfaces.models import (
    FileInfoModel,
    ChunkDownloadStats,
    ChunkRange,
    StoredFileInfo,
    EDownloadStatus,
)


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.list_data = AsyncMock(return_value=[])
    storage.get_data_info = AsyncMock(return_value=None)
    storage.save_binary_data = AsyncMock()
    storage.get_binary_data = MagicMock()
    storage.delete_data = AsyncMock()
    return storage


@pytest.fixture
def recovery_service(mock_storage):
    return RecoveryDownload(mock_storage)


@pytest.fixture
def sample_file_info():
    return FileInfoModel(
        file_name="test.zip",
        file_dir="/tmp",
        file_size=1000,
        file_id="unique_id_123",
        download_url="http://example.com/test.zip",
        transmission_protocol="http",
        server_accept_ranges=True,
        file_created_at=datetime.now(),
        protocol_data=None,
    )


@pytest.mark.asyncio
async def test_save_info_delayed_deletion(recovery_service, mock_storage):
    file_id = "unique_id_123"
    total_size = 1000

    # Setup stats:
    # Chunk 1: Finished 100% -> Keep
    # Chunk 2: Mismatch size -> Mark for deletion
    # Chunk 3: Small chunk -> Mark for deletion

    stats1 = ChunkDownloadStats(
        chunk_file_name="c1.tmp",
        range=ChunkRange(0, 199),
        file_size=200,
        status=EDownloadStatus.COMPLETED,
    )
    stats1.bytes_downloaded = 200

    stats2 = ChunkDownloadStats(
        chunk_file_name="c2.tmp", range=ChunkRange(200, 399), file_size=200
    )
    stats2.bytes_downloaded = 100  # Stats says 100

    stats3 = ChunkDownloadStats(
        chunk_file_name="c3.tmp", range=ChunkRange(400, 1000), file_size=600
    )
    stats3.bytes_downloaded = 50  # Small chunk

    def mock_get_info(key):
        if key == "c1.tmp":
            return StoredFileInfo(
                key="c1.tmp", size_bytes=200, created_at=datetime.now()
            )
        if key == "c2.tmp":
            return StoredFileInfo(
                key="c2.tmp", size_bytes=50, created_at=datetime.now()
            )
        if key == "c3.tmp":
            return StoredFileInfo(
                key="c3.tmp", size_bytes=50, created_at=datetime.now()
            )
        return None

    mock_storage.get_data_info.side_effect = mock_get_info

    # Tracker to verify order of calls
    call_order = []

    async def mock_save(key, stream):
        call_order.append("save")
        async for _ in stream:
            pass
        return None

    async def mock_delete(key):
        call_order.append(f"delete_{key}")
        return None

    mock_storage.save_binary_data.side_effect = mock_save
    mock_storage.delete_data.side_effect = mock_delete

    await recovery_service.save_info(
        file_id, total_size, [stats1, stats2, stats3], min_chunk_size=100
    )

    # Verify save happened before deletions
    assert "save" in call_order
    save_idx = call_order.index("save")
    assert call_order.index("delete_c2.tmp") > save_idx
    assert call_order.index("delete_c3.tmp") > save_idx


@pytest.mark.asyncio
async def test_save_info_with_reduction_and_filter(recovery_service, mock_storage):
    file_id = "unique_id_123"
    total_size = 1000

    # Setup stats:
    # Chunk 1: Finished 100% (200 bytes) -> Should always save
    # Chunk 2: Partial (500 bytes on disk) -> Should save if >= 100 bytes (min_chunk_size)
    # Chunk 3: Partial (50 bytes on disk) -> Should filter out because 50 < 100

    stats1 = ChunkDownloadStats(
        chunk_file_name="c1.tmp", range=ChunkRange(0, 199), file_size=200
    )
    stats1.bytes_downloaded = 200

    stats2 = ChunkDownloadStats(
        chunk_file_name="c2.tmp", range=ChunkRange(200, 999), file_size=800
    )
    stats2.bytes_downloaded = 500

    stats3 = ChunkDownloadStats(
        chunk_file_name="c3.tmp", range=ChunkRange(1000, 1999), file_size=1000
    )
    stats3.bytes_downloaded = 50

    # Mock storage list
    # Mock storage info
    def mock_get_info_reduction(key):
        if key == "c1.tmp":
            return StoredFileInfo(
                key="c1.tmp", size_bytes=200, created_at=datetime.now()
            )
        if key == "c2.tmp":
            return StoredFileInfo(
                key="c2.tmp", size_bytes=500, created_at=datetime.now()
            )
        if key == "c3.tmp":
            return StoredFileInfo(
                key="c3.tmp", size_bytes=50, created_at=datetime.now()
            )
        return None

    mock_storage.get_data_info.side_effect = mock_get_info_reduction

    # Using 100 bytes as min_chunk_size for testing
    await recovery_service.save_info(
        file_id, total_size, [stats1, stats2, stats3], min_chunk_size=100
    )

    # Verify save_binary_data was called
    assert mock_storage.save_binary_data.called
    args = mock_storage.save_binary_data.call_args
    assert args[0][0] == ".sdown_resume_unique_id_123.json"

    # Capture the saved data
    stream = args[0][1]
    saved_bytes = b""
    async for chunk in stream:
        saved_bytes += chunk
    saved_data = json.loads(saved_bytes.decode("utf-8"))

    assert len(saved_data["chunks"]) == 2  # c1 and c2
    assert saved_data["file_id"] == file_id

    # c1 check (DTO format: start, end, bytes)
    assert saved_data["chunks"][0]["chunk_file_name"] == "c1.tmp"
    assert saved_data["chunks"][0]["bytes"] == 200
    assert saved_data["chunks"][0]["start"] == 0
    assert saved_data["chunks"][0]["end"] == 199

    # c2 check
    assert saved_data["chunks"][1]["chunk_file_name"] == "c2.tmp"
    assert saved_data["chunks"][1]["bytes"] == 500
    assert saved_data["chunks"][1]["start"] == 200
    assert saved_data["chunks"][1]["end"] == 699


@pytest.mark.asyncio
async def test_load_info_success(recovery_service, mock_storage):
    file_id = "unique_id_123"
    # DTO-style mock data
    recovery_data = {
        "file_id": file_id,
        "file_size": 1000,
        "chunks": [{"chunk_file_name": "c1.tmp", "start": 0, "end": 199, "bytes": 200}],
        "updated_at": "2026-02-21T00:00:00Z",
    }

    async def mock_get_data(key):
        yield json.dumps(recovery_data).encode("utf-8")

    mock_storage.get_binary_data.side_effect = mock_get_data

    def mock_get_info_success(key):
        if key == ".sdown_resume_unique_id_123.json":
            return StoredFileInfo(key=key, size_bytes=100, created_at=datetime.now())
        if key == "c1.tmp":
            return StoredFileInfo(
                key="c1.tmp", size_bytes=200, created_at=datetime.now()
            )
        return None

    mock_storage.get_data_info.side_effect = mock_get_info_success

    result = await recovery_service.load_info(file_id)

    assert result is not None
    assert result.file_id == file_id
    assert isinstance(result.updated_at, datetime)
    assert len(result.chunks_finished) == 1
    stats = result.chunks_finished[0]
    assert isinstance(stats, ChunkDownloadStats)
    assert stats.chunk_file_name == "c1.tmp"
    assert stats.bytes_downloaded == 200
    assert stats.range.start == 0
    assert stats.range.end == 199
    assert stats.status == EDownloadStatus.COMPLETED


@pytest.mark.asyncio
async def test_load_info_validation_failure(recovery_service, mock_storage):
    file_id = "unique_id_123"
    recovery_data = {
        "file_id": file_id,
        "file_size": 1000,
        "chunks": [{"chunk_file_name": "c1.tmp", "start": 0, "end": 199, "bytes": 200}],
        "updated_at": "2026-02-21T10:00:00Z",
    }

    async def mock_get_data(key):
        yield json.dumps(recovery_data).encode("utf-8")

    mock_storage.get_binary_data.side_effect = mock_get_data

    def mock_get_info_failure(key):
        if key == ".sdown_resume_unique_id_123.json":
            return StoredFileInfo(key=key, size_bytes=100, created_at=datetime.now())
        if key == "c1.tmp":
            return StoredFileInfo(
                key="c1.tmp", size_bytes=100, created_at=datetime.now()
            )
        return None

    mock_storage.get_data_info.side_effect = mock_get_info_failure

    result = await recovery_service.load_info("unique_id_123")
    assert result is not None
    assert len(result.chunks_finished) == 0
