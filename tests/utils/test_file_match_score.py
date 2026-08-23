from datetime import datetime, timedelta, timezone
from typing import AsyncIterable, Optional
import pytest

from sDownload.interfaces.models import StoredFileInfo, FileMatchScore
from sDownload.interfaces.protocols import FileStorageProtocol
from sDownload.utils import calculate_file_match_score


class DummyStorage(FileStorageProtocol):
    def __init__(self, files: dict[str, StoredFileInfo] | None = None):
        self.files = files or {}

    async def get_data_info(self, key: str) -> Optional[StoredFileInfo]:
        return self.files.get(key)

    async def save_binary_data(self, key: str, data: AsyncIterable[bytes]):
        pass

    async def merge_binary_files(self, source_keys: list[str], dest_key: str):
        pass

    async def delete_data(self, key: str):
        pass

    async def shrink_file_to(self, key: str, target_size_bytes: int):
        pass

    async def list_data(self) -> list[str]:
        return list(self.files.keys())

    def get_binary_data(self, key: str) -> AsyncIterable[bytes]:
        async def empty():
            if False:
                yield b""

        return empty()


@pytest.mark.asyncio
async def test_calculate_file_match_score_non_existent_file():
    storage = DummyStorage()
    result = await calculate_file_match_score(
        storage=storage,
        file_name="missing.zip",
        expected_size=1024,
    )

    assert isinstance(result, FileMatchScore)
    assert result.score == 0.0
    assert result.file_exists is False
    assert result.size_matched is False
    assert result.age_seconds is None
    assert "does not exist" in result.reason


@pytest.mark.asyncio
async def test_calculate_file_match_score_size_mismatch():
    now = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    info = StoredFileInfo(
        key="file.zip",
        size_bytes=500,
        created_at=now,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        reference_time=now,
    )

    assert result.score == 0.0
    assert result.file_exists is True
    assert result.size_matched is False
    assert "Size mismatch" in result.reason


@pytest.mark.asyncio
async def test_calculate_file_match_score_within_1_hour():
    ref_time = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    # File created 30 minutes ago
    file_time = ref_time - timedelta(minutes=30)
    info = StoredFileInfo(
        key="file.zip",
        size_bytes=1000,
        created_at=file_time,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        reference_time=ref_time,
    )

    # 0.50 (base) + 0.30 (<= 1h) = 0.80
    assert result.score == pytest.approx(0.80)
    assert result.file_exists is True
    assert result.size_matched is True
    assert result.age_seconds == pytest.approx(1800.0)


@pytest.mark.asyncio
async def test_calculate_file_match_score_within_24_hours():
    ref_time = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    # File created 5 hours ago
    file_time = ref_time - timedelta(hours=5)
    info = StoredFileInfo(
        key="file.zip",
        size_bytes=1000,
        created_at=file_time,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        reference_time=ref_time,
    )

    # 0.50 (base) + 0.20 (<= 24h) = 0.70
    assert result.score == pytest.approx(0.70)
    assert result.file_exists is True
    assert result.size_matched is True


@pytest.mark.asyncio
async def test_calculate_file_match_score_within_7_days():
    ref_time = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    # File created 3 days ago
    file_time = ref_time - timedelta(days=3)
    info = StoredFileInfo(
        key="file.zip",
        size_bytes=1000,
        created_at=file_time,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        reference_time=ref_time,
    )

    # 0.50 (base) + 0.10 (<= 7d) = 0.60
    assert result.score == pytest.approx(0.60)
    assert result.file_exists is True
    assert result.size_matched is True


@pytest.mark.asyncio
async def test_calculate_file_match_score_very_old_file():
    ref_time = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    # File created 60 days ago
    file_time = ref_time - timedelta(days=60)
    info = StoredFileInfo(
        key="file.zip",
        size_bytes=1000,
        created_at=file_time,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        reference_time=ref_time,
    )

    # 0.50 (base only)
    assert result.score == pytest.approx(0.50)
    assert result.file_exists is True
    assert result.size_matched is True


@pytest.mark.asyncio
async def test_calculate_file_match_score_newer_than_remote():
    ref_time = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    # Remote modified 2 days ago
    remote_time = ref_time - timedelta(days=2)
    # Local downloaded 1 hour ago
    local_time = ref_time - timedelta(hours=1)

    info = StoredFileInfo(
        key="file.zip",
        size_bytes=1000,
        created_at=local_time,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        remote_created_at=remote_time,
        reference_time=ref_time,
    )

    # 0.50 (base) + 0.30 (<= 1h) + 0.20 (newer than remote) = 1.0
    assert result.score == pytest.approx(1.0)
    assert "newer than remote" in result.reason


@pytest.mark.asyncio
async def test_calculate_file_match_score_older_than_remote():
    ref_time = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    # Local downloaded 3 days ago
    local_time = ref_time - timedelta(days=3)
    # Remote modified yesterday
    remote_time = ref_time - timedelta(days=1)

    info = StoredFileInfo(
        key="file.zip",
        size_bytes=1000,
        created_at=local_time,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        remote_created_at=remote_time,
        reference_time=ref_time,
    )

    # Local file is older than remote version -> 0.0
    assert result.score == 0.0
    assert "older than remote" in result.reason


@pytest.mark.asyncio
async def test_calculate_file_match_score_handles_naive_datetimes():
    ref_time = datetime(2026, 8, 22, 12, 0, 0)  # Naive
    local_time = datetime(2026, 8, 22, 11, 30, 0)  # Naive (30 min ago)
    info = StoredFileInfo(
        key="file.zip",
        size_bytes=1000,
        created_at=local_time,
    )
    storage = DummyStorage({"file.zip": info})

    result = await calculate_file_match_score(
        storage=storage,
        file_name="file.zip",
        expected_size=1000,
        reference_time=ref_time,
    )

    assert result.score == pytest.approx(0.80)
    assert result.age_seconds == pytest.approx(1800.0)


@pytest.mark.asyncio
async def test_calculate_file_match_score_real_filesystem_lifecycle(tmp_path):
    from sDownload.file_system.local_storage import LocalStorage

    storage = LocalStorage(storage_dir=str(tmp_path))
    file_name = "real_sample.bin"
    data = b"A" * 4096

    async def binary_stream():
        yield data

    # 1. Save real file to disk
    await storage.save_binary_data(file_name, binary_stream())
    assert (tmp_path / file_name).exists()
    assert (tmp_path / file_name).stat().st_size == 4096

    # 2. Check score right after creation (should be <= 1h -> score = 0.80)
    result_fresh = await calculate_file_match_score(
        storage=storage,
        file_name=file_name,
        expected_size=4096,
    )
    assert result_fresh.file_exists is True
    assert result_fresh.size_matched is True
    assert result_fresh.age_seconds is not None
    assert result_fresh.age_seconds < 10.0  # Just created seconds ago
    assert result_fresh.score == pytest.approx(0.80)

    # 3. Check with remote Last-Modified older than local file (score = 1.0)
    remote_past = datetime.now(timezone.utc) - timedelta(days=2)
    result_past = await calculate_file_match_score(
        storage=storage,
        file_name=file_name,
        expected_size=4096,
        remote_created_at=remote_past,
    )
    assert result_past.score == pytest.approx(1.0)
    assert "newer than remote" in result_past.reason

    # 4. Check with remote Last-Modified newer than local file (score = 0.0)
    remote_future = datetime.now(timezone.utc) + timedelta(hours=2)
    result_future = await calculate_file_match_score(
        storage=storage,
        file_name=file_name,
        expected_size=4096,
        remote_created_at=remote_future,
    )
    assert result_future.score == 0.0
    assert "older than remote" in result_future.reason

    # 5. Delete real file from disk
    await storage.delete_data(file_name)
    assert not (tmp_path / file_name).exists()

    # 6. Verify score on deleted file
    result_deleted = await calculate_file_match_score(
        storage=storage,
        file_name=file_name,
        expected_size=4096,
    )
    assert result_deleted.file_exists is False
    assert result_deleted.score == 0.0


@pytest.mark.asyncio
async def test_calculate_file_match_score_real_filesystem_size_mismatch(tmp_path):
    from sDownload.file_system.local_storage import LocalStorage

    storage = LocalStorage(storage_dir=str(tmp_path))
    file_name = "partial.bin"
    data = b"B" * 500

    async def binary_stream():
        yield data

    # 1. Save 500 bytes to disk
    await storage.save_binary_data(file_name, binary_stream())

    # 2. Check score expecting 1000 bytes
    result = await calculate_file_match_score(
        storage=storage,
        file_name=file_name,
        expected_size=1000,
    )
    assert result.file_exists is True
    assert result.size_matched is False
    assert result.score == 0.0
    assert "Size mismatch" in result.reason

    # 3. Clean up
    await storage.delete_data(file_name)
    assert not (tmp_path / file_name).exists()

