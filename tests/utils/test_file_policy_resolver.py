from collections.abc import AsyncIterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sDownload.file_system import LocalStorage
from sDownload.interfaces.models import (
    EFileAction,
    EFilePolicy,
    StoredFileInfo,
)
from sDownload.interfaces.protocols import FileStorageProtocol
from sDownload.utils.file_policy_resolver import (
    _split_stem_and_extension,
    find_available_file_name,
    resolve_file_policy,
)


class DummyStorage(FileStorageProtocol):
    def __init__(self, files: dict[str, StoredFileInfo] | None = None) -> None:
        self.files = files or {}

    async def get_data_info(self, key: str) -> StoredFileInfo | None:
        return self.files.get(key)

    async def save_binary_data(self, key: str, data: AsyncIterable[bytes]) -> None:
        pass

    async def merge_binary_files(self, source_keys: list[str], dest_key: str) -> None:
        pass

    async def delete_data(self, key: str) -> None:
        self.files.pop(key, None)

    async def shrink_file_to(self, key: str, target_size_bytes: int) -> None:
        pass

    async def move_data(self, src_key: str, dest_key: str) -> None:
        pass

    async def crop_file(self, key: str, start_byte: int, end_byte: int) -> None:
        pass

    async def list_data(self) -> list[StoredFileInfo]:
        return list(self.files.values())


def test_split_stem_and_extension():
    # Simple extension
    assert _split_stem_and_extension("file.zip") == ("file", ".zip")
    assert _split_stem_and_extension("video.mp4") == ("video", ".mp4")

    # Compound extensions
    assert _split_stem_and_extension("archive.tar.gz") == ("archive", ".tar.gz")
    assert _split_stem_and_extension("backup.tar.bz2") == ("backup", ".tar.bz2")
    assert _split_stem_and_extension("data.tar.xz") == ("data", ".tar.xz")

    # No extension
    assert _split_stem_and_extension("no_ext_file") == ("no_ext_file", "")


@pytest.mark.asyncio
async def test_find_available_file_name():
    files = {
        "report.pdf": StoredFileInfo(
            key="report.pdf", size_bytes=100, created_at=datetime.now(UTC)
        ),
        "report_1.pdf": StoredFileInfo(
            key="report_1.pdf", size_bytes=100, created_at=datetime.now(UTC)
        ),
    }
    storage = DummyStorage(files)

    # If file not taken, returns as is
    assert await find_available_file_name(storage, "other.pdf") == "other.pdf"

    # If file taken, increments to next available
    assert await find_available_file_name(storage, "report.pdf") == "report_2.pdf"


@pytest.mark.asyncio
async def test_resolve_file_not_existing():
    storage = DummyStorage()
    res = await resolve_file_policy(storage, "new_file.zip", 1000, policy=EFilePolicy.SMART_REUSE)
    assert res.action == EFileAction.DOWNLOAD
    assert res.target_file_name == "new_file.zip"
    assert res.is_renamed is False


@pytest.mark.asyncio
async def test_resolve_policy_overwrite():
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=500, created_at=datetime.now(UTC)),
    }
    storage = DummyStorage(files)
    res = await resolve_file_policy(storage, "file.zip", 1000, policy=EFilePolicy.OVERWRITE)
    assert res.action == EFileAction.DOWNLOAD
    assert res.target_file_name == "file.zip"
    assert res.is_renamed is False


@pytest.mark.asyncio
async def test_resolve_policy_fail_if_exists():
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=1000, created_at=datetime.now(UTC)),
    }
    storage = DummyStorage(files)
    res = await resolve_file_policy(storage, "file.zip", 1000, policy=EFilePolicy.FAIL_IF_EXISTS)
    assert res.action == EFileAction.ERROR
    assert res.target_file_name == "file.zip"


@pytest.mark.asyncio
async def test_resolve_policy_skip_if_exists():
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=500, created_at=datetime.now(UTC)),
    }
    storage = DummyStorage(files)
    res = await resolve_file_policy(storage, "file.zip", 1000, policy=EFilePolicy.SKIP_IF_EXISTS)
    assert res.action == EFileAction.REUSE
    assert res.target_file_name == "file.zip"


@pytest.mark.asyncio
async def test_resolve_policy_auto_rename():
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=1000, created_at=datetime.now(UTC)),
    }
    storage = DummyStorage(files)
    res = await resolve_file_policy(storage, "file.zip", 1000, policy=EFilePolicy.AUTO_RENAME)
    assert res.action == EFileAction.DOWNLOAD
    assert res.target_file_name == "file_1.zip"
    assert res.is_renamed is True


@pytest.mark.asyncio
async def test_resolve_synthetic_name_size_mismatch_auto_renames():
    files = {
        "api_data.bin": StoredFileInfo(
            key="api_data.bin", size_bytes=2000, created_at=datetime.now(UTC)
        ),
    }
    storage = DummyStorage(files)
    # is_generated_name=True and expected_size is 5000 (different from 2000)
    res = await resolve_file_policy(
        storage,
        "api_data.bin",
        expected_size=5000,
        policy=EFilePolicy.SMART_REUSE,
        is_generated_name=True,
    )
    assert res.action == EFileAction.DOWNLOAD
    assert res.target_file_name == "api_data_1.bin"
    assert res.is_renamed is True


@pytest.mark.asyncio
async def test_resolve_policy_reuse_same_size():
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=1000, created_at=datetime.now(UTC)),
    }
    storage = DummyStorage(files)

    # Size match -> REUSE
    res_ok = await resolve_file_policy(
        storage, "file.zip", 1000, policy=EFilePolicy.REUSE_SAME_SIZE
    )
    assert res_ok.action == EFileAction.REUSE

    # Size mismatch -> ERROR
    res_err = await resolve_file_policy(
        storage, "file.zip", 2000, policy=EFilePolicy.REUSE_SAME_SIZE
    )
    assert res_err.action == EFileAction.ERROR


@pytest.mark.asyncio
async def test_resolve_policy_smart_reuse_fresh():
    ref_time = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
    local_time = ref_time - timedelta(hours=2)  # 2h ago (< 24h)
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=1000, created_at=local_time),
    }
    storage = DummyStorage(files)

    res = await resolve_file_policy(
        storage,
        "file.zip",
        1000,
        policy=EFilePolicy.SMART_REUSE,
        reference_time=ref_time,
    )
    assert res.action == EFileAction.REUSE


@pytest.mark.asyncio
async def test_resolve_policy_smart_reuse_stale():
    ref_time = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
    local_time = ref_time - timedelta(days=30)  # 30 days ago (> 24h)
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=1000, created_at=local_time),
    }
    storage = DummyStorage(files)

    res = await resolve_file_policy(
        storage,
        "file.zip",
        1000,
        policy=EFilePolicy.SMART_REUSE,
        reference_time=ref_time,
    )
    assert res.action == EFileAction.ERROR


@pytest.mark.asyncio
async def test_resolve_policy_reuse_or_update():
    ref_time = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
    # Stale file (30 days ago)
    local_time = ref_time - timedelta(days=30)
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=1000, created_at=local_time),
    }
    storage = DummyStorage(files)

    # REUSE_OR_UPDATE with stale file -> DOWNLOAD (will update in place)
    res = await resolve_file_policy(
        storage,
        "file.zip",
        1000,
        policy=EFilePolicy.REUSE_OR_UPDATE,
        reference_time=ref_time,
    )
    assert res.action == EFileAction.DOWNLOAD
    assert res.target_file_name == "file.zip"
    assert res.is_renamed is False


@pytest.mark.asyncio
async def test_resolve_real_filesystem_integration(tmp_path: Path):
    storage = LocalStorage(storage_dir=str(tmp_path))
    file_name = "real_target.bin"

    # Create initial file
    async def sample_gen():
        yield b"0" * 1024

    await storage.save_binary_data(file_name, sample_gen())

    # 1. SMART_REUSE on fresh file -> REUSE
    res_reuse = await resolve_file_policy(storage, file_name, 1024, policy=EFilePolicy.SMART_REUSE)
    assert res_reuse.action == EFileAction.REUSE

    # 2. AUTO_RENAME -> finds real_target_1.bin
    res_rename = await resolve_file_policy(storage, file_name, 1024, policy=EFilePolicy.AUTO_RENAME)
    assert res_rename.action == EFileAction.DOWNLOAD
    assert res_rename.target_file_name == "real_target_1.bin"
    assert res_rename.is_renamed is True
