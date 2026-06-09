import pytest
from types import SimpleNamespace
from sDownload.services.downloader_manager.chunk_utils.cleanup import cleanup_temp_files
from sDownload.file_system.local_storage import LocalStorage


async def iter_helper(data):
    yield data


@pytest.fixture
def temp_storage(tmp_path):
    return LocalStorage(tmp_path)


@pytest.mark.asyncio
async def test_cleanup_temp_files_success(temp_storage):
    """Scenario 1: Cleans up files that exist in storage but keeps unrelated files"""
    f1, f2, f_keep = "f1.bin", "f2.bin", "keep_me.bin"

    # Create files in storage
    await temp_storage.save_binary_data(f1, iter_helper(b"data1"))
    await temp_storage.save_binary_data(f2, iter_helper(b"data2"))
    await temp_storage.save_binary_data(f_keep, iter_helper(b"data_keep"))

    # Stats objects only need chunk_file_name
    stats_list = [
        SimpleNamespace(chunk_file_name=f1),
        SimpleNamespace(chunk_file_name=f2),
        SimpleNamespace(chunk_file_name="nonexistent.bin"),
    ]

    await cleanup_temp_files(temp_storage, stats_list)

    storage_files = {f.key for f in await temp_storage.list_data()}
    assert f1 not in storage_files
    assert f2 not in storage_files
    assert f_keep in storage_files


@pytest.mark.asyncio
async def test_cleanup_temp_files_empty(temp_storage):
    """Scenario 2: Empty input list"""
    await temp_storage.save_binary_data("stay.bin", iter_helper(b"data"))

    await cleanup_temp_files(temp_storage, [])

    storage_files = {f.key for f in await temp_storage.list_data()}
    assert "stay.bin" in storage_files
