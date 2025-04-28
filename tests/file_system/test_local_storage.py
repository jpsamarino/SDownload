
from datetime import datetime
from pathlib import Path
import pytest
from sDownload.file_system.local_storage import LocalStorage
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.interfaces.protocols.filesystem_info_model import FileSystemInfoModel


async def generate_chunks(data: bytes, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i: i + chunk_size]


@pytest.fixture
def storage(tmp_path: str):
    return LocalStorage(storage_dir=tmp_path, chunk_size=4)


@pytest.mark.asyncio
async def test_save_and_get_binary_data(storage: FileStorageProtocol, tmp_path: Path):
    key = "hello.bin"
    data = b"abcdefghijklmnopqrstuvwxyz"

    await storage.save_binary_data(key, generate_chunks(data, storage.chunk_size))

    path = tmp_path / key
    assert path.exists()

    received = b""
    async for chunk in storage.get_binary_data(key):
        received += chunk

    assert received == data


@pytest.mark.asyncio
async def test_list_data(storage: FileStorageProtocol, tmp_path: Path):
    content1 = b"1234"
    content2 = b"abcd"

    await storage.save_binary_data("a.bin", generate_chunks(content1, storage.chunk_size))
    await storage.save_binary_data("b.bin", generate_chunks(content2, storage.chunk_size))

    infos = await storage.list_data()
    assert len(infos) == 2
    keys = {info.key for info in infos}
    assert keys == {"a.bin", "b.bin"}

    for info in infos:
        assert isinstance(info, FileSystemInfoModel)
        expected_size = content1 if info.key == "a.bin" else content2
        assert info.size_bytes == len(expected_size)
        assert isinstance(info.created_at, datetime)


@pytest.mark.asyncio
async def test_delete_data(storage: FileStorageProtocol, tmp_path: Path):
    key = "to_delete.bin"
    data = b"delete me"

    await storage.save_binary_data(key, generate_chunks(data, storage.chunk_size))
    path = tmp_path / key
    assert path.exists()

    await storage.delete_data(key)
    assert not path.exists()
    # delete again should not raise
    await storage.delete_data(key)


@pytest.mark.asyncio
async def test_get_binary_data_not_found(storage: FileStorageProtocol):
    with pytest.raises(FileNotFoundError):
        async for _ in storage.get_binary_data("nope.bin"):
            pass
