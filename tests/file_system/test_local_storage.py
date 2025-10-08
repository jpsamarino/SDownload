
from datetime import datetime
from pathlib import Path
import pytest
from sDownload.file_system.local_storage import LocalStorage
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.interfaces.protocols.filesystem_info_model import FileSystemInfoModel


async def generate_chunks(data: bytes, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i: i + chunk_size]

default_test_chunk_size = 4

@pytest.fixture
def storage(tmp_path: str):
    return LocalStorage(storage_dir=tmp_path, chunk_size=default_test_chunk_size)


@pytest.mark.asyncio
async def test_save_and_get_binary_data(storage: FileStorageProtocol, tmp_path: Path):
    key = "hello.bin"
    data = b"abcdefghijklmnopqrstuvwxyz"

    await storage.save_binary_data(key, generate_chunks(data, default_test_chunk_size))

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

    await storage.save_binary_data("a.bin", generate_chunks(content1, default_test_chunk_size))
    await storage.save_binary_data("b.bin", generate_chunks(content2, default_test_chunk_size))

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

    await storage.save_binary_data(key, generate_chunks(data, default_test_chunk_size))
    path = tmp_path / key
    assert path.exists()

    await storage.delete_data(key)
    assert not path.exists()
    # delete again should not raise ?
    # await storage.delete_data(key)


@pytest.mark.asyncio
async def test_get_binary_data_not_found(storage: FileStorageProtocol):
    with pytest.raises(FileNotFoundError):
        async for _ in storage.get_binary_data("nope.bin"):
            pass


@pytest.mark.asyncio
async def test_merge_three_parts(storage: LocalStorage):
    part1 = b"ABC"
    part2 = b"DEFG"
    part3 = b"HIJKLM"
    await storage.save_binary_data("p1.bin", generate_chunks(part1, default_test_chunk_size))
    await storage.save_binary_data("p2.bin", generate_chunks(part2, default_test_chunk_size))
    await storage.save_binary_data("p3.bin", generate_chunks(part3, default_test_chunk_size))

    await storage.merge_binary_files(["p1.bin", "p2.bin", "p3.bin"], "merged.bin")

    merged = b""
    async for chunk in storage.get_binary_data("merged.bin"):
        merged += chunk
    assert merged == part1 + part2 + part3


@pytest.mark.asyncio
async def test_merge_overwrites_existing(storage: LocalStorage):
    existing = b"OLD"
    await storage.save_binary_data("dest.bin", generate_chunks(existing, default_test_chunk_size))

    new1 = b"123"
    new2 = b"4567"
    await storage.save_binary_data("n1.bin", generate_chunks(new1, default_test_chunk_size))
    await storage.save_binary_data("n2.bin", generate_chunks(new2, default_test_chunk_size))

    await storage.merge_binary_files(["n1.bin", "n2.bin"], "dest.bin")

    merged = b""
    async for chunk in storage.get_binary_data("dest.bin"):
        merged += chunk
    assert merged == new1 + new2


@pytest.mark.asyncio
async def test_shrink_file_to_file_not_found(storage: LocalStorage):
    with pytest.raises(FileNotFoundError):
        await storage.shrink_file_to("no_such_file.txt", 10)


@pytest.mark.asyncio
async def test_shrink_file_to_no_truncation_if_target_larger(storage: LocalStorage):

    file_name = "testfile.bin"
    data = b"1234567890"
    await storage.save_binary_data(file_name, generate_chunks(data, default_test_chunk_size))
    await storage.shrink_file_to(file_name, 15)

    merged = b""
    async for chunk in storage.get_binary_data(file_name):
        merged += chunk
    assert merged == data


@pytest.mark.asyncio
async def test_shrink_file_to_truncates_correctly(storage: LocalStorage):
    file_name = "file_to_truncate.bin"
    data = b"abcdefghij"
    await storage.save_binary_data(file_name, generate_chunks(data, default_test_chunk_size))
    await storage.shrink_file_to(file_name, 6)

    merged = b""
    async for chunk in storage.get_binary_data(file_name):
        merged += chunk
    assert merged == b"abcdef"
