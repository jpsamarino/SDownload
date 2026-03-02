from datetime import datetime
from pathlib import Path
import asyncio
import pytest
from sDownload.file_system.local_storage import LocalStorage
from sDownload.interfaces.protocols import (
    FileRangeParams,
    FileStorageProtocol,
)
from sDownload.interfaces.models import StoredFileInfo


async def generate_chunks(data: bytes, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


default_test_chunk_size = 4


async def huge_stream(total_bytes: int, chunk_size: int = 64 * 1024):
    sent = 0
    chunk = b"x" * chunk_size
    while sent < total_bytes:
        remaining = total_bytes - sent
        yield chunk if remaining >= chunk_size else chunk[:remaining]
        sent += chunk_size


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

    await storage.save_binary_data(
        "a.bin", generate_chunks(content1, default_test_chunk_size)
    )
    await storage.save_binary_data(
        "b.bin", generate_chunks(content2, default_test_chunk_size)
    )

    infos = await storage.list_data()
    assert len(infos) == 2
    keys = {info.key for info in infos}
    assert keys == {"a.bin", "b.bin"}

    for info in infos:
        assert isinstance(info, StoredFileInfo)
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
    await storage.save_binary_data(
        "p1.bin", generate_chunks(part1, default_test_chunk_size)
    )
    await storage.save_binary_data(
        "p2.bin", generate_chunks(part2, default_test_chunk_size)
    )
    await storage.save_binary_data(
        "p3.bin", generate_chunks(part3, default_test_chunk_size)
    )

    await storage.merge_binary_files(["p1.bin", "p2.bin", "p3.bin"], "merged.bin")

    merged = b""
    async for chunk in storage.get_binary_data("merged.bin"):
        merged += chunk
    assert merged == part1 + part2 + part3


@pytest.mark.asyncio
async def test_merge_overwrites_existing(storage: LocalStorage):
    existing = b"OLD"
    await storage.save_binary_data(
        "dest.bin", generate_chunks(existing, default_test_chunk_size)
    )

    new1 = b"123"
    new2 = b"4567"
    await storage.save_binary_data(
        "n1.bin", generate_chunks(new1, default_test_chunk_size)
    )
    await storage.save_binary_data(
        "n2.bin", generate_chunks(new2, default_test_chunk_size)
    )

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
    await storage.save_binary_data(
        file_name, generate_chunks(data, default_test_chunk_size)
    )
    await storage.shrink_file_to(file_name, 15)

    merged = b""
    async for chunk in storage.get_binary_data(file_name):
        merged += chunk
    assert merged == data


@pytest.mark.asyncio
async def test_shrink_file_to_truncates_correctly(storage: LocalStorage):
    file_name = "file_to_truncate.bin"
    data = b"abcdefghij"
    await storage.save_binary_data(
        file_name, generate_chunks(data, default_test_chunk_size)
    )
    await storage.shrink_file_to(file_name, 6)

    merged = b""
    async for chunk in storage.get_binary_data(file_name):
        merged += chunk
    assert merged == b"abcdef"


@pytest.mark.asyncio
async def test_move_data_renames_file(storage: LocalStorage, tmp_path: Path):
    source_key = "source.bin"
    dest_key = "dest.bin"
    data = b"content to move"

    await storage.save_binary_data(
        source_key, generate_chunks(data, default_test_chunk_size)
    )

    assert (tmp_path / source_key).exists()
    assert not (tmp_path / dest_key).exists()

    await storage.move_data(source_key, dest_key)

    assert not (tmp_path / source_key).exists()
    assert (tmp_path / dest_key).exists()

    # Verify content
    content = b""
    async for chunk in storage.get_binary_data(dest_key):
        content += chunk
    assert content == data


@pytest.mark.asyncio
async def test_move_data_overwrites_existing(storage: LocalStorage, tmp_path: Path):
    source_key = "source.bin"
    dest_key = "dest.bin"

    data_source = b"new content"
    data_dest = b"old content"

    await storage.save_binary_data(
        source_key, generate_chunks(data_source, default_test_chunk_size)
    )
    await storage.save_binary_data(
        dest_key, generate_chunks(data_dest, default_test_chunk_size)
    )

    await storage.move_data(source_key, dest_key)

    assert not (tmp_path / source_key).exists()
    assert (tmp_path / dest_key).exists()

    # Verify content overrides
    content = b""
    async for chunk in storage.get_binary_data(dest_key):
        content += chunk
    assert content == data_source


@pytest.mark.asyncio
async def test_move_data_source_not_found(storage: LocalStorage):
    with pytest.raises(FileNotFoundError):
        await storage.move_data("non_existent.bin", "dest.bin")


@pytest.mark.asyncio
async def test_crop_file_head_and_tail(storage: LocalStorage):
    key = "crop_test.bin"
    # 0123456789 (indices)
    data = b"abcdefghij"  # 10 bytes
    await storage.save_binary_data(key, generate_chunks(data, default_test_chunk_size))

    # Crop from index 2 to 7 -> "cdefgh" (6 bytes)
    await storage.crop_file(key, 2, 7)

    received = b""
    async for chunk in storage.get_binary_data(key):
        received += chunk
    assert received == b"cdefgh"
    assert len(received) == 6


@pytest.mark.asyncio
async def test_crop_file_only_head(storage: LocalStorage):
    key = "crop_head.bin"
    data = b"0123456789"
    await storage.save_binary_data(key, generate_chunks(data, default_test_chunk_size))

    # Crop from index 3 to the end -> "3456789"
    await storage.crop_file(key, 3, 9)

    received = b""
    async for chunk in storage.get_binary_data(key):
        received += chunk
    assert received == b"3456789"


@pytest.mark.asyncio
async def test_crop_file_only_tail(storage: LocalStorage):
    key = "crop_tail.bin"
    data = b"0123456789"
    await storage.save_binary_data(key, generate_chunks(data, default_test_chunk_size))

    # Crop from index 0 to 4 -> "01234" (Delegates to shrink_file_to)
    await storage.crop_file(key, 0, 4)

    received = b""
    async for chunk in storage.get_binary_data(key):
        received += chunk
    assert received == b"01234"


@pytest.mark.asyncio
async def test_crop_file_validation_negative_values(storage: LocalStorage):
    key = "neg_test.bin"
    await storage.save_binary_data(
        key, generate_chunks(b"0123456789", default_test_chunk_size)
    )

    with pytest.raises(
        ValueError,
    ):
        await storage.crop_file(key, -1, 5)

    with pytest.raises(
        ValueError,
    ):
        await storage.crop_file(key, 0, -1)

    with pytest.raises(
        ValueError,
    ):
        await storage.crop_file(key, 5, 4)


@pytest.mark.asyncio
async def test_crop_file_validation_out_of_bounds(storage: LocalStorage):
    key = "bound_test.bin"
    data = b"0123456789"  # 10 bytes
    await storage.save_binary_data(key, generate_chunks(data, 2))

    # End byte 10 is out of bounds for a 10-byte file (indices 0-9)
    with pytest.raises(ValueError):
        await storage.crop_file(key, 0, 10)


@pytest.mark.asyncio
async def test_crop_file_validation_invalid_range(storage: LocalStorage):
    key = "range_test.bin"
    await storage.save_binary_data(key, generate_chunks(b"0123456789", 2))

    # start_byte > end_byte results in target_size <= 0
    with pytest.raises(ValueError):
        await storage.crop_file(key, 5, 4)


@pytest.mark.asyncio
async def test_save_binary_data_cancellation_safety(storage: LocalStorage):
    key = "cancel_lock.bin"

    async def slow_generator():
        yield b"thisIsATestVeryLongStringAndShouldTakeSomeTimeToWrite"
        await asyncio.sleep(0.5)  # Simulate network delay/work
        yield b"RandomPayloadToTakeMoreTime"
        yield b"footer"

    # Start saving in a task
    task = asyncio.create_task(storage.save_binary_data(key, slow_generator()))

    # Wait for the first yield to be written
    await asyncio.sleep(0.1)

    # Cancel the task while it's sleeping in slow_generator
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    # Now verify we can immediately CROP or MOVE the file without block file lock
    # If the finally/close didn't work, this would raise PermissionError on Windows
    await storage.crop_file(key, 0, 3)  # Keep only "head"

    content = b""
    async for chunk in storage.get_binary_data(key):
        content += chunk

    assert content == b"this"


@pytest.mark.asyncio
async def test_save_binary_data_cancellation_keeps_written_data(
    storage, tmp_path: Path
):
    key = "huge_cancel_test.bin"

    TOTAL_BYTES = 200 * 1024 * 1024  # 200 MB
    CHUNK_SIZE = 64 * 1024

    cancel_after_bytes = 50 * 1024 * 1024  # cancel after 50MB

    bytes_sent = 0
    cancel_event = asyncio.Event()

    async def controlled_stream():
        nonlocal bytes_sent
        async for chunk in huge_stream(TOTAL_BYTES, CHUNK_SIZE):
            bytes_sent += len(chunk)
            if bytes_sent >= cancel_after_bytes:
                cancel_event.set()
            yield chunk

    save_task = asyncio.create_task(storage.save_binary_data(key, controlled_stream()))

    await cancel_event.wait()
    save_task.cancel()

    try:
        await save_task
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(0.1)

    path = tmp_path / key
    assert path.exists()

    file_size = path.stat().st_size

    assert file_size > 0
    assert file_size == bytes_sent


@pytest.mark.asyncio
async def test_merge_ranges_basic(storage: LocalStorage):
    # Test identifying normal merge (full files)
    await storage.save_binary_data("p1.bin", generate_chunks(b"0123", 2))
    await storage.save_binary_data("p2.bin", generate_chunks(b"4567", 2))

    configs = [
        FileRangeParams(key="p1.bin", start_byte=0, end_byte=3),
        FileRangeParams(key="p2.bin", start_byte=0, end_byte=3),
    ]
    await storage.merge_ranges(configs, "merged.bin")

    received = b""
    async for chunk in storage.get_binary_data("merged.bin"):
        received += chunk
    assert received == b"01234567"


@pytest.mark.asyncio
async def test_merge_ranges_with_offsets(storage: LocalStorage):
    # Scenario: Chunk A (0-10) and Chunk B (5-15)
    # We want bytes 0-10 from A and 11-15 from B
    data_a = b"abcdefghijk"  # 0-10
    data_b = b"fghijklmnop"  # 5-15 (starts at global 5)

    await storage.save_binary_data("a.bin", generate_chunks(data_a, 4))
    await storage.save_binary_data("b.bin", generate_chunks(data_b, 4))

    # Global 11 is index 6 inside B (11 - 5 = 6)
    # Byte 6 of "fghijklmnop" is 'l'
    # Bytes 6-10 of B are "lmnop"
    configs = [
        FileRangeParams(key="a.bin", start_byte=0, end_byte=10),
        FileRangeParams(key="b.bin", start_byte=6, end_byte=10),
    ]
    await storage.merge_ranges(configs, "smart_merged.bin")

    received = b""
    async for chunk in storage.get_binary_data("smart_merged.bin"):
        received += chunk
    assert received == b"abcdefghijklmnop"


@pytest.mark.asyncio
async def test_merge_ranges_optional_params(storage: LocalStorage):
    await storage.save_binary_data("p1.bin", generate_chunks(b"0123456789", 4))
    await storage.save_binary_data("p2.bin", generate_chunks(b"abcdefghij", 4))

    configs = [
        FileRangeParams(key="p1.bin", end_byte=4),  # 0 to 4 -> "01234"
        FileRangeParams(key="p2.bin", start_byte=5),  # 5 to end -> "fghij"
    ]
    await storage.merge_ranges(configs, "optional.bin")

    received = b""
    async for chunk in storage.get_binary_data("optional.bin"):
        received += chunk
    assert received == b"01234fghij"


@pytest.mark.asyncio
async def test_merge_ranges_invalid_range_raises_error(storage: LocalStorage):
    await storage.save_binary_data("p1.bin", generate_chunks(b"012345", 2))

    configs = [
        FileRangeParams(key="p1.bin", start_byte=5, end_byte=2),
    ]
    with pytest.raises(ValueError, match="Invalid range"):
        await storage.merge_ranges(configs, "error.bin")


@pytest.mark.asyncio
async def test_get_data_info_success(storage: LocalStorage, tmp_path: Path):
    key = "info.bin"
    data = b"metadata test content"
    await storage.save_binary_data(key, generate_chunks(data, 4))

    info = await storage.get_data_info(key)

    assert info is not None
    assert info.key == key
    assert info.size_bytes == len(data)
    assert isinstance(info.created_at, datetime)

    # Cross-verify with actual filesystem
    stat = (tmp_path / key).stat()
    assert info.size_bytes == stat.st_size


@pytest.mark.asyncio
async def test_get_data_info_not_found(storage: LocalStorage):
    info = await storage.get_data_info("missing_file.bin")
    assert info is None


def test_local_storage_init_creates_last_dir(tmp_path: Path):
    dest_dir = tmp_path / "new_subdir"
    assert not dest_dir.exists()

    # Should create "new_subdir" since tmp_path exists
    ls = LocalStorage(storage_dir=dest_dir)
    assert ls.storage_dir.exists()
    assert ls.storage_dir.is_dir()


def test_local_storage_init_fails_on_multi_level_missing(tmp_path: Path):
    dest_dir = tmp_path / "level1" / "level2"
    assert not (tmp_path / "level1").exists()

    # Should fail because "level1" doesn't exist
    with pytest.raises(FileNotFoundError, match="Parent directory"):
        LocalStorage(storage_dir=dest_dir)
