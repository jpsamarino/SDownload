import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from pathlib import Path
from datetime import datetime
import aiofiles
import aiofiles.os
import os
from sDownload.interfaces.protocols import (
    FileRangeParams,
    FileStorageProtocol,
)
from sDownload.interfaces.models import StoredFileInfo


class LocalStorage(FileStorageProtocol):
    def __init__(
        self,
        storage_dir: Path | str = "storage",
        chunk_size: int = 8 * 1024,
        io_buffer_size: int = 1024 * 1024,  # 1MB for heavy operations
    ):
        """
        :param storage_dir: path that will be used to store data.
        :param chunk_size: size of the chunks in bytes for streaming.
        :param io_buffer_size: size of the buffer for heavy I/O operations (crop/merge).
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        self.io_buffer_size = io_buffer_size

    async def get_binary_data(self, key: str) -> AsyncIterable[bytes]:
        path = self.storage_dir / key
        if not await aiofiles.os.path.exists(path):
            raise FileNotFoundError(f"Read operation failed: {key} not found at {path}")
        async with aiofiles.open(path, "rb") as f:
            while True:
                chunk = await f.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk

    async def save_binary_data(self, key: str, data: AsyncIterable[bytes]) -> None:
        path = self.storage_dir / key
        async with aiofiles.open(path, "wb") as f:
            try:
                async for chunk in data:
                    await asyncio.shield(f.write(chunk))
            finally:
                await f.flush()
                await asyncio.to_thread(os.fsync, f.fileno())

    async def delete_data(self, key: str) -> None:
        path = self.storage_dir / key
        try:
            await aiofiles.os.remove(path)
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Delete operation failed: {key} not found at {path}"
            ) from e

    async def list_data(self) -> list[StoredFileInfo]:
        def blocking_list():
            files: list[StoredFileInfo] = []
            for path in self.storage_dir.iterdir():
                if not path.is_file():
                    continue
                stat = path.stat()
                info = StoredFileInfo(
                    key=path.name,
                    size_bytes=stat.st_size,
                    created_at=datetime.fromtimestamp(stat.st_ctime),
                )
                files.append(info)
            return files

        return await asyncio.to_thread(blocking_list)

    async def get_data_info(self, key: str) -> StoredFileInfo | None:
        path = self.storage_dir / key
        if not await aiofiles.os.path.exists(path):
            return None

        stat = await aiofiles.os.stat(path)
        return StoredFileInfo(
            key=key,
            size_bytes=stat.st_size,
            created_at=datetime.fromtimestamp(stat.st_ctime),
        )

    async def merge_binary_files(self, source_keys: list[str], dest_key: str) -> None:
        configs = [FileRangeParams(key=k) for k in source_keys]
        await self.merge_ranges(configs, dest_key)

    async def merge_ranges(
        self, source_configs: list[FileRangeParams], dest_key: str
    ) -> None:
        dest_path = self.storage_dir / dest_key
        operation_buffer_size = self.io_buffer_size

        async with aiofiles.open(dest_path, "wb") as dest_file:
            try:
                for config in source_configs:
                    src_path = self.storage_dir / config.key
                    if not await aiofiles.os.path.exists(src_path):
                        raise FileNotFoundError(
                            f"Merge operation failed: source {config.key} not found at {src_path}"
                        )

                    async with aiofiles.open(src_path, "rb") as src_file:
                        if config.start_byte is not None and config.start_byte > 0:
                            await src_file.seek(config.start_byte)

                        remaining_to_read = None
                        if config.end_byte is not None:
                            start = config.start_byte or 0
                            remaining_to_read = config.end_byte - start + 1
                            if remaining_to_read < 0:
                                raise ValueError(
                                    f"Invalid range for {config.key}: start {start}, end {config.end_byte}"
                                )

                        while True:
                            read_size = operation_buffer_size
                            if (
                                remaining_to_read is not None
                                and remaining_to_read < read_size
                            ):
                                read_size = remaining_to_read

                            chunk = await src_file.read(read_size)
                            if not chunk:
                                break

                            await asyncio.shield(dest_file.write(chunk))

                            if remaining_to_read is not None:
                                remaining_to_read -= len(chunk)
                                if remaining_to_read <= 0:
                                    break
            finally:
                await dest_file.flush()
                await asyncio.to_thread(os.fsync, dest_file.fileno())

    async def shrink_file_to(self, key: str, target_size_bytes: int) -> None:
        path = self.storage_dir / key
        if not await aiofiles.os.path.exists(path):
            raise FileNotFoundError(
                f"Shrink operation failed: {key} not found at {path}"
            )

        file_stat = await aiofiles.os.stat(path)
        current_size = file_stat.st_size
        if target_size_bytes >= current_size:
            return

        def do_truncate():
            with path.open("rb+") as f:
                f.truncate(target_size_bytes)
                f.flush()
                os.fsync(f.fileno())

        await asyncio.to_thread(do_truncate)

    async def move_data(self, source_key: str, dest_key: str) -> None:
        source_path = self.storage_dir / source_key
        dest_path = self.storage_dir / dest_key

        if not await aiofiles.os.path.exists(source_path):
            raise FileNotFoundError(
                f"Move operation failed: source {source_key} not found at {source_path}"
            )

        await aiofiles.os.replace(source_path, dest_path)

    async def crop_file(self, key: str, start_byte: int, end_byte: int) -> None:
        target_size = end_byte - start_byte + 1
        path = self.storage_dir / key

        if target_size < 0 or end_byte < 0 or start_byte < 0 or start_byte > end_byte:
            raise ValueError(
                "Parameters must be greater than 0 and start byte must be less than end byte"
            )

        if not await aiofiles.os.path.exists(path):
            raise FileNotFoundError(f"Crop operation failed: {key} not found at {path}")

        file_stat = await aiofiles.os.stat(path)
        current_size = file_stat.st_size

        if end_byte >= current_size:
            raise ValueError(
                f"Crop operation failed: end_byte {end_byte} exceeds file size {current_size}"
            )

        if start_byte == 0:
            return await self.shrink_file_to(key, target_size)

        async with aiofiles.open(path, "r+b") as f:
            operation_buffer_size = self.io_buffer_size
            full_blocks = target_size // operation_buffer_size
            remainder = target_size % operation_buffer_size

            write_pos = 0
            read_pos = start_byte

            try:
                for _ in range(full_blocks):
                    await f.seek(read_pos)
                    data = await f.read(operation_buffer_size)

                    await f.seek(write_pos)
                    await f.write(data)

                    read_pos += operation_buffer_size
                    write_pos += operation_buffer_size

                if remainder > 0:
                    await f.seek(read_pos)
                    data = await f.read(remainder)
                    await f.seek(write_pos)
                    await f.write(data)
            finally:
                await f.truncate(target_size)
                await f.flush()
                await asyncio.to_thread(os.fsync, f.fileno())
