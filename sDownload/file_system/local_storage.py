import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from pathlib import Path
from datetime import datetime
import aiofiles
import aiofiles.os
import os
from sDownload.interfaces.protocols.file_storage_protocol import FileStorageProtocol
from sDownload.interfaces.protocols.filesystem_info_model import FileSystemInfoModel


class LocalStorage(FileStorageProtocol):
    def __init__(self, storage_dir: Path | str = "storage", chunk_size: int = 8 * 1024):
        """
        :param storage_dir: path that will be used to store data.
        :param chunk_size: size of the chunks in bytes.
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size

    async def get_binary_data(self, key: str) -> AsyncIterable[bytes]:
        path = self.storage_dir / key
        if not path.exists():
            raise FileNotFoundError(f"{key} not found in storage")
        async with aiofiles.open(path, "rb") as f:
            while True:
                chunk = await f.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk

    async def save_binary_data(self, key: str, data: AsyncIterable[bytes]) -> None:
        path = self.storage_dir / key
        async with aiofiles.open(path, "wb") as f:
            async for chunk in data:
                await f.write(chunk)
            await f.flush()
            await asyncio.to_thread(os.fsync, f.fileno())

    async def delete_data(self, key: str) -> None:
        path = self.storage_dir / key
        try:
            path.unlink()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"{key} not found in storage: {path}") from e

    async def list_data(self) -> list[FileSystemInfoModel]:
        files: list[FileSystemInfoModel] = []
        for path in self.storage_dir.iterdir():
            if not path.is_file():
                continue
            stat = path.stat()
            info = FileSystemInfoModel(
                key=path.name,
                size_bytes=stat.st_size,
                created_at=datetime.fromtimestamp(stat.st_ctime),
            )
            files.append(info)
        return files

    async def merge_binary_files(self, source_keys: list[str], dest_key: str) -> None:
        dest_path = self.storage_dir / dest_key
        async with aiofiles.open(dest_path, "wb") as dest_file:
            for key in source_keys:
                src_path = self.storage_dir / key
                if not src_path.exists():
                    raise FileNotFoundError(f"{key} not found in storage")
                async with aiofiles.open(src_path, "rb") as src_file:
                    while True:
                        chunk = await src_file.read(self.chunk_size)
                        if not chunk:
                            break
                        await dest_file.write(chunk)
            await dest_file.flush()
            await asyncio.to_thread(os.fsync, dest_file.fileno())

    async def shrink_file_to(self, key: str, target_size_bytes: int) -> None:
        path = self.storage_dir / key
        if not path.exists():
            raise FileNotFoundError(f"{key} not found in storage")

        current_size = path.stat().st_size
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

        if not source_path.exists():
            raise FileNotFoundError(f"{source_key} not found in storage")

        await aiofiles.os.replace(source_path, dest_path)

    async def crop_file(self, key: str, start_byte: int, end_byte: int) -> None:
        target_size = end_byte - start_byte + 1
        path = self.storage_dir / key
        current_size = path.stat().st_size

        if target_size < 0 or end_byte < 0 or start_byte < 0 or start_byte > end_byte:
            raise ValueError(
                "Parameters must be greater than 0 and start byte must be less than end byte"
            )

        if end_byte >= current_size:
            raise ValueError("End byte must be less than current size")

        if start_byte == 0:
            return await self.shrink_file_to(key, target_size)

        if not path.exists():
            raise FileNotFoundError(f"File {key} not found for cropping")

        async with aiofiles.open(path, "r+b") as f:
            buffer_size = 1024 * 1024  # 1MB
            full_blocks = target_size // buffer_size
            remainder = target_size % buffer_size

            write_pos = 0
            read_pos = start_byte

            for _ in range(full_blocks):
                await f.seek(read_pos)
                data = await f.read(buffer_size)

                await f.seek(write_pos)
                await f.write(data)

                read_pos += buffer_size
                write_pos += buffer_size

            if remainder > 0:
                await f.seek(read_pos)
                data = await f.read(remainder)
                await f.seek(write_pos)
                await f.write(data)

            await f.truncate(target_size)
            await f.flush()
            await asyncio.to_thread(os.fsync, f.fileno())
