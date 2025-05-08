from pathlib import Path
from datetime import datetime
from typing import AsyncIterator, List
import aiofiles
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

    async def get_binary_data(self, key: str) -> AsyncIterator[bytes]:
        path = self.storage_dir / key
        if not path.exists():
            raise FileNotFoundError(f"{key} not found in storage")
        async with aiofiles.open(path, "rb") as f:
            while True:
                chunk = await f.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk

    async def save_binary_data(self, key: str, data: AsyncIterator[bytes]) -> None:
        path = self.storage_dir / key
        async with aiofiles.open(path, "wb") as f:
            async for chunk in data:
                await f.write(chunk)

    async def delete_data(self, key: str) -> None:
        path = self.storage_dir / key
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    async def list_data(self) -> List[FileSystemInfoModel]:
        files: List[FileSystemInfoModel] = []
        for path in self.storage_dir.iterdir():
            if not path.is_file():
                continue
            stat = path.stat()
            info = FileSystemInfoModel(
                key=path.name,
                size_bytes=stat.st_size,
                created_at=datetime.fromtimestamp(stat.st_birthtime),
            )
            files.append(info)
        return files

    async def merge_binary_files(self, source_keys: List[str], dest_key: str) -> None:
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
