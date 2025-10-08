from collections.abc import AsyncIterable
from pathlib import Path
from typing import Protocol
from sDownload.interfaces.protocols.filesystem_info_model import FileSystemInfoModel


class FileStorageProtocol(Protocol):

    def get_binary_data(self, key: str) -> AsyncIterable[bytes]:
        """
        Retrieve binary data as an asynchronous stream (in chunks).

        :param key: Identifier of the stored data.
        :return: Async iterator of byte chunks.
        """ 
        ...

    async def save_binary_data(
        self, key: str, data: AsyncIterable[bytes]
    ) -> None:
        """
        Save binary data (e.g., files) to the storage.

        :param key: Unique key for the data.
        :param data: Binary content to store.
        """
        ...

    async def delete_data(self, key: str) -> None:
        """
        Delete binary data associated with the given key.

        :param key: Identifier of the data to delete.
        :return: An awaitable that completes once the data is deleted.
        """
        ...

    async def list_data(self) -> list[FileSystemInfoModel]:
        """
        List all stored data in the storage.

        :return: An awaitable that yields a list of FileInfoModel objects
        {key, sizeBytes, created_at}.
        """
        ...

    async def merge_binary_files(self, source_keys: list[str], dest_key: str) -> None:
        """
        Merge multiple binary files into a single file.

        :param source_keys: List of keys of the source files.
        :param dest_key: Key of the destination file.
        :return: An awaitable that completes once the merge is complete.
        """
        ...

    async def shrink_file_to(self, key: str, target_size_bytes: int) -> None:
        """
        Shrink a file to a specific size.

        :param key: Key of the file to shrink.
        :param target_size_bytes: Target size in bytes.
        :return: An awaitable that completes once the shrink is complete.
        """
        ...
