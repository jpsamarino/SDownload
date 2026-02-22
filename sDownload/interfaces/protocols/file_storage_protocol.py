from collections.abc import AsyncIterable
from typing import Protocol, NamedTuple
from sDownload.interfaces.models import FileSystemInfoModel


class FileRangeParams(NamedTuple):
    key: str
    start_byte: int | None = None
    end_byte: int | None = None


class FileStorageProtocol(Protocol):

    def get_binary_data(self, key: str) -> AsyncIterable[bytes]:
        """
        Retrieve binary data as an asynchronous stream (in chunks).

        :param key: Identifier of the stored data.
        :return: Async iterator of byte chunks.
        """
        ...

    async def save_binary_data(self, key: str, data: AsyncIterable[bytes]) -> None:
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

    async def get_data_info(self, key: str) -> FileSystemInfoModel | None:
        """
        Get info for a specific item in the storage without listing everything.

        :param key: Unique identifier of the data.
        :return: FileInfoModel if found, else None.
        """
        ...

    async def move_data(self, source_key: str, dest_key: str) -> None:
        """
        Move or rename data from source_key to dest_key.

        This operation should be atomic where possible.
        If dest_key already exists, it should be overwritten.

        :param source_key: Current identifier of the data.
        :param dest_key: New identifier for the data.
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

    async def merge_ranges(
        self, source_configs: list[FileRangeParams], dest_key: str
    ) -> None:
        """
        Merge specific ranges of multiple files into a single destination file.

        :param source_configs: List of FileRangeParams specifying which parts of which files to merge.
        :param dest_key: Key of the destination file.
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

    async def crop_file(self, key: str, start_byte: int, end_byte: int) -> None:
        """
        Crop a file to a specific range.

        :param key: Key of the file to crop.
        :param start_byte: Start byte of the range.
        :param end_byte: End byte of the range.
        :return: An awaitable that completes once the crop is complete.
        """
        ...
