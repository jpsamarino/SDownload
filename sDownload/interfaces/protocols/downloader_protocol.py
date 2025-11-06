from collections.abc import AsyncGenerator
from typing import Protocol
from .file_info_model import FileInfoModel


class DownloaderProtocol(Protocol):
    def download_chunk(
        self,
        url: str,
        start_byte: int = 0,
        end_byte: int | None = None,
        file_id: str | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Download a chunk of data from a specified URL.

        This method allows downloading large files in smaller, manageable chunks. It
        supports specifying the byte range to download, as well as retrying on failure.

        Args:
            url (str): The URL to download the data from.
            startByte (int, optional): The starting byte position of the chunk to download.
                Defaults to 0, meaning the download will start from the beginning of the file.
            endByte (int, optional): The ending byte position of the chunk to download.
                Defaults to None, meaning the download will continue until the end of the file.
            retries (int, optional): The number of retry attempts to make in case of failure.
                Defaults to 3. This allows for resilience when encountering transient errors.

        Yields:
            AsyncIterator[bytes]: An asynchronous iterator over the downloaded bytes.
            Each chunk yielded will be a `bytes` object, representing a portion of the file.

        Raises:
            DownloadError: If the download fails after the specified number of retries.
        """
        ...

    async def get_file_info(self, url: str) -> list[FileInfoModel]:
        """
        Retrieve metadata information about a file from a specified URL.

        This method fetches details about a file, such as its name, size, content type,
        and other relevant information, without downloading the actual file.

        Args:
            url (str): The URL from which to fetch the file's metadata.

        Returns:
            FileInfoModel: A `FileInfoModel` instance containing the file's metadata
            (name, size, content type, etc.).

        Raises:
            FileInfoRetrievalError: If there is an error retrieving the file information.
        """
        ...
