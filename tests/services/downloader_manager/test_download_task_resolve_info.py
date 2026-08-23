import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncIterable, Optional
import pytest

from sDownload.exceptions import (
    ResourceNotFoundError,
    CommunicationError,
    FileAlreadyExistsError,
)
from sDownload.interfaces.models import (
    ResourceInfo,
    StoredFileInfo,
    EDownloadStatus,
)
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.interfaces.protocols import DownloaderProtocol, FileStorageProtocol
from sDownload.services.downloader_manager.download_task import DownloadTask


class MockDownloader(DownloaderProtocol):
    def __init__(
        self,
        resources: ResourceInfo | list[ResourceInfo] | None = None,
        exc_to_raise: Exception | None = None,
    ):
        self.resources = resources
        self.exc_to_raise = exc_to_raise
        self.get_file_info_called_with: Optional[str] = None

    async def get_file_info(self, url: str) -> ResourceInfo:
        self.get_file_info_called_with = url
        if self.exc_to_raise:
            raise self.exc_to_raise
        return self.resources

    async def download_chunk(
        self, url: str, start_byte: int = 0, end_byte: int | None = None
    ):
        if False:
            yield b""


class MockStorage(FileStorageProtocol):
    def __init__(self, existing_files: dict[str, StoredFileInfo] | None = None):
        self.existing_files = existing_files or {}

    async def get_data_info(self, key: str) -> Optional[StoredFileInfo]:
        return self.existing_files.get(key)

    async def save_binary_data(self, key: str, data: AsyncIterable[bytes]):
        pass

    async def merge_binary_files(self, source_keys: list[str], dest_key: str):
        pass

    async def delete_data(self, key: str):
        pass

    async def shrink_file_to(self, key: str, target_size_bytes: int):
        pass

    async def list_data(self) -> list[str]:
        return list(self.existing_files.keys())

    def get_binary_data(self, key: str) -> AsyncIterable[bytes]:
        async def empty():
            if False:
                yield b""

        return empty()


@pytest.fixture
def base_resource():
    return ResourceInfo(
        file_name="remote_file.zip",
        file_dir=None,
        file_size=10_000_000,
        file_id="etag-12345",
        download_url="https://example.com/remote_file.zip",
        transmission_protocol="http",
        server_accept_ranges=True,
        file_created_at=datetime.now(timezone.utc),
        protocol_data=None,
    )


@pytest.mark.asyncio
async def test_resolve_file_info_happy_path(tmp_path, base_resource):
    downloader = MockDownloader(base_resource)
    storage = MockStorage()
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        max_conn=4,
        use_chunked=True,
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    resolved = await task._resolve_file_info()

    assert resolved == base_resource
    assert task._resource_info == base_resource
    assert task._file_name == "remote_file.zip"
    assert task._use_chunked is True
    assert task._max_conn == 4
    assert task._dl_stats is not None
    assert task._dl_stats.file_size == 10_000_000
    assert task._status == EDownloadStatus.PENDING


@pytest.mark.asyncio
async def test_resolve_file_info_custom_file_name_override(tmp_path, base_resource):
    downloader = MockDownloader(base_resource)
    storage = MockStorage()
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        file_name="custom_name.tar.gz",
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    await task._resolve_file_info()

    assert task._file_name == "custom_name.tar.gz"


@pytest.mark.asyncio
async def test_resolve_file_info_no_range_support_downgrades_chunking(
    tmp_path, base_resource
):
    base_resource.server_accept_ranges = False
    downloader = MockDownloader(base_resource)
    storage = MockStorage()
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        max_conn=8,
        use_chunked=True,
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    await task._resolve_file_info()

    assert task._use_chunked is False
    assert task._max_conn == 1


@pytest.mark.asyncio
async def test_resolve_file_info_unknown_file_size_downgrades_chunking(
    tmp_path, base_resource
):
    base_resource.file_size = 0
    downloader = MockDownloader(base_resource)
    storage = MockStorage()
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        max_conn=4,
        use_chunked=True,
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    await task._resolve_file_info()

    assert task._use_chunked is False
    assert task._max_conn == 1
    assert task._dl_stats.file_size == 0


@pytest.mark.asyncio
async def test_resolve_file_info_existing_file_trusted_no_overwrite(
    tmp_path, base_resource
):
    # File created just now (age <= 1h, score = 0.5 + 0.3 = 0.8 >= 0.7)
    existing_file = StoredFileInfo(
        key="remote_file.zip",
        size_bytes=10_000_000,
        created_at=datetime.now(timezone.utc),
    )
    downloader = MockDownloader(base_resource)
    storage = MockStorage(existing_files={"remote_file.zip": existing_file})
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        overwrite_existing=False,
        min_trust_score=0.7,
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    resolved = await task._resolve_file_info()

    assert task._status == EDownloadStatus.COMPLETED
    assert task._dl_stats.bytes_downloaded == 10_000_000


@pytest.mark.asyncio
async def test_resolve_file_info_existing_file_untrusted_no_overwrite_raises(
    tmp_path, base_resource
):
    # File created 60 days ago (score = 0.50 < min_trust_score 0.70)
    old_time = datetime.now(timezone.utc) - timedelta(days=60)
    existing_file = StoredFileInfo(
        key="remote_file.zip",
        size_bytes=10_000_000,
        created_at=old_time,
    )
    downloader = MockDownloader(base_resource)
    storage = MockStorage(existing_files={"remote_file.zip": existing_file})
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        overwrite_existing=False,
        min_trust_score=0.7,
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    with pytest.raises(FileAlreadyExistsError):
        await task._resolve_file_info()

    assert task._status == EDownloadStatus.ERROR


@pytest.mark.asyncio
async def test_resolve_file_info_existing_file_untrusted_with_overwrite(
    tmp_path, base_resource
):
    # File created 60 days ago (score = 0.50), but overwrite_existing=True
    old_time = datetime.now(timezone.utc) - timedelta(days=60)
    existing_file = StoredFileInfo(
        key="remote_file.zip",
        size_bytes=10_000_000,
        created_at=old_time,
    )
    downloader = MockDownloader(base_resource)
    storage = MockStorage(existing_files={"remote_file.zip": existing_file})
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        overwrite_existing=True,
        min_trust_score=0.7,
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    await task._resolve_file_info()

    assert task._status == EDownloadStatus.PENDING


@pytest.mark.asyncio
async def test_resolve_file_info_existing_file_size_mismatch_no_overwrite_raises(
    tmp_path, base_resource
):
    existing_file = StoredFileInfo(
        key="remote_file.zip",
        size_bytes=5_000_000,  # Half size
        created_at=datetime.now(timezone.utc),
    )
    downloader = MockDownloader(base_resource)
    storage = MockStorage(existing_files={"remote_file.zip": existing_file})
    params = DownloadTaskParams(
        url="https://example.com/remote_file.zip",
        dest_dir=str(tmp_path),
        overwrite_existing=False,
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    with pytest.raises(FileAlreadyExistsError):
        await task._resolve_file_info()

    assert task._status == EDownloadStatus.ERROR


@pytest.mark.asyncio
async def test_resolve_file_info_empty_resources_raises_error(tmp_path):
    downloader = MockDownloader(resources=None)
    storage = MockStorage()
    params = DownloadTaskParams(
        url="https://example.com/not_found.zip",
        dest_dir=str(tmp_path),
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    with pytest.raises(ResourceNotFoundError):
        await task._resolve_file_info()

    assert task._status == EDownloadStatus.ERROR
    assert task._last_error is not None


@pytest.mark.asyncio
async def test_resolve_file_info_downloader_exception_propagates(tmp_path):
    downloader = MockDownloader(
        exc_to_raise=CommunicationError("DNS Resolution failed")
    )
    storage = MockStorage()
    params = DownloadTaskParams(
        url="https://example.com/error.zip",
        dest_dir=str(tmp_path),
    )
    task = DownloadTask(params, downloader=downloader, storage=storage)

    with pytest.raises(CommunicationError):
        await task._resolve_file_info()

    assert task._status == EDownloadStatus.ERROR
    assert isinstance(task._last_error, CommunicationError)

