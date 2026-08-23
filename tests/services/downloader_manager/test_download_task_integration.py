import logging
import pytest
from sDownload.interfaces.models import EDownloadStatus, ChunkRange
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.services.downloader_manager.download_task import DownloadTask
from sDownload.file_system.local_storage import LocalStorage
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.exceptions import StorageNotFoundError

# Configuração de log para ajudar na depuração dos testes
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(storage_dir=str(tmp_path))


@pytest.fixture
def downloader():
    from sDownload.interfaces.models import HttpConfigModel

    return HttpxDownloader(HttpConfigModel(timeout_connect_s=20.0))


def test_download_task_initialization_defaults(tmp_path):
    params = DownloadTaskParams(
        url="http://example.com/file.zip",
        dest_dir=str(tmp_path),
    )
    task = DownloadTask(params)

    assert task._params == params
    assert isinstance(task._downloader, HttpxDownloader)
    assert isinstance(task._storage, LocalStorage)
    assert task._storage.storage_dir == tmp_path.resolve()
    assert task._status == EDownloadStatus.PENDING


def test_download_task_initialization_with_non_existent_dir():
    non_existent_dir = "/tmp/non_existent_directory_12345"
    params = DownloadTaskParams(
        url="http://example.com/file.zip",
        dest_dir=non_existent_dir,
    )

    with pytest.raises(StorageNotFoundError):
        DownloadTask(params)


@pytest.mark.asyncio
async def test_download_task_resolve_file_info_real_nginx(tmp_path, nginx_custom):
    url = f"{nginx_custom['http']}/default/file_100k.bin"
    params = DownloadTaskParams(
        url=url,
        dest_dir=str(tmp_path),
        max_conn=4,
        use_chunked=True,
    )
    task = DownloadTask(params)

    info = await task._resolve_file_info()

    assert info.file_name == "file_100k.bin"
    assert info.file_size == 100 * 1024
    assert info.server_accept_ranges is True
    assert task.file_name == "file_100k.bin"
    assert task.status == EDownloadStatus.PENDING
    assert task.stats is not None
    assert task.stats.file_size == 100 * 1024
    assert task._use_chunked is True
    assert task._max_conn == 4


@pytest.mark.asyncio
async def test_download_task_resolve_file_info_no_range_nginx(tmp_path, nginx_custom):
    url = f"{nginx_custom['http']}/no_resume/file_10M.bin"
    params = DownloadTaskParams(
        url=url,
        dest_dir=str(tmp_path),
        max_conn=8,
        use_chunked=True,
    )
    task = DownloadTask(params)

    info = await task._resolve_file_info()

    assert info.file_name == "file_10M.bin"
    assert info.server_accept_ranges is False
    assert task.file_name == "file_10M.bin"
    assert task._use_chunked is False
    assert task._max_conn == 1

