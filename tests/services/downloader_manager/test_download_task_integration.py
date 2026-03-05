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
