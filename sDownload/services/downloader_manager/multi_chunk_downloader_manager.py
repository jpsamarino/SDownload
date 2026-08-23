import logging
from urllib.parse import urlparse

from sDownload.file_system.local_storage import LocalStorage
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.models import DLManagerConfig
from sDownload.interfaces.protocols import (
    DownloaderManagerProtocol,
    DownloaderProtocol,
    FileStorageProtocol,
)


class MultiChunkDownloader(DownloaderManagerProtocol):
    def __init__(
        self,
        config: DLManagerConfig = None,
        http_executor: DownloaderProtocol = None,
        ftp_executor: DownloaderProtocol = None,
        sftp_executor: DownloaderProtocol = None,
        torrent_executor: DownloaderProtocol = None,
        file_storage_executor: FileStorageProtocol = None,
    ):

        self.config = config or DLManagerConfig()
        self.http_executor = http_executor or HttpxDownloader(config.http_config)
        self.ftp_executor = ftp_executor
        self.sftp_executor = sftp_executor
        self.torrent_executor = torrent_executor
        self.file_storage_executor = file_storage_executor or LocalStorage(
            storage_dir=self.config.destination_folder
        )
        self.file_storage_handler = LocalStorage(storage_dir=self.config.destination_handler)
        self._logger = config.logger or logging.getLogger(__name__)

        def _get_executor(url: str) -> DownloaderProtocol:
            scheme = urlparse(url).scheme.lower()
            match scheme:
                case "http" | "https":
                    return self.http_executor
                case "ftp":
                    return self.ftp_executor
                case "sftp":
                    return self.sftp_executor
                case "magnet":
                    return self.torrent_executor
                case _:
                    return self.http_executor
