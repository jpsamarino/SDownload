from pathlib import Path
from urllib.parse import urlparse

from sDownload.file_system import LocalStorage
from sDownload.global_settings import global_settings
from sDownload.http_client import HttpxDownloader
from sDownload.interfaces.models import HttpConfigModel
from sDownload.interfaces.protocols import (
    DownloaderProtocol,
    DownloadStrategyProtocol,
    FileStorageProtocol,
    RecoveryProtocol,
)
from sDownload.services.downloader_manager.recovery_download import RecoveryDownload
from sDownload.services.downloader_manager.strategies import (
    MultiChunkDownloadStrategy,
    SingleStreamStrategy,
)


class DefaultComponentProvider:
    """
    Default component provider managing instance caching and factory creation
    for FileStorage, Downloader, DownloadStrategy, and Recovery protocols.

    - Storage: Memoizes the last resolved directory path to avoid repeated disk I/O in batch tasks.
    - Downloader: Reuses a shared HttpxDownloader for standard requests, or instantiates dedicated clients for custom headers.
    - Strategy: Instantiates isolated, stateful strategy objects per task.
    - Recovery: Memoizes the last RecoveryDownload instance bound to the active storage.
    """

    def __init__(self) -> None:
        self._cached_storage_path: Path | None = None
        self._cached_storage: FileStorageProtocol | None = None
        self._cached_recovery: RecoveryProtocol | None = None
        self._default_http_downloader: DownloaderProtocol | None = None

    def get_storage(self, dest_dir: str | Path | None = None) -> FileStorageProtocol:
        """
        Returns a LocalStorage instance, reusing the cached instance if the resolved path matches.
        """
        target_path = Path(dest_dir or global_settings.default_storage_dir).resolve()

        if self._cached_storage is not None and self._cached_storage_path == target_path:
            return self._cached_storage

        new_storage = LocalStorage(storage_dir=target_path)
        self._cached_storage_path = target_path
        self._cached_storage = new_storage
        return new_storage

    def get_downloader(
        self,
        url: str,
        headers: dict[str, str] | None = None,
    ) -> DownloaderProtocol:
        """
        Resolves the appropriate downloader based on the URL scheme, reusing a shared
        instance for default HTTP(S) configurations or creating a new instance for custom headers.
        """
        scheme = urlparse(url).scheme.lower()
        match scheme:
            case "http" | "https" | "":
                if not headers:
                    if self._default_http_downloader is None:
                        self._default_http_downloader = HttpxDownloader()
                    return self._default_http_downloader

                return HttpxDownloader(config=HttpConfigModel(headers=headers))

            case "ftp":
                raise NotImplementedError("FTP protocol is not yet implemented.")
            case "sftp":
                raise NotImplementedError("SFTP protocol is not yet implemented.")
            case _:
                raise ValueError(
                    f"Unsupported URL scheme '{scheme}'. Supported schemes: 'http', 'https'."
                )

    def get_strategy(
        self,
        use_chunked: bool = True,
        max_conn: int = 1,
    ) -> DownloadStrategyProtocol:
        """
        Creates a fresh, stateful download strategy instance for a single DownloadTask.
        """
        if not use_chunked:
            return SingleStreamStrategy()
        return MultiChunkDownloadStrategy(
            max_conn=max_conn,
            use_chunked_download=use_chunked,
        )

    def get_recovery(self, storage: FileStorageProtocol) -> RecoveryProtocol:
        """
        Returns a RecoveryProtocol instance, reusing the cached instance if the storage matches.
        """
        if self._cached_recovery is not None and self._cached_storage is storage:
            return self._cached_recovery

        new_recovery = RecoveryDownload(storage)
        self._cached_recovery = new_recovery
        return new_recovery

    def clear_cache(self) -> None:
        """
        Clears internal memoized instances (useful for test resets).
        """
        self._cached_storage_path = None
        self._cached_storage = None
        self._cached_recovery = None
        self._default_http_downloader = None


# Process-wide singleton instance
default_provider = DefaultComponentProvider()
