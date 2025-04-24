from dataclasses import dataclass
from typing import Protocol

from .http_config_model import HttpConfigModel


@dataclass
class URLConfig:
    url: str
    destination_path: str | None
    group_name: str | None
    max_connections_per_download: int | None
    max_speed_bytes_per_second: int | None
    validate_before_download: bool | None
    processing_pipeline: str | None  # change
    processing_regex: str | None
    output_filename: str | None  # ???
    start_new_session: bool = False


@dataclass
class DLManagerConfig:
    urls_config: list[URLConfig] | None = None
    abort_on_first_error: bool = False  # improve this name
    max_simultaneous_downloads: int | None = 10
    max_connections_per_download: int | None = 5
    auto_start: bool = True
    destination_folder: str | None = None
    destination_handler: object | None = None
    max_speed_bytes_per_second: int | None = None
    run_in_separate_process: bool = False
    show_progress_in_terminal: bool = True
    progress_update_interval_seconds: int = 30
    fix_urls: bool = True
    logger: object | None = None
    http_config: HttpConfigModel | None = None
    ftp_config: object | None = None
    sftp_config: object | None = None
    torrent_config: object | None = None


class DownloaderManagerProtocol(Protocol):

    def add_url(self, urlConfig: str):
        pass

    def add_urls(self, urls: list):
        pass

    def remove_url(self, url: str):
        pass

    async def wait_until_done(self):
        pass

    async def check_url(self, url: str):
        pass

    async def start(self, indexes: list):
        pass

    async def stop(self, indexes: list):
        pass

    async def pause(self, indexes: list):
        pass

    async def get_download_info(self, indexes: int):
        pass
