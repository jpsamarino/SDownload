from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from .http_config_model import HttpConfigModel


@dataclass
class DownloadConfig:
    file_name: str
    file_dir: Optional[str]
    file_size: int | None
    file_id: Optional[str]
    download_url: str
    file_created_at: datetime
    protocol_data: Optional[dict]
    max_connections_per_download: int = 1
    max_speed_bytes_per_second: int | None = None  # use None for unlimited


@dataclass
class URLConfig:
    url: str
    destination_path: str | None = None
    group_name: str | None = None
    max_connections_per_download: int | None = None
    max_speed_bytes_per_second: int | None = None
    validate_before_download: bool | None = None
    processing_pipeline: str | None = None
    processing_regex: str | None = None
    output_filename: str | None = None
    start_new_session: bool = False


@dataclass
class DLManagerConfig:
    urls_config: list[URLConfig] | None = None
    abort_on_first_error: bool = False
    max_simultaneous_downloads: int | None = 10
    max_connections_per_download: int | None = 5
    auto_start: bool = True
    destination_folder: str | None = "."
    destination_handler: object | None = "."
    max_speed_bytes_per_second: int | None = None
    run_in_separate_process: bool = False
    show_progress_in_terminal: bool = True
    progress_update_interval_seconds: int = 30
    fix_urls: bool = True
    logger: object | None = None
    http_config: HttpConfigModel | None = field(default_factory=HttpConfigModel)
    ftp_config: object | None = None
    sftp_config: object | None = None
    torrent_config: object | None = None
