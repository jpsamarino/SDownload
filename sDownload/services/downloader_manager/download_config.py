from dataclasses import dataclass
from datetime import datetime
from typing import Optional


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
