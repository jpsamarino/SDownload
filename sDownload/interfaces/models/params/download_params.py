from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(frozen=True)
class DownloadTaskParams:
    """
    Consolidates all business logic configuration for a single DownloadTask.

    This object separates what should be downloaded and where it should go
    from the infrastructural concerns (Storage, Downloader, Strategy).
    """

    url: str
    dest_dir: str
    file_name: Optional[str] = None

    # Performance & Behavior
    max_conn: int = 4
    max_conn_per_server: int = 1
    target_speed_bytes: Optional[int] = None
    overwrite_existing: bool = False
    enable_recovery: bool = True
    use_chunked: bool = True

    # Common Headers
    headers: Dict[str, str] = field(default_factory=dict)
