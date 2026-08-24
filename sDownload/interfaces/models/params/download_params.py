from dataclasses import dataclass, field

from sDownload.interfaces.models.file_policy_model import EFilePolicy


@dataclass(frozen=True)
class DownloadTaskParams:
    """
    Consolidates all business logic configuration for a single DownloadTask.

    This object separates what should be downloaded and where it should go
    from the infrastructural concerns (Storage, Downloader, Strategy).
    """

    url: str
    dest_dir: str
    file_name: str | None = None

    # Performance & Behavior
    max_conn: int = 4
    max_conn_per_server: int = 1
    target_speed_bytes: int | None = None
    file_policy: EFilePolicy = EFilePolicy.SMART_REUSE
    enable_recovery: bool = True
    use_chunked: bool = True

    # Common Headers
    headers: dict[str, str] = field(default_factory=dict)
