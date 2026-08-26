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

    def __post_init__(self) -> None:
        if self.max_conn < 1:
            raise ValueError(f"max_conn must be >= 1, got {self.max_conn}")
        if not self.use_chunked and self.max_conn > 1:
            raise ValueError(
                f"Conflicting parameters: max_conn={self.max_conn} cannot be greater than 1 "
                "when use_chunked=False (single-stream mode)."
            )
