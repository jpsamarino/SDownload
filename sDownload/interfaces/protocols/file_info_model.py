from dataclasses import dataclass
from datetime import datetime


@dataclass
class FileInfoModel:
    file_name: str
    content_type: str | None
    file_size: int
    file_id: str | None  # can be etag for http or other unique id for file in other protocols
    download_url: str
    transmission_protocol: str
    server_accept_ranges: bool
    file_created_at: datetime
