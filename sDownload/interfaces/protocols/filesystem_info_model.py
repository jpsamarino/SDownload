from dataclasses import dataclass
from datetime import datetime


@dataclass
class FileSystemInfoModel():
    key: str
    size_bytes: int
    created_at: datetime
