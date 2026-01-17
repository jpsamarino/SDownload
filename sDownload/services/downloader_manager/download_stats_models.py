from dataclasses import dataclass, field
from enum import Enum
import time
from sDownload.interfaces.protocols.chunk_models import ChunkRange


class EDownloadStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ERROR = "error"


@dataclass
class ChunkDownloadStats:
    chunk_file_name: str
    range: ChunkRange
    file_size: int
    bytes_downloaded: int = 0
    qt_bytes_last_update: int = 0
    progress: float = 0.0
    speed_bps: float = 0.0
    status: EDownloadStatus = EDownloadStatus.PENDING
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)
    target_speed_bps: float = float("inf")

    def add_qt_bytes_downloaded(self, qt_bytes: int):
        self.bytes_downloaded += qt_bytes

    def set_status(self, status: EDownloadStatus):
        self.status = status

    def update(self):
        qt_bytes_elapsed = self.bytes_downloaded - self.qt_bytes_last_update
        self.qt_bytes_last_update = self.bytes_downloaded
        now = time.monotonic()
        time_elapsed = now - self.last_update
        self.progress = 100.0 * self.bytes_downloaded / self.file_size
        self.speed_bps = qt_bytes_elapsed / time_elapsed if time_elapsed > 0 else 0
        self.last_update = now if qt_bytes_elapsed > 0 else self.last_update
        if self.progress >= 100.0:
            self.status = EDownloadStatus.COMPLETED


# add status here ?
@dataclass
class DownloadStats:
    file_size: int
    bytes_downloaded: int = 0
    qt_bytes_last_update: int = 0
    progress: float = 0.0
    speed_bps: float = 0.0
    avg_speed_bps: float = 0.0
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)

    def add_qt_bytes_downloaded(self, qt_bytes: int):
        self.bytes_downloaded += qt_bytes

    def set_bytes_downloaded(self, qt_bytes: int):
        self.bytes_downloaded = qt_bytes

    def update(self):
        now = time.monotonic()
        time_elapsed_avg = now - self.start_time
        self.progress = 100.0 * self.bytes_downloaded / self.file_size
        self.avg_speed_bps = (
            self.bytes_downloaded / time_elapsed_avg if time_elapsed_avg > 0 else 0
        )
        qt_bytes_elapsed = self.bytes_downloaded - self.qt_bytes_last_update
        self.qt_bytes_last_update = self.bytes_downloaded
        time_elapsed_period = now - self.last_update
        self.speed_bps = (
            qt_bytes_elapsed / time_elapsed_period if time_elapsed_period > 0 else 0
        )
        self.last_update = now if qt_bytes_elapsed > 0 else self.last_update
