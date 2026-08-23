import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

from .chunk_models import ChunkRange


class EDownloadStatus(StrEnum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    AWAITING_SUCCESSION = "awaiting_succession"
    DEPRECATED = "deprecated"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ERROR = "error"


@dataclass(slots=True)
class ChunkDownloadStats:
    chunk_file_name: str
    range: ChunkRange
    file_size: int | None
    bytes_downloaded: int = 0
    qt_bytes_last_update: int = 0
    progress: float | None = None
    speed_bps: float = 0.0
    status: EDownloadStatus = EDownloadStatus.PENDING
    start_time: float = field(default_factory=time.monotonic)
    last_update: float = field(default_factory=time.monotonic)
    target_speed_bps: int | None = None  # use None for unlimited
    limit_qt_bytes: int = field(default=0, init=False)
    _on_limit: Callable[[], None] | None = field(default=None, init=False)
    last_error: Exception | None = field(default=None, init=False)
    add_qt_bytes_downloaded: Callable[[int], None] = field(init=False)

    def __post_init__(self):
        self.add_qt_bytes_downloaded = self._add_no_limit

    def _add_no_limit(self, qt_bytes: int):
        self.bytes_downloaded += qt_bytes

    def _add_with_limit(self, qt_bytes: int):
        self.bytes_downloaded += qt_bytes
        if self.bytes_downloaded >= self.limit_qt_bytes:
            cb = self._on_limit
            self._on_limit = None
            self.add_qt_bytes_downloaded = self._add_no_limit
            if cb:
                cb()

    def add_limit_observer(self, qt_max_useful_bytes: int, callback: Callable[[], None]):
        if self.bytes_downloaded >= qt_max_useful_bytes:
            callback()
            return
        self.limit_qt_bytes = qt_max_useful_bytes
        self._on_limit = callback
        self.add_qt_bytes_downloaded = self._add_with_limit

    def remove_limit_observer(self):
        self._on_limit = None
        self.limit_qt_bytes = 0
        self.add_qt_bytes_downloaded = self._add_no_limit

    def set_status(self, status: EDownloadStatus):
        if status == EDownloadStatus.ERROR:
            raise ValueError(
                "Do not set EDownloadStatus.ERROR directly. Use set_error(exc) instead."
            )
        self.status = status

    def set_error(self, exc: Exception):
        self.status = EDownloadStatus.ERROR
        self.last_error = exc

    def update(self):
        qt_bytes_elapsed = self.bytes_downloaded - self.qt_bytes_last_update
        self.qt_bytes_last_update = self.bytes_downloaded
        now = time.monotonic()
        time_elapsed = now - self.last_update
        self.progress = 100.0 * self.bytes_downloaded / self.file_size if self.file_size else None
        self.speed_bps = qt_bytes_elapsed / time_elapsed if time_elapsed > 0 else 0.0
        self.last_update = now if qt_bytes_elapsed > 0 else self.last_update


@dataclass
class DownloadStats:
    file_size: int | None = None
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
        # use EMA and solve problem with speed_bps when set_bytes_downloaded is used to avoid negative speed
        now = time.monotonic()
        time_elapsed_avg = now - self.start_time
        if self.file_size and self.file_size > 0:
            self.progress = 100.0 * self.bytes_downloaded / self.file_size
        else:
            self.progress = 0.0

        self.avg_speed_bps = (
            self.bytes_downloaded / time_elapsed_avg if time_elapsed_avg > 0 else 0.0
        )
        qt_bytes_elapsed = self.bytes_downloaded - self.qt_bytes_last_update
        self.qt_bytes_last_update = self.bytes_downloaded
        time_elapsed_period = now - self.last_update
        self.speed_bps = qt_bytes_elapsed / time_elapsed_period if time_elapsed_period > 0 else 0.0
        self.last_update = now if qt_bytes_elapsed > 0 else self.last_update
