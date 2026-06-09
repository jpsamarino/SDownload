from dataclasses import dataclass
from datetime import datetime
from typing import List
from .stats_models import ChunkDownloadStats


@dataclass
class RecoveryChunkDTO:
    """Minimal data for a single chunk in the JSON file."""

    chunk_file_name: str
    start: int
    end: int
    bytes: int


@dataclass
class RecoveryStateDTO:
    """Minimal data for the entire download state in the JSON file."""

    file_id: str
    file_size: int
    chunks: List[RecoveryChunkDTO]
    updated_at: datetime


@dataclass
class DownloadInfo:
    """Domain model returned by the recovery service."""

    file_id: str
    file_size: int
    chunks_finished: List[ChunkDownloadStats]
    updated_at: datetime
