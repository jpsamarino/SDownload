from typing import Protocol, runtime_checkable

from sDownload.interfaces.models import ChunkDownloadStats, DownloadInfo


@runtime_checkable
class RecoveryProtocol(Protocol):
    """
    Protocol defining the contract for persisting, loading, and purging download recovery states.
    """

    async def save_info(
        self,
        file_id: str,
        total_file_size: int,
        stats_list: list[ChunkDownloadStats],
        min_chunk_size: int = 1024 * 1024,
        delete_useless_chunks: bool = True,
    ) -> None:
        """Persists recovery metadata for the current download chunks."""
        ...

    async def load_info(self, file_id: str) -> DownloadInfo | None:
        """Loads and verifies previously saved recovery state for a file_id."""
        ...

    async def delete_info(self, file_id: str) -> None:
        """Deletes recovery metadata for the given file_id."""
        ...

    async def purge_all(self, file_id: str) -> None:
        """Deletes both recovery metadata and all associated temporary chunk files."""
        ...
