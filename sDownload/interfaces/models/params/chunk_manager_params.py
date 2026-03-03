from typing import NamedTuple


class ChunkManagerParams(NamedTuple):
    """
    Configuration parameters for ChunkManager.
    """

    file_name: str
    file_size: int | None
    download_url: str
