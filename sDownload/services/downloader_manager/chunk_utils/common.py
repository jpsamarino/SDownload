from sDownload.interfaces.protocols.chunk_models import ChunkRange


def format_chunk_file_name(chunk_range: ChunkRange, file_name: str) -> str:
    """
    Returns the standardized temporary file name for a chunk.
    """
    return f"{chunk_range}_{file_name}.sdownload"


def get_effective_range_info(
    chunk_range: ChunkRange, total_file_size: int | None
) -> tuple[int | None, int | None]:
    """
    Calculates the effective end byte and total size for a chunk range.

    Returns:
        tuple[int | None, int | None]: (effective_end, total_bytes)
    """
    if chunk_range.end is not None:
        effective_end = chunk_range.end
        total_bytes = effective_end - chunk_range.start + 1
    elif total_file_size is not None:
        effective_end = total_file_size - 1
        total_bytes = effective_end - chunk_range.start + 1
    else:
        effective_end = None
        total_bytes = None

    return effective_end, total_bytes
