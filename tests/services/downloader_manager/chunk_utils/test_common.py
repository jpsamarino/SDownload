import pytest
from sDownload.interfaces.protocols.chunk_models import ChunkRange
from sDownload.services.downloader_manager.chunk_utils.common import (
    format_chunk_file_name,
    get_effective_range_info,
)


def test_format_chunk_file_name():
    r1 = ChunkRange(0, 100)
    assert format_chunk_file_name(r1, "test.bin") == "0_100_test.bin.sdownload"

    r2 = ChunkRange(500, None)
    assert format_chunk_file_name(r2, "test.bin") == "500_EOF_test.bin.sdownload"


def test_get_effective_range_info():
    # Fixed range
    r1 = ChunkRange(0, 100)
    eff_end, total_bytes = get_effective_range_info(r1, 1000)
    assert eff_end == 100
    assert total_bytes == 101  # 0 to 100 inclusive

    # Tail range (end is None)
    r2 = ChunkRange(900, None)
    eff_end, total_bytes = get_effective_range_info(r2, 1000)
    assert eff_end == 999
    assert total_bytes == 100  # 900 to 999 inclusive
