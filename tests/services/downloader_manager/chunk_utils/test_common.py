import pytest
import asyncio
from unittest.mock import MagicMock
from sDownload.interfaces.models import ChunkRange
from sDownload.services.downloader_manager.chunk_utils.common import (
    format_chunk_file_name,
    get_effective_range_info,
    create_succession_stop_callback,
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


def test_create_succession_stop_callback():
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(50, 150)
    stats_a = MagicMock()
    stats_a.bytes_downloaded = 10
    stats_a.file_size = 101

    mock_task = MagicMock()
    mock_task.done.return_value = False

    callback = create_succession_stop_callback(r1, r2, stats_a, mock_task)
    callback()

    mock_task.cancel.assert_called_once()


def test_create_succession_stop_callback_finished():
    """If the task already finished, it shouldn't be cancelled"""
    r1 = ChunkRange(0, 100)
    r2 = ChunkRange(50, 150)
    stats_a = MagicMock()
    stats_a.bytes_downloaded = 101
    stats_a.file_size = 101

    mock_task = MagicMock()
    mock_task.done.return_value = False

    callback = create_succession_stop_callback(r1, r2, stats_a, mock_task)
    callback()

    mock_task.cancel.assert_not_called()
