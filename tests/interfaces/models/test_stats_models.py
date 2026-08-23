import pytest

from sDownload.interfaces.models import ChunkDownloadStats, ChunkRange, EDownloadStatus


def test_chunk_download_stats_slots():
    """Verify that ChunkDownloadStats uses slots."""
    stats = ChunkDownloadStats(chunk_file_name="test.bin", range=ChunkRange(0, 100), file_size=101)
    with pytest.raises(AttributeError):
        stats.new_attr = "should fail"


def test_chunk_download_stats_set_error():
    """Verify set_error correctly updates status and last_error."""
    stats = ChunkDownloadStats(chunk_file_name="test.bin", range=ChunkRange(0, 100), file_size=101)
    err = ValueError("test error")
    stats.set_error(err)

    assert stats.status == EDownloadStatus.ERROR
    assert stats.last_error == err


def test_chunk_download_stats_set_status_restriction():
    """Verify set_status(ERROR) raises ValueError."""
    stats = ChunkDownloadStats(chunk_file_name="test.bin", range=ChunkRange(0, 100), file_size=101)

    # Valid status
    stats.set_status(EDownloadStatus.DOWNLOADING)
    assert stats.status == EDownloadStatus.DOWNLOADING

    # Invalid status
    with pytest.raises(ValueError, match="Use set_error"):
        stats.set_status(EDownloadStatus.ERROR)


def test_download_stats_with_none_file_size():
    from sDownload.interfaces.models import DownloadStats

    stats = DownloadStats(file_size=None)
    assert stats.file_size is None
    assert stats.bytes_downloaded == 0
    assert stats.progress == 0.0

    stats.add_qt_bytes_downloaded(1024)
    stats.update()

    assert stats.bytes_downloaded == 1024
    assert stats.progress == 0.0
    assert stats.avg_speed_bps >= 0.0


def test_download_stats_with_zero_file_size():
    from sDownload.interfaces.models import DownloadStats

    stats = DownloadStats(file_size=0)
    assert stats.file_size == 0
    stats.add_qt_bytes_downloaded(512)
    stats.update()
    assert stats.progress == 0.0
