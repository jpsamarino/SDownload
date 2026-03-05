import asyncio
import errno
import pytest
from unittest.mock import MagicMock
from sDownload.file_system.local_storage import LocalStorage
from sDownload.exceptions import (
    StorageNotFoundError,
    StoragePermissionError,
    StorageFullError,
    StorageError,
    SDownloadError,
)


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(storage_dir=tmp_path)


def test_map_os_error_logic_errors(storage):
    for err_type in [ValueError, TypeError, KeyError]:
        err = err_type("test")
        assert storage._map_os_error(err) is err


def test_map_os_error_cancellation(storage):
    err = asyncio.CancelledError()
    assert storage._map_os_error(err) is err


def test_map_os_error_not_found(storage):
    err = FileNotFoundError("nope")
    mapped = storage._map_os_error(err, "path/to/cat")
    assert isinstance(mapped, StorageNotFoundError)
    assert "path/to/cat" in mapped.path


def test_map_os_error_permissions(storage):
    # Test PermissionError
    err = PermissionError("denied")
    assert isinstance(storage._map_os_error(err), StoragePermissionError)

    # Test OSError with errno
    err_eaccess = OSError(errno.EACCES, "denied")
    assert isinstance(storage._map_os_error(err_eaccess), StoragePermissionError)

    err_eperm = OSError(errno.EPERM, "denied")
    assert isinstance(storage._map_os_error(err_eperm), StoragePermissionError)


def test_map_os_error_full(storage):
    err = OSError(errno.ENOSPC, "full")
    assert isinstance(storage._map_os_error(err), StorageFullError)


def test_map_os_error_unrecognized(storage):
    err = RuntimeError("unknown")
    mapped = storage._map_os_error(err)
    assert isinstance(mapped, StorageError)
    assert "Storage operation failed" in mapped.message


@pytest.mark.asyncio
async def test_storage_cancellation_flow(storage, tmp_path):
    # This verifies that if a storage operation is cancelled, the mapper returns the CancelledError
    key = "cancel_test.bin"
    path = tmp_path / key

    # We mock aiofiles.open to raise CancelledError when used
    import aiofiles
    from unittest.mock import patch

    with patch("aiofiles.open") as mock_open:
        mock_open.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await storage.save_binary_data(key, None)
