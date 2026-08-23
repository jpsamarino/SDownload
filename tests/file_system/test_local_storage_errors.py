import asyncio
import errno

import pytest

from sDownload.exceptions import (
    StorageError,
    StorageFullError,
    StorageNotFoundError,
    StoragePermissionError,
)
from sDownload.file_system.local_storage import LocalStorage
from sDownload.file_system.os_error_mapper import map_os_error


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(storage_dir=tmp_path)


def test_map_os_error_logic_errors():
    for err_type in [ValueError, TypeError, KeyError]:
        err = err_type("test")
        assert map_os_error(err) is err


def test_map_os_error_cancellation():
    err = asyncio.CancelledError()
    assert map_os_error(err) is err


def test_map_os_error_not_found():
    err = FileNotFoundError("nope")
    mapped = map_os_error(err, "path/to/cat")
    assert isinstance(mapped, StorageNotFoundError)
    assert "path/to/cat" in mapped.path


def test_map_os_error_permissions():
    # Test PermissionError
    err = PermissionError("denied")
    assert isinstance(map_os_error(err), StoragePermissionError)

    # Test OSError with errno
    err_eaccess = OSError(errno.EACCES, "denied")
    assert isinstance(map_os_error(err_eaccess), StoragePermissionError)

    err_eperm = OSError(errno.EPERM, "denied")
    assert isinstance(map_os_error(err_eperm), StoragePermissionError)


def test_map_os_error_full():
    err = OSError(errno.ENOSPC, "full")
    assert isinstance(map_os_error(err), StorageFullError)


def test_map_os_error_unrecognized():
    err = RuntimeError("unknown")
    mapped = map_os_error(err)
    assert isinstance(mapped, StorageError)
    assert "Storage operation failed" in mapped.message


@pytest.mark.asyncio
async def test_storage_cancellation_flow(storage, tmp_path):
    # This verifies that if a storage operation is cancelled, it bubbles up correctly
    key = "cancel_test.bin"
    tmp_path / key

    from unittest.mock import patch

    with patch("aiofiles.open") as mock_open:
        mock_open.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await storage.save_binary_data(key, None)
