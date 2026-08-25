from datetime import UTC, datetime, timedelta

import pytest

from sDownload.file_system import LocalStorage
from sDownload.global_settings import GlobalSettings, global_settings
from sDownload.interfaces.models import (
    DLManagerConfig,
    EFileAction,
    EFilePolicy,
    HttpConfigModel,
    StoredFileInfo,
)
from sDownload.services.downloader_manager.strategies import MultiChunkDownloadStrategy
from sDownload.utils.file_policy_resolver import resolve_file_policy


class DummyStorage:
    def __init__(self, files: dict[str, StoredFileInfo] | None = None) -> None:
        self.files = files or {}

    async def get_data_info(self, file_name: str) -> StoredFileInfo | None:
        return self.files.get(file_name)


def test_global_settings_defaults():
    settings = GlobalSettings()
    assert settings.freshness_ttl_seconds == 86400
    assert settings.clock_skew_tolerance_seconds == 300
    assert settings.default_timeout_connect_s == 15.0
    assert settings.default_chunk_size_bytes == 1024 * 1024
    assert settings.default_storage_dir == "storage"
    assert settings.default_io_buffer_size_bytes == 1024 * 1024
    assert settings.max_scrape_size_bytes == 1024 * 1024
    assert settings.probe_timeout_s == 2.0
    assert settings.min_chunk_split_size_bytes == 2 * 1024 * 1024
    assert settings.max_simultaneous_downloads == 10
    assert settings.max_connections_per_download == 5
    assert settings.monitor_update_interval_s == 0.5


def test_models_inherit_global_settings_defaults(tmp_path):
    http_cfg = HttpConfigModel()
    assert http_cfg.timeout_connect_s == global_settings.default_timeout_connect_s

    dl_mgr_cfg = DLManagerConfig()
    assert dl_mgr_cfg.max_simultaneous_downloads == global_settings.max_simultaneous_downloads
    assert dl_mgr_cfg.max_connections_per_download == global_settings.max_connections_per_download

    storage = LocalStorage(storage_dir=tmp_path)
    assert storage.io_buffer_size == global_settings.default_io_buffer_size_bytes

    strategy = MultiChunkDownloadStrategy(max_conn=4)
    assert strategy.min_chunk_size == global_settings.min_chunk_split_size_bytes


@pytest.mark.asyncio
async def test_global_settings_runtime_modification_affects_policy():
    """Modifying global_settings.freshness_ttl_seconds in runtime dynamically alters policy decisions."""
    ref_time = datetime(2026, 8, 25, 12, 0, 0, tzinfo=UTC)
    local_time = ref_time - timedelta(hours=2)  # 2 hours old
    files = {
        "file.zip": StoredFileInfo(key="file.zip", size_bytes=1000, created_at=local_time),
    }
    storage = DummyStorage(files)

    # By default (freshness_ttl_seconds=86400 / 24h), a 2h old file is fresh -> REUSE
    global_settings.freshness_ttl_seconds = 86400
    res_default = await resolve_file_policy(
        storage, "file.zip", 1000, policy=EFilePolicy.SMART_REUSE, reference_time=ref_time
    )
    assert res_default.action == EFileAction.REUSE

    # Change runtime global_settings to 1 hour (3600s) -> 2h old file is now stale -> ERROR
    global_settings.freshness_ttl_seconds = 3600
    res_stale = await resolve_file_policy(
        storage, "file.zip", 1000, policy=EFilePolicy.SMART_REUSE, reference_time=ref_time
    )
    assert res_stale.action == EFileAction.ERROR
    assert "age=7200s > 3600s" in res_stale.reason

    # Reset back to default for safety
    global_settings.freshness_ttl_seconds = 86400
