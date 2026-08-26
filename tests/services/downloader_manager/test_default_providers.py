from pathlib import Path

import pytest

from sDownload.file_system import LocalStorage
from sDownload.global_settings import global_settings
from sDownload.http_client import HttpxDownloader
from sDownload.interfaces.models.file_policy_model import EFilePolicy
from sDownload.interfaces.models.params import DownloadTaskParams
from sDownload.interfaces.protocols import (
    RecoveryProtocol,
)
from sDownload.services.downloader_manager.default_providers import (
    DefaultComponentProvider,
    default_provider,
)
from sDownload.services.downloader_manager.download_task import DownloadTask
from sDownload.services.downloader_manager.recovery_download import RecoveryDownload
from sDownload.services.downloader_manager.strategies import (
    MultiChunkDownloadStrategy,
    SingleStreamStrategy,
)


@pytest.fixture(autouse=True)
def reset_provider_cache():
    default_provider.clear_cache()
    yield
    default_provider.clear_cache()


# ============================================================================
# 1. get_storage() Tests
# ============================================================================


def test_get_storage_defaults_to_global_setting(tmp_path):
    prov = DefaultComponentProvider()
    global_settings.default_storage_dir = str(tmp_path / "global_store")

    # Both None and explicit default string return the exact same memoized instance
    storage_none = prov.get_storage(None)
    storage_explicit = prov.get_storage(global_settings.default_storage_dir)

    assert storage_none is storage_explicit
    assert storage_none.storage_dir == Path(global_settings.default_storage_dir).resolve()


@pytest.mark.parametrize(
    "relative_path",
    [
        "custom_folder",
        "./custom_folder",
        "custom_folder/",
        "./custom_folder/.",
        "custom_folder/sub/..",
    ],
)
def test_get_storage_path_normalization_and_memoization(tmp_path, relative_path):
    prov = DefaultComponentProvider()
    base_folder = tmp_path / "custom_folder"

    # Reference storage
    storage_ref = prov.get_storage(base_folder)

    # All normalized paths resolving to base_folder reuse the same instance
    test_path = tmp_path / relative_path
    storage_test = prov.get_storage(test_path)

    assert storage_test is storage_ref
    assert storage_test.storage_dir == base_folder.resolve()


def test_get_storage_switching_folders_preserves_previous_instances(tmp_path):
    prov = DefaultComponentProvider()
    path_a = tmp_path / "store_a"
    path_b = tmp_path / "store_b"
    path_c = tmp_path / "store_c"

    # Create storage for folder A
    storage_a = prov.get_storage(path_a)
    assert storage_a.storage_dir == path_a.resolve()

    # Switch to folder B -> creates new storage instance
    storage_b = prov.get_storage(path_b)
    assert storage_b is not storage_a
    assert storage_b.storage_dir == path_b.resolve()

    # Previous storage A remains alive, intact, and referencing path_a
    assert storage_a.storage_dir == path_a.resolve()

    # Switch to folder C
    storage_c = prov.get_storage(path_c)
    assert storage_c is not storage_b
    assert storage_c.storage_dir == path_c.resolve()


@pytest.mark.asyncio
async def test_get_storage_functional_io_operations(tmp_path):
    prov = DefaultComponentProvider()
    dest = tmp_path / "functional_store"
    storage = prov.get_storage(dest)

    assert isinstance(storage, LocalStorage)

    # Verify storage can write and retrieve binary data
    async def sample_generator():
        yield b"Hello SDownload Architecture!"

    await storage.save_binary_data("sample.txt", sample_generator())

    info = await storage.get_data_info("sample.txt")
    assert info is not None
    assert info.key == "sample.txt"
    assert info.size_bytes == len(b"Hello SDownload Architecture!")


# ============================================================================
# 2. get_downloader() Tests
# ============================================================================


@pytest.mark.parametrize(
    "url",
    [
        "https://cdn.example.com/archive.zip",
        "http://legacy.example.org/installer.exe",
        "//protocol-relative.com/bundle.js",
        "meusite.edu/arquivo.xml",
        "www.meusite.edu/arquivo.xml",
        "//portal.org:8080/files/data.csv",
        "http://portal.org:8080/files/data.csv",
        "subdomain.domain.co.uk/path/to/resource?auth=token&download=true",
        "192.168.1.100:5000/media/video.mp4",
        "[::1]:8000/ipv6_test.bin",
    ],
)
def test_get_downloader_reuses_singleton_for_all_http_and_schemeless_urls(url):
    prov = DefaultComponentProvider()

    dl_first = prov.get_downloader("https://initial.com/file")
    dl_current = prov.get_downloader(url)

    assert isinstance(dl_current, HttpxDownloader)
    # Reuses the exact same singleton instance
    assert dl_current is dl_first


def test_get_downloader_dedicated_instance_for_custom_headers():
    prov = DefaultComponentProvider()

    dl_default = prov.get_downloader("https://example.com/file.zip")
    dl_auth1 = prov.get_downloader(
        "https://example.com/file.zip", headers={"Authorization": "Bearer token_alpha"}
    )
    dl_auth2 = prov.get_downloader(
        "https://example.com/file.zip", headers={"Authorization": "Bearer token_beta"}
    )
    dl_agent = prov.get_downloader(
        "https://example.com/file.zip", headers={"User-Agent": "CustomScraper/2.0"}
    )

    # Distinct instances for distinct custom headers
    assert dl_auth1 is not dl_default
    assert dl_auth2 is not dl_auth1
    assert dl_agent is not dl_auth1

    # Headers properly stored inside each client config
    assert dl_auth1.config.headers == {"Authorization": "Bearer token_alpha"}
    assert dl_auth2.config.headers == {"Authorization": "Bearer token_beta"}
    assert dl_agent.config.headers == {"User-Agent": "CustomScraper/2.0"}


@pytest.mark.parametrize(
    ("ftp_url", "expected_match"),
    [
        ("ftp://ftp.debian.org/debian/README", "FTP protocol"),
        ("FTP://FTP.MICROSOFT.COM/softlib/index.txt", "FTP protocol"),
        ("sftp://secure.vault.org/backups/db.tar.gz", "SFTP protocol"),
        ("SFTP://SSH.SERVER.NET/home/user/file.bin", "SFTP protocol"),
    ],
)
def test_get_downloader_ftp_and_sftp_raise_not_implemented(ftp_url, expected_match):
    prov = DefaultComponentProvider()
    with pytest.raises(NotImplementedError, match=expected_match):
        prov.get_downloader(ftp_url)


@pytest.mark.parametrize(
    ("unsupported_url", "scheme_name"),
    [
        ("s3://my-bucket/data/file.parquet", "s3"),
        ("S3://UPPER-BUCKET/DATA.CSV", "s3"),
        ("file:///C:/local/path/file.zip", "file"),
        ("gopher://gopher.floodgap.com/1/", "gopher"),
        ("torrent://tracker.example.com/hash", "torrent"),
        ("magnet:?xt=urn:btih:xyz", "magnet"),
        ("htttp://typo-in-url.com/file.bin", "htttp"),
        ("httpss://typo-https.com/file.bin", "httpss"),
        ("ws://realtime.io/stream", "ws"),
        ("wss://secure-stream.io/feed", "wss"),
    ],
)
def test_get_downloader_unsupported_schemes_raise_value_error(unsupported_url, scheme_name):
    prov = DefaultComponentProvider()
    with pytest.raises(
        ValueError,
        match=f"Unsupported URL scheme '{scheme_name}'. Supported schemes: 'http', 'https'.",
    ):
        prov.get_downloader(unsupported_url)


# ============================================================================
# 3. get_strategy() Tests
# ============================================================================


def test_get_strategy_returns_fresh_independent_instances():
    prov = DefaultComponentProvider()

    # MultiChunkStrategy instances are fresh per call
    strat_multi_1 = prov.get_strategy(use_chunked=True, max_conn=4)
    strat_multi_2 = prov.get_strategy(use_chunked=True, max_conn=4)

    assert isinstance(strat_multi_1, MultiChunkDownloadStrategy)
    assert isinstance(strat_multi_2, MultiChunkDownloadStrategy)
    assert strat_multi_1 is not strat_multi_2
    assert strat_multi_1.max_conn == 4
    assert strat_multi_2.max_conn == 4

    # SingleStreamStrategy instances are fresh per call
    strat_single_1 = prov.get_strategy(use_chunked=False, max_conn=1)
    strat_single_2 = prov.get_strategy(use_chunked=False, max_conn=1)

    assert isinstance(strat_single_1, SingleStreamStrategy)
    assert isinstance(strat_single_2, SingleStreamStrategy)
    assert strat_single_1 is not strat_single_2


# ============================================================================
# 4. get_recovery() Tests
# ============================================================================


def test_get_recovery_returns_recovery_protocol_instance(tmp_path):
    prov = DefaultComponentProvider()
    storage = prov.get_storage(tmp_path / "recovery_store")

    recovery = prov.get_recovery(storage)

    assert isinstance(recovery, RecoveryProtocol)
    assert isinstance(recovery, RecoveryDownload)
    assert recovery._storage is storage


def test_get_recovery_memoizes_instance_for_same_storage(tmp_path):
    prov = DefaultComponentProvider()
    storage_a = prov.get_storage(tmp_path / "store_a")
    storage_b = LocalStorage(storage_dir=tmp_path / "store_b")

    rec_1 = prov.get_recovery(storage_a)
    rec_2 = prov.get_recovery(storage_a)

    # Reuses same instance for the same storage
    assert rec_1 is rec_2

    # Different storage creates new recovery instance
    rec_b = prov.get_recovery(storage_b)
    assert rec_b is not rec_1
    assert rec_b._storage is storage_b


# ============================================================================
# 5. clear_cache() Tests
# ============================================================================


def test_clear_cache_resets_memoized_instances(tmp_path):
    prov = DefaultComponentProvider()
    folder = tmp_path / "memoized_folder"

    # Populate caches
    storage_before = prov.get_storage(folder)
    dl_before = prov.get_downloader("https://example.com/test")

    prov.clear_cache()

    # Next calls create brand new instances
    storage_after = prov.get_storage(folder)
    dl_after = prov.get_downloader("https://example.com/test")

    assert storage_after is not storage_before
    assert dl_after is not dl_before


# ============================================================================
# 6. Integration with DownloadTask & DownloadTaskParams Tests
# ============================================================================


def test_download_task_params_validation_rules():
    # max_conn < 1 is strictly rejected
    with pytest.raises(ValueError, match="max_conn must be >= 1"):
        DownloadTaskParams(url="https://site.com/file.bin", dest_dir="./", max_conn=0)

    with pytest.raises(ValueError, match="max_conn must be >= 1"):
        DownloadTaskParams(url="https://site.com/file.bin", dest_dir="./", max_conn=-5)

    # use_chunked=False with max_conn > 1 is rejected as conflicting
    with pytest.raises(ValueError, match="Conflicting parameters"):
        DownloadTaskParams(
            url="https://site.com/file.bin",
            dest_dir="./",
            use_chunked=False,
            max_conn=8,
        )

    # Valid combinations succeed
    params_chunked = DownloadTaskParams(
        url="https://site.com/file.bin",
        dest_dir="./",
        use_chunked=True,
        max_conn=8,
    )
    assert params_chunked.use_chunked is True
    assert params_chunked.max_conn == 8

    params_stream = DownloadTaskParams(
        url="https://site.com/file.bin",
        dest_dir="./",
        use_chunked=False,
        max_conn=1,
    )
    assert params_stream.use_chunked is False
    assert params_stream.max_conn == 1


def test_download_task_zero_config_initialization(tmp_path):
    """DownloadTask initializes all 4 dependencies cleanly via default_provider."""
    params = DownloadTaskParams(
        url="https://example.com/file.iso",
        dest_dir=str(tmp_path),
        file_policy=EFilePolicy.SMART_REUSE,
    )
    task = DownloadTask(params)

    assert isinstance(task._storage, LocalStorage)
    assert isinstance(task._downloader, HttpxDownloader)
    assert isinstance(task._strategy, MultiChunkDownloadStrategy)
    assert isinstance(task._recovery, RecoveryDownload)
    assert isinstance(task._recovery, RecoveryProtocol)


def test_download_task_custom_dependency_injection(tmp_path):
    """Passing custom instances to DownloadTask overrides default_provider entirely."""
    params = DownloadTaskParams(
        url="https://example.com/file.iso",
        dest_dir=str(tmp_path),
    )

    custom_storage = LocalStorage(storage_dir=tmp_path / "custom")
    custom_downloader = HttpxDownloader()
    custom_strategy = SingleStreamStrategy()
    custom_recovery = RecoveryDownload(custom_storage)

    task = DownloadTask(
        params=params,
        storage=custom_storage,
        downloader=custom_downloader,
        strategy=custom_strategy,
        recovery=custom_recovery,
    )

    assert task._storage is custom_storage
    assert task._downloader is custom_downloader
    assert task._strategy is custom_strategy
    assert task._recovery is custom_recovery
    assert task._recovery is custom_recovery
