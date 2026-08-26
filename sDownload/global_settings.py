import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load .env variables if present
load_dotenv()


@dataclass
class GlobalSettings:
    """
    Global process-wide settings and environment defaults for SDownload.
    Can be configured via environment variables (.env) or modified in runtime.
    """

    default_storage_dir: str = os.getenv("SDOWNLOAD_DEFAULT_STORAGE_DIR", "storage")
    """Default local directory for downloaded files. Default: 'storage'."""

    freshness_ttl_seconds: int = int(os.getenv("SDOWNLOAD_FRESHNESS_TTL_SECONDS", 24 * 3600))
    """Maximum age in seconds for a cached file to be considered fresh. Default: 86400 (24h)."""

    clock_skew_tolerance_seconds: int = int(
        os.getenv("SDOWNLOAD_CLOCK_SKEW_TOLERANCE_SECONDS", 300)
    )
    """Tolerance window in seconds for server-vs-client clock drift. Default: 300 (5min)."""

    default_timeout_connect_s: float = float(os.getenv("SDOWNLOAD_DEFAULT_TIMEOUT_CONNECT_S", 15.0))
    """Default HTTP connection timeout in seconds. Default: 15.0."""

    default_chunk_size_bytes: int = int(
        os.getenv("SDOWNLOAD_DEFAULT_CHUNK_SIZE_BYTES", 1024 * 1024)
    )
    """Default chunk size in bytes for chunked downloads. Default: 1048576 (1MB)."""

    default_io_buffer_size_bytes: int = int(
        os.getenv("SDOWNLOAD_DEFAULT_IO_BUFFER_SIZE_BYTES", 1024 * 1024)
    )
    """Buffer size in bytes for disk I/O operations (file merging). Default: 1048576 (1MB)."""

    max_scrape_size_bytes: int = int(os.getenv("SDOWNLOAD_MAX_SCRAPE_SIZE_BYTES", 1024 * 1024))
    """Maximum bytes scraped from web pages during crawler resource discovery. Default: 1048576 (1MB)."""

    probe_timeout_s: float = float(os.getenv("SDOWNLOAD_PROBE_TIMEOUT_S", 2.0))
    """Timeout in seconds for OPTIONS probes during resource exploration. Default: 2.0."""

    min_chunk_split_size_bytes: int = int(
        os.getenv("SDOWNLOAD_MIN_CHUNK_SPLIT_SIZE_BYTES", 2 * 1024 * 1024)
    )
    """Minimum file size threshold in bytes before multi-chunk splitting. Default: 2097152 (2MB)."""

    max_simultaneous_downloads: int = int(os.getenv("SDOWNLOAD_MAX_SIMULTANEOUS_DOWNLOADS", 10))
    """Default maximum concurrent downloads in download manager. Default: 10."""

    max_connections_per_download: int = int(os.getenv("SDOWNLOAD_MAX_CONNECTIONS_PER_DOWNLOAD", 5))
    """Default maximum concurrent chunk connections per download. Default: 5."""

    monitor_update_interval_s: float = float(os.getenv("SDOWNLOAD_MONITOR_UPDATE_INTERVAL_S", 0.5))
    """Interval in seconds between progress monitor logging and speed updates. Default: 0.5."""


# Global singleton instance
global_settings = GlobalSettings()
