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

    freshness_ttl_seconds: int = int(os.getenv("SDOWNLOAD_FRESHNESS_TTL_SECONDS", 24 * 3600))
    clock_skew_tolerance_seconds: int = int(
        os.getenv("SDOWNLOAD_CLOCK_SKEW_TOLERANCE_SECONDS", 300)
    )
    default_timeout_connect_s: float = float(os.getenv("SDOWNLOAD_DEFAULT_TIMEOUT_CONNECT_S", 15.0))
    default_chunk_size_bytes: int = int(
        os.getenv("SDOWNLOAD_DEFAULT_CHUNK_SIZE_BYTES", 1024 * 1024)
    )


# Global singleton instance
global_settings = GlobalSettings()
