from dataclasses import dataclass
from enum import StrEnum


class EFilePolicy(StrEnum):
    """
    Policy defining how existing files in local storage are handled prior to downloading.

    Note on Auto-Generated (Synthetic) Filenames:
        When a URL or endpoint does not specify an explicit filename (e.g. `https://api.com/export`),
        SDownload synthesizes a fallback name (e.g. `export.bin`). If a local file with the same
        synthetic name already exists in storage and identity cannot be confirmed (i.e. size differs
        or is unknown), policies `SMART_REUSE` and `REUSE_OR_UPDATE` will automatically rename the
        new download (e.g. `export_1.bin`) to avoid collisions across endpoints.
    """

    OVERWRITE = "overwrite"
    """
    Always download and replace any existing local file.
    - If file exists: Downloads to temporary chunks and atomically overwrites target on completion.
    - Otherwise: Proceeds to download normally if file does not exist.
    - Synthetic filenames: Follows normal overwrite behavior.
    - Use case: When you always want the latest version and previous local copies can be discarded.
    """

    SMART_REUSE = "smart_reuse"
    """
    (Default) Strict cache reuse with error safety.
    - Reuses (REUSE) only if:
        1. Exact size matches (both sizes known and equal).
        2. Local file is up-to-date: modified <24h (default freshness_ttl_seconds)
           OR newer than remote Last-Modified (default 5min clock skew tolerance).
    - Otherwise: Raises FileAlreadyExistsError (ERROR) if size differs/unknown, local is stale, or server updated.
    - Synthetic filenames: Auto-renames (file_1.bin) if size differs or is unknown.
    - Use case: Critical data pipelines where stale, modified, or unverifiable files must be rejected.
    """

    REUSE_OR_UPDATE = "reuse_or_update"
    """
    Smart synchronizer / mirror policy.
    - Reuses (REUSE) only if:
        1. Exact size matches (both sizes known and equal).
        2. Local file is up-to-date: modified <24h (default freshness_ttl_seconds)
           OR newer than remote Last-Modified (default 5min clock skew tolerance).
    - Otherwise: Re-downloads and updates the file in place (DOWNLOAD) — no error is raised.
    - Synthetic filenames: Auto-renames (file_1.bin) if size differs or is unknown.
    - Use case: Scheduled sync jobs and recurring download tasks that need to keep local files up-to-date.
    """

    REUSE_SAME_SIZE = "reuse_same_size"
    """
    Size-only reuse policy (ignores modification timestamps).
    - Reuses (REUSE) only if: Exact size matches.
    - Otherwise: Raises FileAlreadyExistsError (ERROR) if size differs or is unknown.
    - Synthetic filenames: Follows strict size matching (raises ERROR if size differs or is unknown).
    - Use case: Immutable static assets with known sizes (e.g., software releases, ISOs, finalized media).
    """

    FAIL_IF_EXISTS = "fail_if_exists"
    """
    Strict collision prevention.
    - If file exists: Immediately raises FileAlreadyExistsError (ERROR) without inspecting metadata.
    - Otherwise: Proceeds to download normally if file does not exist.
    - Synthetic filenames: Immediately raises FileAlreadyExistsError (ERROR) if synthetic name exists.
    - Use case: When target filenames must be strictly unique and accidental collisions must halt execution.
    """

    SKIP_IF_EXISTS = "skip_if_exists"
    """
    Silent skip policy.
    - Reuses (REUSE) only if: A file with the same name already exists in storage (ignores size and timestamps).
    - Otherwise: Proceeds to download normally (DOWNLOAD) if file does not exist.
    - Synthetic filenames: Skips downloading if synthetic filename already exists in storage.
    - Use case: Batch scraping or crawling queues where existing files should be left intact and untouched.
    """

    AUTO_RENAME = "auto_rename"
    """
    Automatic collision renaming.
    - If file exists: Finds the next available non-colliding name (e.g. report_1.pdf, archive_1.tar.gz) and downloads. Preserves double extensions (.tar.gz, .user.js).
    - Otherwise: Proceeds to download normally if file does not exist.
    - Synthetic filenames: Follows normal auto-renaming behavior.
    - Use case: Browser-like download managers where all downloads must be saved without overwriting existing files.
    """


class EFileAction(StrEnum):
    """Action decided by the policy resolver for the download task."""

    REUSE = "reuse"
    """Existing file is valid and should be reused. Mark task COMPLETED without downloading."""

    DOWNLOAD = "download"
    """Proceed to download the file using the resolved target_file_name (which may be auto-renamed)."""

    ERROR = "error"
    """Collision or policy violation. Raise FileAlreadyExistsError."""


@dataclass(frozen=True, slots=True)
class FilePolicyResolution:
    """Outcome of evaluating storage and policy for a target download file."""

    action: EFileAction
    target_file_name: str
    reason: str
    is_renamed: bool = False
