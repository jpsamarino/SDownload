from dataclasses import dataclass
from enum import StrEnum


class EFilePolicy(StrEnum):
    """
    Policy defining how existing files in local storage should be handled
    prior to starting a download.
    """

    OVERWRITE = "overwrite"
    """Always download and replace any existing file in storage (no checks)."""

    SMART_REUSE = "smart_reuse"
    """
    (Default) Reuse existing file if size matches and was modified within 24h
    (or is newer than remote). If stale or size mismatches, raises FileAlreadyExistsError.
    """

    REUSE_SAME_SIZE = "reuse_same_size"
    """
    Reuse existing file if size matches, regardless of modification time.
    If size mismatches, raises FileAlreadyExistsError.
    """

    REUSE_OR_UPDATE = "reuse_or_update"
    """
    Reuse existing file if fresh (<24h). If stale or size mismatches,
    automatically re-downloads and updates the file.
    """

    FAIL_IF_EXISTS = "fail_if_exists"
    """
    Immediately raise FileAlreadyExistsError if a file with the same name
    already exists in storage, without evaluating metadata.
    """

    SKIP_IF_EXISTS = "skip_if_exists"
    """
    Silently skip downloading if a file with the same name already exists in storage.
    Marks task as completed without raising errors.
    """

    AUTO_RENAME = "auto_rename"
    """
    If a file with the same name exists, automatically choose an available name
    (e.g., file_1.ext, file_2.ext) and download as a new file without overwriting.
    """


class EFileAction(StrEnum):
    """Action decided by the policy resolver for the download task."""

    REUSE = "reuse"
    """Existing file is valid and should be reused. Mark task COMPLETED without downloading."""

    DOWNLOAD = "download"
    """Proceed to download the file using the resolved target_file_name."""

    ERROR = "error"
    """Collision or policy violation. Raise FileAlreadyExistsError."""


@dataclass(frozen=True, slots=True)
class FilePolicyResolution:
    """Outcome of evaluating storage and policy for a target download file."""

    action: EFileAction
    target_file_name: str
    reason: str
    is_renamed: bool = False
