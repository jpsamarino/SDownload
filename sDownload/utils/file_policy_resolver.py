import logging
from datetime import UTC, datetime

from sDownload.interfaces.models import (
    EFileAction,
    EFilePolicy,
    FilePolicyResolution,
)
from sDownload.interfaces.protocols import FileStorageProtocol

logger = logging.getLogger(__name__)

KNOWN_COMPOUND_EXTENSIONS = (
    ".tar.gz",
    ".tar.bz2",
    ".tar.xz",
    ".tar.zst",
    ".tar.lz4",
)


def _split_stem_and_extension(file_name: str) -> tuple[str, str]:
    """
    Splits a filename into stem and extension, preserving compound extensions
    such as .tar.gz.
    """
    lower_name = file_name.lower()
    for compound_ext in KNOWN_COMPOUND_EXTENSIONS:
        if lower_name.endswith(compound_ext):
            stem = file_name[: -len(compound_ext)]
            ext = file_name[-len(compound_ext) :]
            return stem, ext

    if "." in file_name:
        dot_idx = file_name.rfind(".")
        return file_name[:dot_idx], file_name[dot_idx:]
    return file_name, ""


async def find_available_file_name(
    storage: FileStorageProtocol,
    file_name: str,
) -> str:
    """
    Finds the next non-colliding filename in storage by appending an incrementing index
    (e.g., file.zip -> file_1.zip -> file_2.zip).
    """
    info = await storage.get_data_info(file_name)
    if info is None:
        return file_name

    stem, ext = _split_stem_and_extension(file_name)
    counter = 1
    while True:
        candidate = f"{stem}_{counter}{ext}"
        candidate_info = await storage.get_data_info(candidate)
        if candidate_info is None:
            return candidate
        counter += 1


async def resolve_file_policy(
    storage: FileStorageProtocol,
    file_name: str,
    expected_size: int | None,
    remote_created_at: datetime | None = None,
    policy: EFilePolicy = EFilePolicy.SMART_REUSE,
    is_generated_name: bool = False,
    reference_time: datetime | None = None,
) -> FilePolicyResolution:
    """
    Evaluates storage status and resolves the file policy decision.

    Returns a FilePolicyResolution containing the determined action (REUSE, DOWNLOAD, ERROR),
    the resolved target filename (which may be auto-renamed), and the rationale.
    """
    info = await storage.get_data_info(file_name)

    # 1. File does not exist in storage -> proceed to download
    if info is None:
        return FilePolicyResolution(
            action=EFileAction.DOWNLOAD,
            target_file_name=file_name,
            reason="File does not exist in storage",
            is_renamed=False,
        )

    # 2. OVERWRITE policy -> always download and replace
    if policy == EFilePolicy.OVERWRITE:
        return FilePolicyResolution(
            action=EFileAction.DOWNLOAD,
            target_file_name=file_name,
            reason="Policy is OVERWRITE, existing file will be replaced",
            is_renamed=False,
        )

    # 3. FAIL_IF_EXISTS policy -> immediate error
    if policy == EFilePolicy.FAIL_IF_EXISTS:
        return FilePolicyResolution(
            action=EFileAction.ERROR,
            target_file_name=file_name,
            reason=f"File {file_name} already exists in storage and policy is FAIL_IF_EXISTS",
            is_renamed=False,
        )

    # 4. SKIP_IF_EXISTS policy -> reuse without downloading
    if policy == EFilePolicy.SKIP_IF_EXISTS:
        return FilePolicyResolution(
            action=EFileAction.REUSE,
            target_file_name=file_name,
            reason=f"File {file_name} already exists in storage and policy is SKIP_IF_EXISTS",
            is_renamed=False,
        )

    # 5. AUTO_RENAME policy -> find next free filename and download
    if policy == EFilePolicy.AUTO_RENAME:
        available_name = await find_available_file_name(storage, file_name)
        return FilePolicyResolution(
            action=EFileAction.DOWNLOAD,
            target_file_name=available_name,
            reason=f"Auto-renamed '{file_name}' to '{available_name}' due to AUTO_RENAME policy",
            is_renamed=True,
        )

    # 6. Check synthetic/generated filename collision with different size
    size_matched = expected_size is not None and info.size_bytes == expected_size
    if (
        is_generated_name
        and expected_size is not None
        and not size_matched
        and policy
        in (
            EFilePolicy.SMART_REUSE,
            EFilePolicy.REUSE_OR_UPDATE,
            EFilePolicy.AUTO_RENAME,
        )
    ):
        available_name = await find_available_file_name(storage, file_name)
        logger.info(
            "Synthetic filename '%s' exists with different size (local=%dB, expected=%dB). "
            "Auto-renaming to '%s'.",
            file_name,
            info.size_bytes,
            expected_size,
            available_name,
        )
        return FilePolicyResolution(
            action=EFileAction.DOWNLOAD,
            target_file_name=available_name,
            reason=(
                f"Synthetic filename '{file_name}' collided with different size "
                f"({info.size_bytes}B vs {expected_size}B). Auto-renamed to '{available_name}'."
            ),
            is_renamed=True,
        )

    # 7. REUSE_SAME_SIZE policy
    if policy == EFilePolicy.REUSE_SAME_SIZE:
        if size_matched:
            return FilePolicyResolution(
                action=EFileAction.REUSE,
                target_file_name=file_name,
                reason=f"Size matched ({info.size_bytes}B)",
                is_renamed=False,
            )
        return FilePolicyResolution(
            action=EFileAction.ERROR,
            target_file_name=file_name,
            reason=f"Size mismatch for REUSE_SAME_SIZE: local={info.size_bytes}B, remote={expected_size}B",
            is_renamed=False,
        )

    # 8. Evaluate Freshness (for SMART_REUSE and REUSE_OR_UPDATE)
    now = reference_time or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)

    local_dt = info.created_at
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=UTC)

    age_seconds = (now - local_dt).total_seconds()
    is_recent_24h = 0 <= age_seconds <= 24 * 3600

    is_newer_than_remote = False
    is_older_than_remote = False
    if remote_created_at is not None:
        rem_dt = remote_created_at
        if rem_dt.tzinfo is None:
            rem_dt = rem_dt.replace(tzinfo=UTC)
        if local_dt >= rem_dt:
            is_newer_than_remote = True
        else:
            is_older_than_remote = True

    # Trusted if size matches AND (recent <=24h OR newer than remote) AND NOT older than remote
    is_trusted = (
        size_matched and not is_older_than_remote and (is_recent_24h or is_newer_than_remote)
    )

    if is_trusted:
        return FilePolicyResolution(
            action=EFileAction.REUSE,
            target_file_name=file_name,
            reason=f"File is fresh and size matched ({info.size_bytes}B)",
            is_renamed=False,
        )

    # Not trusted: decide based on policy
    failure_reason = (
        f"Size mismatch: local={info.size_bytes}B, remote={expected_size}B"
        if not size_matched
        else (
            "Local file is older than remote Last-Modified timestamp"
            if is_older_than_remote
            else f"Local file is stale (age={int(age_seconds)}s > 24h)"
        )
    )

    if policy == EFilePolicy.REUSE_OR_UPDATE:
        logger.info(
            "File '%s' exists but is not fresh (%s). Policy REUSE_OR_UPDATE will re-download.",
            file_name,
            failure_reason,
        )
        return FilePolicyResolution(
            action=EFileAction.DOWNLOAD,
            target_file_name=file_name,
            reason=f"File is stale or size mismatch ({failure_reason}). Re-downloading to update.",
            is_renamed=False,
        )

    # SMART_REUSE fails if not trusted
    logger.warning(
        "File '%s' exists in storage but is not trusted (%s). Policy SMART_REUSE raises error.",
        file_name,
        failure_reason,
    )
    return FilePolicyResolution(
        action=EFileAction.ERROR,
        target_file_name=file_name,
        reason=failure_reason,
        is_renamed=False,
    )
