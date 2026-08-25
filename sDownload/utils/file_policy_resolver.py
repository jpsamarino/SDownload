import logging
import re
from datetime import UTC, datetime

from sDownload.interfaces.models import (
    EFileAction,
    EFilePolicy,
    FilePolicyResolution,
)
from sDownload.interfaces.protocols import FileStorageProtocol

logger = logging.getLogger(__name__)

_DOUBLE_EXTENSION_RE = re.compile(
    r"^(?P<stem>.+?)(?P<ext>\.(?:tar|cpio)\.[a-zA-Z0-9_-]+|\.user\.js|\.[^.]+)$",
    re.IGNORECASE,
)


def _split_stem_and_extension(file_name: str) -> tuple[str, str]:
    """
    Splits a filename into stem and extension, preserving double extensions
    (e.g., .tar.gz, .tar.zst, .user.js).
    """
    match = _DOUBLE_EXTENSION_RE.match(file_name)
    if match:
        return match.group("stem"), match.group("ext")
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


CLOCK_SKEW_TOLERANCE_SECONDS = 300  # 5 minutes clock skew tolerance


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

    size_known = expected_size is not None
    size_matched = size_known and info.size_bytes == expected_size
    size_mismatched = size_known and info.size_bytes != expected_size

    # 6. Check synthetic/generated filename collision when identity cannot be verified
    if (
        is_generated_name
        and not size_matched
        and policy
        in (EFilePolicy.SMART_REUSE, EFilePolicy.REUSE_OR_UPDATE, EFilePolicy.REUSE_SAME_SIZE)
    ):
        available_name = await find_available_file_name(storage, file_name)
        size_desc = f"{expected_size}B" if size_known else "unknown"
        logger.info(
            "Synthetic filename '%s' exists with different or unknown size (local=%dB, expected=%s). "
            "Auto-renaming to '%s'.",
            file_name,
            info.size_bytes,
            size_desc,
            available_name,
        )
        return FilePolicyResolution(
            action=EFileAction.DOWNLOAD,
            target_file_name=available_name,
            reason=(
                f"Synthetic filename '{file_name}' collided (local={info.size_bytes}B vs "
                f"expected={size_desc}). Auto-renamed to '{available_name}'."
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
        mismatch_reason = (
            f"Size mismatch for REUSE_SAME_SIZE: local={info.size_bytes}B, remote={expected_size}B"
            if size_known
            else "Remote file size is unknown for REUSE_SAME_SIZE policy"
        )
        return FilePolicyResolution(
            action=EFileAction.ERROR,
            target_file_name=file_name,
            reason=mismatch_reason,
            is_renamed=False,
        )

    # 8. Evaluate Freshness (for SMART_REUSE and REUSE_OR_UPDATE)
    if policy in (EFilePolicy.SMART_REUSE, EFilePolicy.REUSE_OR_UPDATE):
        now = reference_time or datetime.now(UTC)
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)

        local_dt = info.created_at
        if local_dt.tzinfo is None:
            local_dt = local_dt.replace(tzinfo=UTC)

        age_seconds = (now - local_dt).total_seconds()
        is_recent_24h = -CLOCK_SKEW_TOLERANCE_SECONDS <= age_seconds <= 24 * 3600

        is_newer_than_remote = False
        is_older_than_remote = False
        if remote_created_at is not None:
            rem_dt = remote_created_at
            if rem_dt.tzinfo is None:
                rem_dt = rem_dt.replace(tzinfo=UTC)

            remote_diff_seconds = (rem_dt - local_dt).total_seconds()
            if remote_diff_seconds > CLOCK_SKEW_TOLERANCE_SECONDS:
                is_older_than_remote = True
            else:
                is_newer_than_remote = True

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

        # Not trusted: decide failure reason based on exact cause
        if not size_known:
            failure_reason = "Remote file size is unknown"
        elif size_mismatched:
            failure_reason = f"Size mismatch: local={info.size_bytes}B, remote={expected_size}B"
        elif is_older_than_remote:
            failure_reason = "Local file is older than remote Last-Modified timestamp"
        else:
            failure_reason = f"Local file is stale (age={int(age_seconds)}s > 24h)"

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

        if policy == EFilePolicy.SMART_REUSE:
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

    # 9. Fallback for unhandled / invalid policy
    raise ValueError(f"Unhandled file policy: {policy}")
