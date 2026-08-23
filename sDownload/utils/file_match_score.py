import logging
from datetime import UTC, datetime

from sDownload.interfaces.models import FileMatchScore
from sDownload.interfaces.protocols import FileStorageProtocol

logger = logging.getLogger(__name__)


async def calculate_file_match_score(
    storage: FileStorageProtocol,
    file_name: str,
    expected_size: int | None,
    remote_created_at: datetime | None = None,
    reference_time: datetime | None = None,
) -> FileMatchScore:
    """
    Inspects storage metadata for a given file and calculates a confidence score (0.0 to 1.0)
    evaluating whether the local file matches the remote target file.
    """
    info = await storage.get_data_info(file_name)

    file_exists = info is not None
    size_matched = False
    age_seconds = None
    score = 0.0
    reason = "File does not exist in storage"

    if info:
        has_expected_size = expected_size is not None and expected_size > 0
        size_matched = has_expected_size and info.size_bytes == expected_size

        if not size_matched:
            reason = f"Size mismatch: local={info.size_bytes}B, remote={expected_size}B"
        else:
            score = 0.50
            now = reference_time or datetime.now(UTC)
            now = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)

            created_at = info.created_at
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            else:
                created_at = created_at.astimezone(UTC)

            age_seconds = max(0.0, (now - created_at).total_seconds())

            if age_seconds <= 3600:  # 1 hour
                score += 0.30
            elif age_seconds <= 86400:  # 1 day
                score += 0.20
            elif age_seconds <= 7 * 86400:  # 1 week
                score += 0.10

            if remote_created_at:
                if remote_created_at.tzinfo is None:
                    remote_dt = remote_created_at.replace(tzinfo=UTC)
                else:
                    remote_dt = remote_created_at.astimezone(UTC)

                if created_at < remote_dt:
                    score = 0.0
                    reason = "Local file is older than remote Last-Modified timestamp"
                else:
                    score += 0.20
                    reason = f"Size matched ({info.size_bytes}B) and newer than remote"
            else:
                reason = f"Size matched ({info.size_bytes}B) with age {age_seconds:.0f}s"

            score = min(1.0, score)

    return FileMatchScore(
        score=score,
        file_exists=file_exists,
        size_matched=size_matched,
        age_seconds=age_seconds,
        reason=reason,
    )
