from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FileMatchScore:
    """
    Heuristic confidence score indicating the likelihood that an existing file
    in storage is identical, valid, and up-to-date with respect to remote metadata.
    """

    score: float
    file_exists: bool
    size_matched: bool
    age_seconds: float | None
    reason: str
