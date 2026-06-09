from typing import NamedTuple, TypeAlias


class ChunkRange(NamedTuple):
    """
    Represents a byte range used for chunked downloads.

    The range is defined as **inclusive** on both ends.
    An `end` value of `None` indicates an open-ended range that goes until EOF.

    Attributes:
        start (int):
            Starting byte offset (0-based, inclusive).

        end (int | None):
            Ending byte offset (inclusive), or `None` to represent EOF.

    Examples:
        >>> r = ChunkRange(0, 9)
        >>> 5 in r
        True

        >>> sub = ChunkRange(3, 7)
        >>> sub in r
        True
    """

    start: int
    end: int | None

    def __str__(self) -> str:
        return f"{self.start}_{self.end if self.end is not None else 'EOF'}"

    @property
    def length(self) -> int | None:
        if self.end is None:
            return None
        return self.end - self.start + 1

    def __contains__(self, item: object) -> bool:

        if isinstance(item, int):
            if item < self.start:
                return False
            if self.end is not None and item > self.end:
                return False
            return True
        if isinstance(item, ChunkRange):
            if item.start < self.start:
                return False

            if self.end is not None:
                if item.end is None:
                    return False
                if item.end > self.end:
                    return False

            if item.end is not None and item.end < item.start:
                return False

            return True

        return False


class StartChunkAction(NamedTuple):
    range: ChunkRange
    target_speed_bps: int | None = None


class CancelChunkAction(NamedTuple):
    range: ChunkRange


class ResizeChunkAction(NamedTuple):
    current_range: ChunkRange
    new_range: ChunkRange
    target_speed_bps: int | None = None


class SetSpeedAction(NamedTuple):
    range: ChunkRange
    target_speed_bps: int | None


class StrategyAction:
    """Namespace for the actions that a strategy can emit."""

    __slots__ = ()
    Start = StartChunkAction
    Cancel = CancelChunkAction
    Resize = ResizeChunkAction
    SetSpeed = SetSpeedAction


AnyStrategyAction: TypeAlias = (
    StartChunkAction | CancelChunkAction | ResizeChunkAction | SetSpeedAction
)


class ChunkFragment(NamedTuple):
    """
    Represents a specific fragment of a chunk that should be read.
    Used when multiple chunks overlap and we need to pick specific parts.
    """

    range: ChunkRange
    read_limit_qt_bytes: int | None
