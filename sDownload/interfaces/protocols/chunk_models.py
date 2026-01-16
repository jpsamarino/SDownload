from typing import NamedTuple, TypedDict


class ChunkRange(NamedTuple):
    start: int
    end: int | None

    def __str__(self) -> str:
        return f"{self.start}_{self.end or 'EOF'}"

    @property
    def length(self) -> int | None:
        if self.end is None:
            return None
        return self.end - self.start + 1


class ChunkOperationPlanModel(TypedDict):
    chunks_to_start: list[ChunkRange] | None
    chunks_to_stop: list[ChunkRange] | None
