from typing import NamedTuple, TypedDict


class ChunkRange(NamedTuple):
    start: int
    end: int | None


class ChunkOperationPlanModel(TypedDict):
    chunks_to_start: list[ChunkRange] | None
    chunks_to_stop: list[ChunkRange] | None
