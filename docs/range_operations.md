# Range Operations (`sDownload.utils.range_operations`)

This module provides core mathematical and algorithmic utilities for connection partitioning, real-time download progress tracking, and graph-theoretic file reconstruction across partial or overlapping byte fragments.

---

## 1. `calculate_ranges`

### Description
Calculates and partitions byte ranges (`ChunkRange`) for a download job based on the total file size, target parallel connection count, and any pre-existing cached or recovered chunk fragments.

When resuming an interrupted download:
- Preserves all pre-existing chunk fragments.
- Identifies all missing intervals (*gaps*) between cached parts.
- Partitions each gap into proportional chunk sizes for parallel downloading.

### Signature
```python
def calculate_ranges(
    file_size: int,
    num_parts: int,
    cache: list[ChunkRange] | None = None
) -> list[ChunkRange]:
```

### Parameters
* `file_size` (*int*): Total size of the file in bytes.
* `num_parts` (*int*): Desired number of parallel connection partitions.
* `cache` (*list[ChunkRange] | None*): Optional list of pre-existing chunks on disk or storage.

### Returns
* `list[ChunkRange]`: Continuous, sorted list of ranges covering the entire file from `0` to `file_size - 1`.

---

### Example 1: Initial Partitioning from Scratch
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_ranges

# 100 MB file partitioned into 4 equal segments
file_size = 100 * 1024 * 1024  # 104,857,600 bytes
ranges = calculate_ranges(file_size, num_parts=4)

for r in ranges:
    print(r)
```

**Expected Output:**
```text
ChunkRange(start=0, end=26214399)
ChunkRange(start=26214400, end=52428799)
ChunkRange(start=52428800, end=78643199)
ChunkRange(start=78643200, end=None)
```
*(The last chunk uses `end=None` to read until EOF without premature truncation).*

---

### Example 2: Resume with Cached Chunks (Gap Filling)
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_ranges

# 200-byte file with recovered chunks: [0..10] and [50..100]
cached = [ChunkRange(0, 10), ChunkRange(50, 100)]
ranges = calculate_ranges(file_size=200, num_parts=4, cache=cached)

for r in ranges:
    print(r)
```

**Expected Output:**
```text
ChunkRange(start=0, end=10)       # Preserved recovered chunk
ChunkRange(start=11, end=49)      # Missing gap partitioned
ChunkRange(start=50, end=100)     # Preserved recovered chunk
ChunkRange(start=101, end=150)    # Missing gap partitioned
ChunkRange(start=151, end=None)   # Final gap to EOF
```

---

## 2. `calculate_downloaded_bytes`

### Description
Calculates the **exact total unique downloaded bytes** across active and completed chunks from `ChunkDownloadStats`.

Engineered specifically for the *Hot Path* (executed on every progress tick), this function has linear time complexity $O(k)$ and avoids object allocations, ensuring lightweight telemetry that is immune to:
* Overlapping chunk boundaries caused by dynamic resizing (*work-stealing*).
* Socket buffer overshoots on cancelled or stalled connections.
* Deprecated or cancelled chunks (only `DOWNLOADING` and `COMPLETED` chunks with `bytes_downloaded > 0` are counted).
* Continuous streaming downloads where `file_size=None`.

### Signature
```python
def calculate_downloaded_bytes(
    stats_list: Iterable[ChunkDownloadStats | None],
    file_size: int | None = None,
) -> int:
```

### Parameters
* `stats_list` (*Iterable[ChunkDownloadStats | None]*): Collection of chunk statistics.
* `file_size` (*int | None*): Total file size (if known), used as an upper-bound clamp to prevent counting buffer overshoots beyond file bounds.

### Returns
* `int`: Exact count of unique, non-overlapping downloaded bytes.

---

### Example 1: Dynamic Split with Overlapping Ranges
```python
from sDownload.interfaces.models import ChunkDownloadStats, ChunkRange, EDownloadStatus
from sDownload.utils.range_operations import calculate_downloaded_bytes

# Dynamic split scenario: Chunk A reached byte 59 before split, Chunk B started at 50
chunk_a = ChunkDownloadStats(
    chunk_file_name="chunk_a.bin",
    range=ChunkRange(0, 100),
    bytes_downloaded=60,  # Covered [0..59] (60 bytes)
    status=EDownloadStatus.DOWNLOADING,
)

chunk_b = ChunkDownloadStats(
    chunk_file_name="chunk_b.bin",
    range=ChunkRange(50, 100),
    bytes_downloaded=30,  # Covered [50..79] (30 bytes)
    status=EDownloadStatus.DOWNLOADING,
)

# Interval union of [0..59] and [50..79] is [0..79] = 80 unique bytes
total_bytes = calculate_downloaded_bytes([chunk_a, chunk_b], file_size=100)
print(f"Total downloaded: {total_bytes} bytes")
```

**Expected Output:**
```text
Total downloaded: 80 bytes
```

---

### Example 2: Ignoring Cancelled or Failed Chunks
```python
from sDownload.interfaces.models import ChunkDownloadStats, ChunkRange, EDownloadStatus
from sDownload.utils.range_operations import calculate_downloaded_bytes

chunk_ok = ChunkDownloadStats(
    chunk_file_name="ok.bin",
    range=ChunkRange(0, 49),
    bytes_downloaded=50,
    status=EDownloadStatus.COMPLETED,
)

chunk_cancelled = ChunkDownloadStats(
    chunk_file_name="dead.bin",
    range=ChunkRange(50, 99),
    bytes_downloaded=30,  # Downloaded 30 bytes before connection failed
    status=EDownloadStatus.CANCELLED,  # Cancelled status is filtered out
)

total_bytes = calculate_downloaded_bytes([chunk_ok, chunk_cancelled])
print(f"Total downloaded: {total_bytes} bytes")
```

**Expected Output:**
```text
Total downloaded: 50 bytes
```

---

## 3. `calculate_optimal_coverage`

### Description
Solves the **optimal file reconstruction and assembly problem**.

During multi-chunk downloads and dynamic resizing, multiple partial or overlapping file fragments may exist on disk. This function models available chunk fragments as nodes in a directed graph and uses Breadth-First Search (BFS / Dijkstra) to find the **shortest path with the minimum number of file operations** to reconstruct the entire file range from `0` to `file_size` with byte-level precision and zero duplicate writes.

If any byte interval is missing (*gap*), the function raises `ValueError` to prevent saving a corrupted file.

### Signature
```python
def calculate_optimal_coverage(
    chunks: list[ChunkRange],
    file_size: int | None = None
) -> list[ChunkFragment]:
```

### Parameters
* `chunks` (*list[ChunkRange]*): List of all available chunk ranges on storage.
* `file_size` (*int | None*): Target file size. If `None`, searches for an open-ended chunk (`end=None`).

### Returns
* `list[ChunkFragment]`: Ordered list of fragments, where each `ChunkFragment` specifies the source chunk range and the read boundary limit (`read_limit_qt_bytes`) for accurate truncation during merge.

---

### Example 1: Selecting Shortest Path and Pruning Redundant Fragments
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_optimal_coverage

# Available fragments on storage
chunks = [
    ChunkRange(0, 100),  # Chunk A: [0..100] (101 bytes)
    ChunkRange(50, 200),  # Chunk B: [50..200] (Redundant overlap)
    ChunkRange(101, 300),  # Chunk C: [101..300] (Continuous with Chunk A)
]

# Reconstruct file of 301 bytes (0..300)
coverage = calculate_optimal_coverage(chunks, file_size=301)

for fragment in coverage:
    print(f"Source: {fragment.range} | Read limit: {fragment.read_limit_qt_bytes}")
```

**Expected Output:**
```text
Source: ChunkRange(start=0, end=100) | Read limit: None (read entire fragment)
Source: ChunkRange(start=101, end=300) | Read limit: None (read entire fragment)
```
*(Redundant Chunk B was pruned automatically; assembly uses the minimum 2 merge operations).*

---

### Example 2: Crop Limits for In-Flight Extended Chunks
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_optimal_coverage

# Overlapping fragments requiring boundary crop
chunks = [
    ChunkRange(0, 150),
    ChunkRange(100, 200),
]

coverage = calculate_optimal_coverage(chunks, file_size=201)

for fragment in coverage:
    print(f"Source: {fragment.range} | Read limit: {fragment.read_limit_qt_bytes}")
```

**Expected Output:**
```text
Source: ChunkRange(start=0, end=150) | Read limit: 100 (read only first 100 bytes)
Source: ChunkRange(start=100, end=200) | Read limit: None (read remaining bytes to end)
```

---

## 4. Summary Table

| Function | Primary Responsibility | Complexity | Execution Phase |
|---|---|---|---|
| `calculate_ranges` | Initial partitioning and resume gap filling | $O(N \log N)$ | Download start (`on_start`) |
| `calculate_downloaded_bytes` | Unique byte accumulation for progress and stats | $O(k \log k)$ | Hot path on every tick (`on_update`) |
| `calculate_optimal_coverage` | Graph assembly and crop limit resolution | $O(V + E)$ (BFS) | Finalization and merge (`_finalize`) |
