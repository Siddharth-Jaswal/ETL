from __future__ import annotations

from pathlib import Path
from typing import Iterable


DEFAULT_CHUNK_RATIOS = (0.02, 0.05, 0.1, 0.2)
MIN_CHUNK_SIZE_MB = 1
MAX_CHUNK_SIZE_MB = 64


def get_file_size_mb(file_path: Path) -> float:
    if not file_path.exists():
        return 0.0
    return round(file_path.stat().st_size / (1024 * 1024), 4)


def determine_chunk_sizes_mb(
    db_size_mb: float,
    ratios: tuple[float, float, float, float] = DEFAULT_CHUNK_RATIOS,
    min_mb: int = MIN_CHUNK_SIZE_MB,
    max_mb: int = MAX_CHUNK_SIZE_MB,
) -> list[int]:
    if db_size_mb <= 0:
        return [min_mb, min_mb * 2, min_mb * 4, min_mb * 8]

    chunk_sizes: list[int] = []
    for ratio in ratios:
        candidate = int(round(db_size_mb * ratio))
        candidate = max(min_mb, min(max_mb, candidate))
        chunk_sizes.append(candidate)

    normalized = sorted(set(chunk_sizes))
    while len(normalized) < 4:
        next_size = min(max_mb, normalized[-1] + min_mb) if normalized else min_mb
        if next_size == normalized[-1]:
            break
        normalized.append(next_size)

    return normalized[:4]


def estimate_record_size_bytes(record: dict[str, object]) -> int:
    return sum(len(str(value).encode("utf-8")) for value in record.values())


def determine_common_chunk_sizes_mb(
    db_sizes_mb: Iterable[float],
    ratios: tuple[float, float, float, float] = DEFAULT_CHUNK_RATIOS,
    min_mb: int = MIN_CHUNK_SIZE_MB,
    max_mb: int = MAX_CHUNK_SIZE_MB,
) -> list[int]:
    sizes = [size for size in db_sizes_mb if size > 0]
    if not sizes:
        return determine_chunk_sizes_mb(0, ratios=ratios, min_mb=min_mb, max_mb=max_mb)

    # Use the smallest database as the baseline so every chosen chunk size
    # remains practical across the full benchmark set.
    baseline_size_mb = min(sizes)
    return determine_chunk_sizes_mb(
        baseline_size_mb,
        ratios=ratios,
        min_mb=min_mb,
        max_mb=max_mb,
    )
