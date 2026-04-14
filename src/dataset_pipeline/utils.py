"""
utils.py — Shared utility functions.

All functions are pure or side-effect-free (no I/O beyond listing files).
"""

from __future__ import annotations

from pathlib import Path
from typing import List


def list_dbn_files(directory: Path | str) -> list[Path]:
    """Return sorted list of .dbn and .dbn.zst files in *directory*.

    Searches recursively — the DataBento client places downloaded files
    under a ``{job_id}/`` subdirectory, so a flat glob would miss them.

    Parameters
    ----------
    directory:
        Root directory to search (e.g. ``data/dbn``).

    Returns
    -------
    list[Path]
        Sorted paths. Empty list if directory doesn't exist or has no matches.

    Example
    -------
    >>> from dataset_pipeline import list_dbn_files
    >>> files = list_dbn_files("./data/dbn")
    """
    directory = Path(directory)
    if not directory.exists():
        return []
    return sorted(
        list(directory.rglob("*.dbn.zst")) + list(directory.rglob("*.dbn"))
    )


def list_parquet_files(directory: Path | str) -> list[Path]:
    """Return sorted list of .parquet files in *directory*.

    Parameters
    ----------
    directory:
        Directory to search. Not recursive.

    Returns
    -------
    list[Path]
        Sorted paths.
    """
    directory = Path(directory)
    if not directory.exists():
        return []
    return sorted(directory.glob("*.parquet"))


def month_range(start: str, end: str) -> list[str]:
    """Generate a list of 'YYYY-MM' strings between *start* and *end* inclusive.

    Parameters
    ----------
    start:
        Start month as 'YYYY-MM', e.g. '2022-01'.
    end:
        End month as 'YYYY-MM', e.g. '2022-06'.

    Returns
    -------
    list[str]
        All months in the range, inclusive.

    Raises
    ------
    ValueError
        If the format is invalid or start > end.

    Example
    -------
    >>> month_range("2022-01", "2022-04")
    ['2022-01', '2022-02', '2022-03', '2022-04']
    """
    from datetime import date  # noqa: PLC0415
    import calendar             # noqa: PLC0415

    def _parse(s: str) -> tuple[int, int]:
        parts = s.split("-")
        if len(parts) != 2:
            raise ValueError(f"Expected 'YYYY-MM', got: '{s}'")
        return int(parts[0]), int(parts[1])

    start_y, start_m = _parse(start)
    end_y, end_m     = _parse(end)

    if (start_y, start_m) > (end_y, end_m):
        raise ValueError(
            f"start '{start}' must be ≤ end '{end}'."
        )

    months = []
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    return months


def format_bytes(n_bytes: int | float) -> str:
    """Format a byte count as a human-readable string.

    Uses powers of 1024 (binary prefixes).

    Parameters
    ----------
    n_bytes:
        Number of bytes.

    Returns
    -------
    str
        Formatted string, e.g. '2.4 GB', '512.0 MB', '1.0 KB'.

    Example
    -------
    >>> format_bytes(40 * 1024**3)
    '40.0 GB'
    >>> format_bytes(1_048_576)
    '1.0 MB'
    """
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n_bytes) < 1024.0:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024.0
    return f"{n_bytes:.1f} PB"
