"""
loader.py — Type-aware, binary-first Parquet loading for VectorBT PRO.

Types are restored automatically from Parquet schema metadata — no re-casting
needed on load (float32, int64, uint32 are all locked into the file footer at
write time by converter.py).

Key functions
-------------
load_parquet        — single-file load with optional column selection + filters
load_month_range    — load and concatenate a range of monthly Parquet files
stream_monthly      — lazy generator over a directory of monthly files
read_file_metadata  — inspect Parquet footer metadata without loading row data
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Generator, List, Optional

logger = logging.getLogger(__name__)


# ── Single-file load ────────────────────────────────────────────────────────────

def load_parquet(
    path: Path | str,
    columns: Optional[list[str]] = None,
    filters: Optional[list] = None,
) -> "pd.DataFrame":
    """Load a single Parquet file into a pandas DataFrame.

    Types are restored automatically from the Parquet schema — no explicit
    casting is needed.

    Parameters
    ----------
    path:
        Path to the .parquet file.
    columns:
        Optional list of column names to load.  If None, all columns are
        returned.  Use this to avoid loading unused columns in RAM when
        running single-indicator sweeps.
    filters:
        Optional PyArrow filter list for predicate pushdown.
        Example: [('ts_event', '>=', 1672531200_000_000_000)]
        Rows that fail the filter are skipped at the storage layer — they
        never enter RAM.  Requires the file to be sorted by timestamp and
        have appropriate row group sizes.

    Returns
    -------
    pandas.DataFrame

    Example
    -------
    >>> from es_pipeline import load_parquet
    >>> df = load_parquet("parquet/ES_2023_01.parquet")
    >>> df = load_parquet("parquet/ES_2023_01.parquet", columns=["ts_event", "close"])
    """
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError("pip install pandas") from exc

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    logger.debug("[load] %s  columns=%s  filters=%s", path.name, columns, filters)
    df = pd.read_parquet(str(path), columns=columns, filters=filters)
    logger.debug("[load] Loaded %d rows from %s", len(df), path.name)
    return df


# ── Multi-file month range load ─────────────────────────────────────────────────

def load_month_range(
    data_dir: Path | str,
    start_month: str,
    end_month: str,
    *,
    columns: Optional[list[str]] = None,
    filters: Optional[list] = None,
    glob_pattern: str = "*.parquet",
    sort_after_concat: bool = True,
) -> "pd.DataFrame":
    """Load and concatenate monthly Parquet files within a date range.

    File selection is based on YYYY-MM strings in filenames.  Files are sorted
    alphabetically before concatenation, which preserves chronological order
    for the standard 'ES_YYYY_MM.parquet' naming convention.

    Parameters
    ----------
    data_dir:
        Directory containing monthly .parquet files.
    start_month:
        Inclusive start month as 'YYYY-MM', e.g. '2022-01'.
    end_month:
        Inclusive end month as 'YYYY-MM', e.g. '2022-12'.
    columns:
        Optional list of columns to load (passed to load_parquet).
    filters:
        Optional PyArrow filter for predicate pushdown (passed to load_parquet).
    glob_pattern:
        Glob pattern to find Parquet files. Default '*.parquet'.
    sort_after_concat:
        If True (default), sorts the concatenated DataFrame by the first column
        (assumed to be the timestamp) after concatenation.  Safe to disable if
        you know files are already in order.

    Returns
    -------
    pandas.DataFrame
        Concatenated DataFrame spanning the requested month range.

    Raises
    ------
    FileNotFoundError
        If no files are found in *data_dir* matching *glob_pattern*.
    ValueError
        If no files fall within the requested date range.

    Example
    -------
    >>> from es_pipeline import load_month_range
    >>> df = load_month_range("./parquet", "2022-01", "2022-06")
    """
    import pandas as pd  # noqa: PLC0415
    from .utils import month_range  # noqa: PLC0415

    data_dir = Path(data_dir)
    all_files = sorted(data_dir.glob(glob_pattern))

    if not all_files:
        raise FileNotFoundError(
            f"No Parquet files found in {data_dir} matching '{glob_pattern}'."
        )

    # Build the target month set for fast membership testing
    target_months = set(month_range(start_month, end_month))

    selected = [
        f for f in all_files
        if any(m.replace("-", "_") in f.stem or m in f.stem for m in target_months)
    ]

    if not selected:
        raise ValueError(
            f"No Parquet files in {data_dir} matched month range "
            f"{start_month} → {end_month}.  "
            f"Available files: {[f.name for f in all_files[:5]]} …"
        )

    logger.info(
        "[load_month_range] %s → %s: loading %d file(s).",
        start_month, end_month, len(selected),
    )

    frames = [load_parquet(f, columns=columns, filters=filters) for f in selected]
    df = pd.concat(frames, ignore_index=True)

    if sort_after_concat and len(df.columns) > 0:
        first_col = df.columns[0]
        df = df.sort_values(first_col).reset_index(drop=True)
        logger.debug("[load_month_range] Sorted by '%s'.", first_col)

    logger.info("[load_month_range] Loaded %d total rows.", len(df))
    return df


# ── Lazy streaming ──────────────────────────────────────────────────────────────

def stream_monthly(
    data_dir: Path | str,
    *,
    columns: Optional[list[str]] = None,
    filters: Optional[list] = None,
    glob_pattern: str = "*.parquet",
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> Generator[tuple[str, "pd.DataFrame"], None, None]:
    """Lazily yield (month_label, DataFrame) tuples from a directory of monthly files.

    This is the correct pattern for 10-year grid sweeps: load one month,
    run the sweep, capture results, then discard the DataFrame before loading
    the next.  Peak RAM is bounded to a single month at a time.

    Parameters
    ----------
    data_dir:
        Directory containing monthly .parquet files.
    columns:
        Optional column filter (passed to load_parquet).
    filters:
        Optional predicate pushdown filter (passed to load_parquet).
    glob_pattern:
        Glob pattern for finding Parquet files.
    start_month / end_month:
        Optional 'YYYY-MM' range filter.  Files outside this range are skipped.

    Yields
    ------
    (month_label, DataFrame)
        month_label is the file stem, e.g. 'ES_2023_01'.

    Example
    -------
    >>> from es_pipeline import stream_monthly, batch_grid_generator
    >>> all_results = []
    >>> for month, df in stream_monthly("./parquet"):
    ...     n_rows = len(df)
    ...     for sub_grid in batch_grid_generator(grid, n_rows):
    ...         portfolio = vbt.Portfolio.from_signals(df["close"], **sub_grid)
    ...         all_results.append(portfolio.stats())
    ...     del df   # explicit release before loading next month
    """
    from .utils import month_range  # noqa: PLC0415

    data_dir  = Path(data_dir)
    all_files = sorted(data_dir.glob(glob_pattern))

    if not all_files:
        logger.warning("[stream_monthly] No files found in %s", data_dir)
        return

    # Build optional range filter
    target_months: Optional[set] = None
    if start_month and end_month:
        months = month_range(start_month, end_month)
        target_months = {m.replace("-", "_") for m in months} | set(months)

    for f in all_files:
        if target_months is not None:
            if not any(m in f.stem for m in target_months):
                continue

        df = load_parquet(f, columns=columns, filters=filters)
        logger.info("[stream_monthly] Yielding %s (%d rows)", f.stem, len(df))
        yield f.stem, df


# ── Metadata inspection ─────────────────────────────────────────────────────────

def read_file_metadata(path: Path | str) -> dict[str, str]:
    """Read Parquet footer metadata without loading any row data.

    Delegates to metadata.read_metadata().  Provided here as a convenience
    import from the loader module so callers don't need to import two modules.

    Parameters
    ----------
    path:
        Path to a .parquet file.

    Returns
    -------
    dict[str, str]
        Decoded metadata dict (custom fields only; Arrow internals stripped).

    Example
    -------
    >>> from es_pipeline import read_file_metadata
    >>> meta = read_file_metadata("parquet/ES_2023_01.parquet")
    >>> print(meta.get("symbol"), meta.get("tick_size"))
    ES 0.25
    """
    from .metadata import read_metadata  # noqa: PLC0415
    return read_metadata(path)
