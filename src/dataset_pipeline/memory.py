"""
memory.py — RAM estimation and VBT grid-sweep gatekeeper.

The core insight from the doc: in VBT, every parameter combination adds a
column, so a 1,000-combination grid on a 10-year dataset multiplies your
memory footprint by 1,000.  This module gives you the tools to measure that
before it happens.

Key functions
-------------
estimate_ram_bytes      — pure maths, no side effects
check_ram_gate          — measure + warn; optionally auto-batch
split_param_grid        — dict-of-lists → list-of-smaller-dicts
batch_grid_generator    — yields safe-sized param sub-grids lazily
"""

from __future__ import annotations

import gc
import itertools
import logging
import math
from dataclasses import dataclass
from typing import Generator, Optional

logger = logging.getLogger(__name__)


# ── Config ──────────────────────────────────────────────────────────────────────

@dataclass
class MemoryConfig:
    """Thresholds and assumptions for the RAM gatekeeper.

    Attributes
    ----------
    warn_threshold:
        Fraction of total physical RAM at which a warning is emitted.
        Default 0.75 (75%) gives headroom for OS, Python overhead, etc.
    hard_threshold:
        Fraction at which the gatekeeper raises (or forces batching).
        Default 0.85.  Above this, VBT grid sweeps will almost certainly
        trigger disk-swap and appear to hang.
    bytes_per_value:
        Expected bytes per data cell.  4 for float32 (recommended),
        8 for float64 (default numpy/pandas dtype).
    n_columns:
        Number of OHLCV columns expected.  Default 5.
    safety_multiplier:
        Headroom factor applied to the raw estimate to account for VBT
        internal copies, index arrays, and Python object overhead.
        Default 1.5 (i.e. estimate × 1.5).
    """
    warn_threshold: float     = 0.75
    hard_threshold: float     = 0.85
    bytes_per_value: int      = 4
    n_columns: int            = 5
    safety_multiplier: float  = 1.5


# ── Pure estimation ─────────────────────────────────────────────────────────────

def estimate_ram_bytes(
    n_rows: int,
    n_params: int,
    cfg: Optional[MemoryConfig] = None,
) -> int:
    """Estimate RAM required for a VBT grid sweep in bytes.

    Formula:
        RAM ≈ (R × C × P × bytes_per_value) × safety_multiplier

    Parameters
    ----------
    n_rows:
        Number of time steps (rows) in the dataset.
    n_params:
        Total number of parameter combinations in the grid.
        Use count_param_combinations() to compute this from a grid dict.
    cfg:
        MemoryConfig. Uses defaults if None.

    Returns
    -------
    int
        Estimated RAM in bytes.

    Example
    -------
    >>> from es_pipeline import estimate_ram_bytes, MemoryConfig, format_bytes
    >>> cfg = MemoryConfig(bytes_per_value=4)   # float32
    >>> n   = estimate_ram_bytes(2_600_000, 1000, cfg)
    >>> print(format_bytes(n))
    '78.0 GB'
    """
    cfg = cfg or MemoryConfig()
    raw = n_rows * cfg.n_columns * n_params * cfg.bytes_per_value
    return int(raw * cfg.safety_multiplier)


def count_param_combinations(grid: dict) -> int:
    """Count total combinations in a VBT-style param grid (dict of lists).

    Parameters
    ----------
    grid:
        Dict mapping parameter names to lists of values,
        e.g. {'ema_fast': [5, 10, 20], 'ema_slow': [50, 100]}.

    Returns
    -------
    int
        Product of all list lengths (i.e. the full Cartesian product size).

    Example
    -------
    >>> count_param_combinations({'ema_fast': [5, 10, 20], 'ema_slow': [50, 100]})
    6
    """
    total = 1
    for values in grid.values():
        total *= len(values)
    return total


def available_ram_bytes() -> int:
    """Return available physical RAM in bytes using psutil.

    Falls back to a conservative 8 GB if psutil is unavailable.
    """
    try:
        import psutil  # noqa: PLC0415
        return psutil.virtual_memory().available
    except ImportError:
        logger.warning(
            "psutil not installed — assuming 8 GB available RAM. "
            "Install with: pip install psutil"
        )
        return 8 * 1024 ** 3


def total_ram_bytes() -> int:
    """Return total physical RAM in bytes using psutil."""
    try:
        import psutil  # noqa: PLC0415
        return psutil.virtual_memory().total
    except ImportError:
        return 8 * 1024 ** 3


# ── Gatekeeper ──────────────────────────────────────────────────────────────────

def check_ram_gate(
    n_rows: int,
    n_params: int,
    cfg: Optional[MemoryConfig] = None,
    *,
    auto_batch: bool = False,
) -> Optional[int]:
    """Pre-flight RAM check before launching a VBT grid sweep.

    Behaviour
    ---------
    - Always logs the estimated vs available RAM.
    - If estimate < warn_threshold × total_ram → logs "OK".
    - If estimate ≥ warn_threshold but < hard_threshold → warns and continues.
    - If estimate ≥ hard_threshold:
        - auto_batch=False → raises MemoryError with a clear message.
        - auto_batch=True  → returns the number of batches needed to stay
                             under warn_threshold.  Does NOT raise.

    Parameters
    ----------
    n_rows:
        Number of rows in the dataset.
    n_params:
        Total number of parameter combinations.
    cfg:
        MemoryConfig. Uses defaults if None.
    auto_batch:
        If True, return the number of batches instead of raising on
        hard threshold breach.

    Returns
    -------
    None
        If estimated RAM is below the hard threshold (safe to proceed).
    int
        Number of batches required, if auto_batch=True and hard threshold
        would be exceeded.

    Raises
    ------
    MemoryError
        If estimated RAM exceeds hard_threshold and auto_batch=False.

    Example
    -------
    >>> from es_pipeline import MemoryConfig, check_ram_gate
    >>> cfg = MemoryConfig()
    >>> n_batches = check_ram_gate(2_600_000, 1000, cfg, auto_batch=True)
    >>> if n_batches:
    ...     for sub_grid in split_param_grid(grid, n_batches):
    ...         run_sweep(sub_grid)
    """
    from .utils import format_bytes  # noqa: PLC0415

    cfg        = cfg or MemoryConfig()
    total_ram  = total_ram_bytes()
    avail_ram  = available_ram_bytes()
    estimated  = estimate_ram_bytes(n_rows, n_params, cfg)

    warn_limit = total_ram * cfg.warn_threshold
    hard_limit = total_ram * cfg.hard_threshold

    logger.info(
        "[ram_gate] Estimated: %s | Available: %s | Total: %s",
        format_bytes(estimated), format_bytes(avail_ram), format_bytes(total_ram),
    )

    if estimated < warn_limit:
        logger.info("[ram_gate] ✓ RAM estimate is within safe limits.")
        return None

    if estimated < hard_limit:
        logger.warning(
            "[ram_gate] ⚠ Estimated RAM (%s) exceeds %.0f%% of total (%s). "
            "Consider batching the parameter grid.  "
            "Call check_ram_gate(..., auto_batch=True) to get a batch count.",
            format_bytes(estimated), cfg.warn_threshold * 100, format_bytes(total_ram),
        )
        return None

    # Exceeds hard threshold
    n_batches = math.ceil(estimated / warn_limit)
    msg = (
        f"[ram_gate] ✗ Estimated sweep RAM ({format_bytes(estimated)}) exceeds "
        f"{cfg.hard_threshold:.0%} of total RAM ({format_bytes(total_ram)}). "
        f"Split the grid into at least {n_batches} batches."
    )

    if auto_batch:
        logger.warning(msg)
        logger.info("[ram_gate] auto_batch=True → returning n_batches=%d", n_batches)
        return n_batches

    raise MemoryError(msg)


# ── Grid splitting ──────────────────────────────────────────────────────────────

def split_param_grid(grid: dict, n_batches: int) -> list[dict]:
    """Split a VBT-style param grid into *n_batches* sub-grids.

    Flattens the Cartesian product of all parameter combinations, splits
    the resulting list evenly, and returns each chunk as a dict of lists
    in VBT's expected format.

    Parameters
    ----------
    grid:
        Dict of parameter name → list of values.
    n_batches:
        Number of sub-grids to produce. Use the value returned by
        check_ram_gate(..., auto_batch=True).

    Returns
    -------
    list[dict]
        List of sub-grid dicts.  The union of all sub-grids covers every
        combination in the original grid exactly once.

    Example
    -------
    >>> grid = {'fast': [5, 10], 'slow': [50, 100, 200]}
    >>> batches = split_param_grid(grid, 2)
    >>> # batch 0 covers 3 combinations, batch 1 covers 3 combinations
    """
    keys   = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    total  = len(combos)

    if n_batches >= total:
        # More batches than combinations — just give one combo per batch
        n_batches = total

    chunk_size = math.ceil(total / n_batches)
    batches: list[dict] = []

    for i in range(0, total, chunk_size):
        chunk = combos[i : i + chunk_size]
        sub_grid: dict[str, list] = {k: [] for k in keys}
        for combo in chunk:
            for k, v in zip(keys, combo):
                sub_grid[k].append(v)
        batches.append(sub_grid)

    logger.info(
        "[split_grid] Split %d combinations into %d batches of ~%d each.",
        total, len(batches), chunk_size,
    )
    return batches


def batch_grid_generator(
    grid: dict,
    n_rows: int,
    cfg: Optional[MemoryConfig] = None,
    *,
    gc_between_batches: bool = True,
) -> Generator[dict, None, None]:
    """Yield safe-sized sub-grids, automatically calling gc.collect() between batches.

    This is the highest-level memory management function.  It combines
    check_ram_gate, split_param_grid, and gc.collect() into one lazy iterator
    suitable for direct use in a VBT sweep loop.

    Parameters
    ----------
    grid:
        Full parameter grid dict.
    n_rows:
        Number of rows in the dataset for the current period being swept.
    cfg:
        MemoryConfig. Uses defaults if None.
    gc_between_batches:
        If True (default), calls gc.collect() after each yielded batch is
        consumed, before generating the next one.  Helps reclaim memory from
        the previous sweep iteration before loading the next.

    Yields
    ------
    dict
        Sub-grid dict safe to pass directly to VBT.

    Example
    -------
    >>> for sub_grid in batch_grid_generator(grid, n_rows=2_600_000):
    ...     portfolio = vbt.Portfolio.from_signals(..., **sub_grid)
    ...     results.append(portfolio.stats())
    """
    cfg      = cfg or MemoryConfig()
    n_params = count_param_combinations(grid)
    n_batches = check_ram_gate(n_rows, n_params, cfg, auto_batch=True)

    if n_batches is None:
        # Fits in RAM — yield the whole grid as a single batch
        logger.info("[batch_generator] Grid fits in RAM — no splitting needed.")
        yield grid
        return

    batches = split_param_grid(grid, n_batches)
    logger.info("[batch_generator] Yielding %d batches.", len(batches))

    for i, batch in enumerate(batches):
        logger.info(
            "[batch_generator] Batch %d/%d (%d combinations)",
            i + 1, len(batches), count_param_combinations(batch),
        )
        yield batch
        if gc_between_batches:
            collected = gc.collect()
            logger.debug("[batch_generator] gc.collect() freed %d objects.", collected)
