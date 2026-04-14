"""
converter.py — DBN → Parquet conversion pipeline with dtype optimisation.

Default path:  DBN → optimise dtypes → inject metadata → write Parquet (snappy)
Fallback path: DBN → CSV  (raw, no type optimisation; use for debugging/inspection)

The key design principle is that types are cast *once* before writing to Parquet,
so they are locked into the schema and never need re-casting on load.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Config ──────────────────────────────────────────────────────────────────────

@dataclass
class ConversionConfig:
    """Controls type optimisation and output format during conversion.

    Attributes
    ----------
    price_cols:
        Column names containing OHLC price data.  Cast to price_dtype.
    volume_col:
        Column name for trade volume.  Cast to volume_dtype.
    timestamp_col:
        Column name for the primary timestamp.  Kept as int64 Unix.
    price_dtype:
        Target dtype for price columns.  'float32' saves 50% RAM vs float64
        with no meaningful precision loss for ES tick data ($0.25 increments).
    volume_dtype:
        Target dtype for volume.  'uint32' saves space when negatives are
        impossible (raw bar volume is always non-negative).
    timestamp_dtype:
        Target dtype for the timestamp column.
    parquet_compression:
        Parquet compression codec.  'snappy' is the best balance of CPU speed
        and file-size reduction for columnar numeric data.
        Alternatives: 'zstd' (better ratio, more CPU), 'none'.
    fallback_csv:
        If True, also write a .csv file alongside the Parquet output.
        Intended for small sample files used in visual debugging only.
    row_group_size:
        Number of rows per Parquet row group.  Controls predicate pushdown
        granularity.  500_000 rows ≈ ~8 MB per group for ES 1-minute data.
    """
    price_cols: list[str]  = field(default_factory=lambda: ["open", "high", "low", "close"])
    volume_col: str        = "volume"
    timestamp_col: str     = "ts_event"
    price_dtype: str       = "float32"
    volume_dtype: str      = "uint32"
    timestamp_dtype: str   = "int64"
    parquet_compression: str = "snappy"
    fallback_csv: bool     = False
    row_group_size: int    = 500_000


# ── dtype optimisation ──────────────────────────────────────────────────────────

def optimize_dtypes(df, cfg: ConversionConfig):
    """Cast DataFrame columns to memory-efficient types in-place.

    Applies the type mapping defined in *cfg*.  Columns that are absent in
    *df* are silently skipped so the function is safe to call on partial schemas.

    Parameters
    ----------
    df:
        A pandas DataFrame (typically from DBNStore.to_df() or pd.read_csv()).
    cfg:
        ConversionConfig controlling which columns get which dtypes.

    Returns
    -------
    pandas.DataFrame
        The same DataFrame with updated column dtypes.

    Example
    -------
    >>> from es_pipeline import ConversionConfig, optimize_dtypes
    >>> cfg = ConversionConfig()
    >>> df  = optimize_dtypes(df, cfg)
    """
    import pandas as pd  # noqa: PLC0415

    # Price columns → float32
    present_price = [c for c in cfg.price_cols if c in df.columns]
    if present_price:
        df[present_price] = df[present_price].astype(cfg.price_dtype)
        logger.debug("Cast %s → %s", present_price, cfg.price_dtype)

    # Volume → uint32  (skip if column has NaNs — uint32 can't represent NaN)
    if cfg.volume_col in df.columns:
        if not df[cfg.volume_col].isna().any():
            df[cfg.volume_col] = df[cfg.volume_col].astype(cfg.volume_dtype)
            logger.debug("Cast %s → %s", cfg.volume_col, cfg.volume_dtype)
        else:
            logger.warning(
                "Column '%s' contains NaN — skipping uint32 cast. "
                "Fill NaNs first if uint32 is required.",
                cfg.volume_col,
            )

    # Timestamp → int64
    if cfg.timestamp_col in df.columns:
        col = df[cfg.timestamp_col]
        # If it's already a datetime, convert to Unix nanoseconds first
        if hasattr(col, "dt"):
            df[cfg.timestamp_col] = col.astype("int64")
        else:
            df[cfg.timestamp_col] = col.astype(cfg.timestamp_dtype)
        logger.debug("Cast %s → %s", cfg.timestamp_col, cfg.timestamp_dtype)

    return df


# ── Single-file conversion ──────────────────────────────────────────────────────

def dbn_to_parquet(
    dbn_path: Path | str,
    out_path: Path | str,
    cfg: Optional[ConversionConfig] = None,
    metadata: Optional[dict[str, str]] = None,
) -> Path:
    """Convert a single DBN (or .dbn.zst) file to an optimised Parquet file.

    Pipeline:
      1. Load DBN → pandas DataFrame via DataBento's DBNStore
      2. optimise_dtypes() — cast prices/volume/timestamps
      3. inject_metadata() — stamp Parquet footer with custom fields
      4. pq.write_table()  — write with snappy compression

    Parameters
    ----------
    dbn_path:
        Path to the source .dbn or .dbn.zst file.
    out_path:
        Destination .parquet file path.
        Parent directory is created automatically.
    cfg:
        ConversionConfig. Uses sensible defaults if None.
    metadata:
        Flat string→string dict from build_metadata().  Optional but strongly
        recommended for auditability.

    Returns
    -------
    Path
        The written Parquet file path.

    Example
    -------
    >>> from es_pipeline import ConversionConfig, dbn_to_parquet, build_metadata
    >>> from es_pipeline import InstrumentMeta, ProvenanceMeta
    >>> meta = build_metadata(InstrumentMeta(), ProvenanceMeta())
    >>> dbn_to_parquet("ES_2023_01.dbn.zst", "parquet/ES_2023_01.parquet", metadata=meta)
    """
    try:
        import databento as db      # noqa: PLC0415
        import pyarrow as pa        # noqa: PLC0415
        import pyarrow.parquet as pq  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "Required packages: pip install databento pyarrow"
        ) from exc

    from .metadata import inject_metadata  # noqa: PLC0415

    cfg = cfg or ConversionConfig()
    dbn_path = Path(dbn_path)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("[convert] %s → %s", dbn_path.name, out_path.name)

    # 1. DBN → DataFrame
    store = db.DBNStore.from_file(str(dbn_path))
    df = store.to_df()

    # 2. Optimise types
    df = optimize_dtypes(df, cfg)

    # 3. Arrow table + metadata
    table = pa.Table.from_pandas(df)
    if metadata:
        table = inject_metadata(table, metadata)

    # 4. Write Parquet
    pq.write_table(
        table,
        str(out_path),
        compression=cfg.parquet_compression,
        row_group_size=cfg.row_group_size,
    )

    size_mb = out_path.stat().st_size / 1_048_576
    logger.info("[convert] Written: %s  (%.1f MB)", out_path.name, size_mb)

    # Optional CSV fallback
    if cfg.fallback_csv:
        csv_path = out_path.with_suffix(".csv")
        dbn_to_csv(dbn_path, csv_path)

    return out_path


def dbn_to_csv(
    dbn_path: Path | str,
    out_path: Path | str,
) -> Path:
    """Convert a DBN file to CSV.

    This is the fallback/debug path.  No type optimisation is applied —
    the CSV will use DataBento's default text representation.
    Intended for visual inspection of small sample files only.

    Parameters
    ----------
    dbn_path:
        Path to the source .dbn or .dbn.zst file.
    out_path:
        Destination .csv file path.

    Returns
    -------
    Path
        The written CSV file path.
    """
    try:
        import databento as db  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError("pip install databento") from exc

    dbn_path = Path(dbn_path)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("[csv-fallback] %s → %s", dbn_path.name, out_path.name)
    store = db.DBNStore.from_file(str(dbn_path))
    store.to_csv(str(out_path))

    size_mb = out_path.stat().st_size / 1_048_576
    logger.info("[csv-fallback] Written: %s  (%.1f MB)", out_path.name, size_mb)
    return out_path


# ── Batch directory conversion ──────────────────────────────────────────────────

def convert_directory(
    input_dir: Path | str,
    output_dir: Path | str,
    cfg: Optional[ConversionConfig] = None,
    metadata: Optional[dict[str, str]] = None,
    *,
    glob_pattern: str = "*.dbn*",
    skip_existing: bool = True,
) -> list[Path]:
    """Convert all DBN files in *input_dir* to Parquet files in *output_dir*.

    Searches *input_dir* recursively so files inside DataBento's automatic
    ``{job_id}/`` subdirectory are found without any extra configuration.

    Files are processed in sorted order (chronological for monthly splits).
    Each input file maps 1:1 to an output .parquet file with the same stem,
    written flat into *output_dir* regardless of subdirectory depth.

    Parameters
    ----------
    input_dir:
        Root directory containing .dbn or .dbn.zst files (searched recursively).
    output_dir:
        Directory where .parquet files will be written (always flat).
    cfg:
        ConversionConfig. Uses sensible defaults if None.
    metadata:
        Shared metadata dict injected into every output file.
    glob_pattern:
        Pattern used to find source files via ``rglob``.
        Default catches both '.dbn' and '.dbn.zst' at any depth.
    skip_existing:
        If True (default), skip files whose Parquet equivalent already exists.
        Set False to force re-conversion.

    Returns
    -------
    list[Path]
        Paths of all written (or already-existing, if skipped) Parquet files.

    Example
    -------
    >>> from es_pipeline import ConversionConfig, convert_directory, build_metadata
    >>> from es_pipeline import InstrumentMeta, ProvenanceMeta
    >>> cfg  = ConversionConfig(fallback_csv=False)
    >>> meta = build_metadata(InstrumentMeta(symbol="ES"), ProvenanceMeta())
    >>> parquet_files = convert_directory("./data/dbn", "./data/parquet", cfg, meta)
    """
    input_dir  = Path(input_dir)
    output_dir = Path(output_dir)
    cfg        = cfg or ConversionConfig()

    dbn_files = sorted(input_dir.rglob(glob_pattern))
    if not dbn_files:
        logger.warning("[convert_directory] No files matched '%s' in %s",
                       glob_pattern, input_dir)
        return []

    logger.info("[convert_directory] Found %d file(s) to convert.", len(dbn_files))
    written: list[Path] = []

    for dbn_file in dbn_files:
        # Derive output path: strip all extensions (handles .dbn.zst double ext)
        stem = dbn_file.name.split(".")[0]  # e.g. "ES_2023_01"
        out_path = output_dir / f"{stem}.parquet"

        if skip_existing and out_path.exists():
            logger.info("[skip] %s already exists.", out_path.name)
            written.append(out_path)
            continue

        try:
            written.append(dbn_to_parquet(dbn_file, out_path, cfg, metadata))
        except Exception as exc:
            logger.error("[convert_directory] Failed on %s: %s", dbn_file.name, exc)
            raise

    logger.info("[convert_directory] Done. %d Parquet file(s) ready.", len(written))
    return written
