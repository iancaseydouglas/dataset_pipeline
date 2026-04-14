"""
metadata.py — Parquet metadata management.

Four dataclass categories mirror the industry-standard fields from the doc:
  InstrumentMeta  — market & instrument (symbol, tick size, point value, …)
  ProvenanceMeta  — time-series provenance (provider, schema, timezone, …)
  StrategyMeta    — grid sweep / lab-notebook fields (strategy, param grid, …)
  ComputeMeta     — computational hints for readers (row groups, sort order, …)

build_metadata()   → flat dict ready for Parquet file-level metadata
inject_metadata()  → stamps a PyArrow Table with the dict
read_metadata()    → reads metadata back from any .parquet file
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ── Config dataclasses ──────────────────────────────────────────────────────────

@dataclass
class InstrumentMeta:
    """Market and instrument identifiers.

    Attributes
    ----------
    symbol:
        Root symbol, e.g. 'ES' for E-mini S&P 500.
    contract_month:
        Active contract expiry in YYYY-MM format, e.g. '2026-06'.
        Leave empty for continuous/adjusted series.
    tick_size:
        Minimum price increment as a float.  ES ticks in $0.25 increments.
    point_value:
        Dollar value per full point move. ES = $50/point.
    exchange:
        Primary listing exchange. Used for trading hours and holiday calendars.
    asset_class:
        Broad asset category — drives slippage/fee model routing in VBT.
    """
    symbol: str        = "ES"
    contract_month: str = ""
    tick_size: float   = 0.25
    point_value: float = 50.0
    exchange: str      = "CME"
    asset_class: str   = "Futures"


@dataclass
class ProvenanceMeta:
    """Time-series data provenance and lineage.

    Attributes
    ----------
    data_provider:
        Source of the raw data.
    dataset_schema:
        DataBento schema string, e.g. 'ohlcv-1m', 'mbo', 'mbp-1'.
    timezone:
        Timezone of stored timestamps. Always store UTC; convert on display.
    is_adjusted:
        True if prices have been back-adjusted for contract rolls.
    aggregation:
        Bar/sample frequency as a string, e.g. '1min', '5min', 'tick'.
    timestamp_unit:
        Unit of the Unix timestamp column: 'ns' (nanoseconds) or 's' (seconds).
    """
    data_provider: str   = "databento"
    dataset_schema: str  = "ohlcv-1m"
    timezone: str        = "UTC"
    is_adjusted: bool    = True
    aggregation: str     = "1min"
    timestamp_unit: str  = "ns"


@dataclass
class StrategyMeta:
    """Grid sweep and strategy lab-notebook fields.

    Storing these inside the Parquet file footer eliminates the need
    for external tracking spreadsheets or complex file naming conventions.

    Attributes
    ----------
    strategy_name:
        Human-readable strategy identifier.
    parameter_grid:
        Dict of parameter arrays that were swept, e.g.
        {'ema_fast': [5, 10, 20], 'ema_slow': [50, 100]}.
        Serialised to JSON string inside the file.
    vbt_version:
        VectorBT PRO version used for reproducibility.
    split_id:
        Walk-forward fold identifier, e.g. 'fold_1', 'fold_3_test'.
        Enables instant train/test filtering without loading full dataset.
    creation_date:
        ISO date the file was produced.
    """
    strategy_name: str         = ""
    parameter_grid: dict       = field(default_factory=dict)
    vbt_version: str           = ""
    split_id: str              = ""
    creation_date: str         = ""


@dataclass
class ComputeMeta:
    """Computational hints for Parquet readers and VBT loaders.

    Attributes
    ----------
    row_group_size:
        Target row-group size as a descriptive string. Parquet predicate
        pushdown operates at row-group granularity — larger groups reduce
        overhead; smaller groups improve column-range query speed.
        '256MB' is a good default for monthly ES 1-minute data.
    sorted_by:
        Name of the column the data is sorted by.  'timestamp' enables
        fast as-of joins and binary-search range queries.
    is_sorted:
        Boolean flag — lets downstream loaders skip re-sorting.
    price_dtype:
        Dtype used for OHLC columns, for loader validation.
    timestamp_dtype:
        Dtype used for the timestamp column.
    """
    row_group_size: str  = "256MB"
    sorted_by: str       = "timestamp"
    is_sorted: bool      = True
    price_dtype: str     = "float32"
    timestamp_dtype: str = "int64"


# ── Builders ────────────────────────────────────────────────────────────────────

def build_metadata(
    instrument: Optional[InstrumentMeta] = None,
    provenance: Optional[ProvenanceMeta] = None,
    strategy: Optional[StrategyMeta]     = None,
    compute: Optional[ComputeMeta]       = None,
) -> dict[str, str]:
    """Merge all four metadata categories into a flat string→string dict.

    All values are serialised to strings because the Parquet key-value
    metadata spec requires byte-string keys and values.  Dicts (parameter_grid)
    are JSON-encoded; booleans become 'True'/'False'.

    Parameters
    ----------
    instrument, provenance, strategy, compute:
        Any combination of the four config dataclasses.  Missing categories
        are silently skipped.

    Returns
    -------
    dict[str, str]
        Flat string dict suitable for pa.Table.replace_schema_metadata().

    Example
    -------
    >>> meta = build_metadata(
    ...     InstrumentMeta(symbol="ES", tick_size=0.25),
    ...     ProvenanceMeta(dataset_schema="ohlcv-1m"),
    ... )
    >>> inject_metadata(table, meta)
    """
    sources: list[Any] = []
    if instrument is not None:
        sources.append(asdict(instrument))
    if provenance is not None:
        sources.append(asdict(provenance))
    if strategy is not None:
        d = asdict(strategy)
        # JSON-encode the parameter_grid dict so it survives the string round-trip
        d["parameter_grid"] = json.dumps(d.get("parameter_grid", {}))
        sources.append(d)
    if compute is not None:
        sources.append(asdict(compute))

    flat: dict[str, str] = {}
    for source in sources:
        for k, v in source.items():
            flat[str(k)] = str(v)

    return flat


# ── Inject / read ───────────────────────────────────────────────────────────────

def inject_metadata(table, metadata: dict[str, str]):
    """Stamp a PyArrow Table with custom key-value metadata.

    Merges *metadata* with any existing schema metadata so that
    PyArrow's own internal keys (e.g. pandas index info) are preserved.

    Parameters
    ----------
    table:
        A ``pyarrow.Table`` object.
    metadata:
        Flat string→string dict produced by build_metadata().

    Returns
    -------
    pyarrow.Table
        A new Table with updated schema metadata (PyArrow Tables are immutable).

    Example
    -------
    >>> import pyarrow as pa
    >>> table = pa.Table.from_pandas(df)
    >>> meta  = build_metadata(InstrumentMeta(), ProvenanceMeta())
    >>> table = inject_metadata(table, meta)
    """
    try:
        import pyarrow as pa  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError("pyarrow is required: pip install pyarrow") from exc

    existing = table.schema.metadata or {}
    # PyArrow expects bytes keys/values
    combined = {
        **existing,
        **{k.encode(): v.encode() for k, v in metadata.items()},
    }
    return table.replace_schema_metadata(combined)


def read_metadata(path: Path | str) -> dict[str, str]:
    """Read the custom key-value metadata from a Parquet file footer.

    Does **not** load any row data — only the file footer is read,
    making this very fast even for multi-GB files.

    Parameters
    ----------
    path:
        Path to a .parquet file.

    Returns
    -------
    dict[str, str]
        Decoded metadata dict.  Values that look like JSON objects/arrays
        are returned as-is strings (use json.loads() downstream if needed).

    Example
    -------
    >>> meta = read_metadata("ES_2023_01.parquet")
    >>> print(meta["symbol"], meta["tick_size"])
    ES 0.25
    """
    try:
        import pyarrow.parquet as pq  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError("pyarrow is required: pip install pyarrow") from exc

    file_meta = pq.read_metadata(str(path))
    raw: dict[bytes, bytes] = file_meta.metadata or {}

    decoded = {}
    for k, v in raw.items():
        try:
            decoded[k.decode()] = v.decode()
        except (UnicodeDecodeError, AttributeError):
            # Skip internal binary keys (e.g. Arrow schema serialisation)
            pass

    # Drop PyArrow's internal 'pandas' key to keep output clean
    decoded.pop("pandas", None)
    return decoded
