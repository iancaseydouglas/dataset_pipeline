"""
config.py — Config loading, deep-merge, and CLI-override engine.

Config resolution order (later sources override earlier ones):
  1. Built-in defaults (hardcoded in each module's dataclass)
  2. Config files listed under "configs" in the top-level file (in list order)
  3. Config files found in directories listed under "config_dirs" (alphabetical)
  4. CLI overrides passed as dot-notation strings  (e.g. "download.start=2024-01-01")

The merge is a true deep merge down to scalar leaves — dicts are merged
recursively; lists and scalars are replaced wholesale at the key level.

Public API
----------
load_config(path, cli_overrides)   → fully-resolved config dict
deep_merge(base, override)         → pure merge, no side effects
set_dotted(d, dotted_key, value)   → set a leaf by "a.b.c" notation
get_dotted(d, dotted_key)          → read a leaf by "a.b.c" notation
resolve_to_dataclasses(cfg)        → translate merged dict → typed dataclasses
"""

from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Pure merge primitives ───────────────────────────────────────────────────────

def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base*, returning a new dict.

    Rules:
    - If both values are dicts → recurse.
    - Otherwise → override wins (list, scalar, None all replace wholesale).
    - Keys present only in *base* are preserved.
    - Keys present only in *override* are added.

    Parameters
    ----------
    base:
        The starting dict (lower priority).
    override:
        The dict whose values win on conflict (higher priority).

    Returns
    -------
    dict
        New merged dict. Neither input is mutated.

    Example
    -------
    >>> deep_merge({"a": {"x": 1, "y": 2}}, {"a": {"y": 99, "z": 3}})
    {'a': {'x': 1, 'y': 99, 'z': 3}}
    """
    result = deepcopy(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = deep_merge(result[key], val)
        else:
            result[key] = deepcopy(val)
    return result


def set_dotted(d: dict, dotted_key: str, value: Any) -> dict:
    """Set a leaf value in *d* using dot-notation key, returning a new dict.

    Creates intermediate dicts as needed.

    Parameters
    ----------
    d:
        Target dict (not mutated).
    dotted_key:
        Dot-separated path, e.g. 'download.start' or 'instrument.tick_size'.
    value:
        Value to set at the leaf.  Strings that look like numbers, booleans,
        or JSON structures are auto-cast.

    Returns
    -------
    dict
        New dict with the leaf set.

    Example
    -------
    >>> set_dotted({}, "download.start", "2024-01-01")
    {'download': {'start': '2024-01-01'}}
    >>> set_dotted({}, "memory.bytes_per_value", "4")
    {'memory': {'bytes_per_value': 4}}
    """
    parts = dotted_key.split(".")
    patch: dict = {}
    node = patch
    for part in parts[:-1]:
        node[part] = {}
        node = node[part]
    node[parts[-1]] = _auto_cast(value)
    return deep_merge(d, patch)


def get_dotted(d: dict, dotted_key: str, default: Any = None) -> Any:
    """Read a leaf from *d* using dot-notation key.

    Parameters
    ----------
    d:
        Source dict.
    dotted_key:
        Dot-separated path, e.g. 'download.start'.
    default:
        Returned if any key in the path is missing.

    Returns
    -------
    Any
        The value at the leaf, or *default*.
    """
    node = d
    for part in dotted_key.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def _auto_cast(value: str) -> Any:
    """Attempt to cast a CLI string to int, float, bool, JSON, or leave as str."""
    if not isinstance(value, str):
        return value
    # Boolean
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    # JSON array / object
    if value.startswith(("[", "{")):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    # Int
    try:
        return int(value)
    except ValueError:
        pass
    # Float
    try:
        return float(value)
    except ValueError:
        pass
    return value


# ── File / directory loading ────────────────────────────────────────────────────

def load_json(path: Path | str) -> dict:
    """Load and parse a JSON file, returning an empty dict on empty file.

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    json.JSONDecodeError
        If the file is not valid JSON.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    return json.loads(text)


def _load_dir_partials(directory: Path | str) -> dict:
    """Deep-merge all JSON files in a directory in alphabetical order.

    Files are sorted by name before merging, so a numeric prefix gives
    explicit override ordering:
        00_base.json       ← lowest priority
        10_instrument.json
        20_strategy.json   ← highest priority in the directory

    Parameters
    ----------
    directory:
        Directory to scan.  Non-JSON files are silently skipped.

    Returns
    -------
    dict
        Merged result of all JSON files in the directory.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise NotADirectoryError(f"Config directory not found: {directory.resolve()}")

    files = sorted(directory.glob("*.json"))
    logger.debug("[config] Loading %d partial(s) from %s", len(files), directory)

    result: dict = {}
    for f in files:
        logger.debug("[config]   + %s", f.name)
        result = deep_merge(result, load_json(f))
    return result


# ── Top-level resolution ────────────────────────────────────────────────────────

_TOP_LEVEL_KEYS = {"configs", "config_dirs"}

def _is_top_level(d: dict) -> bool:
    """Return True if *d* looks like a top-level orchestrator config."""
    return bool(set(d.keys()) & _TOP_LEVEL_KEYS)


def resolve_config_chain(
    path: Path | str,
    *,
    _seen: set | None = None,
) -> dict:
    """Recursively resolve a config file or top-level orchestrator.

    If the loaded JSON contains 'configs' or 'config_dirs' keys, it is treated
    as a top-level orchestrator and its referenced files/dirs are loaded and
    merged in order.  The top-level file itself may also carry inline overrides,
    which are applied last (highest priority among file sources).

    Cycle detection is performed via the *_seen* set of resolved paths.

    Parameters
    ----------
    path:
        Path to a JSON config file.

    Returns
    -------
    dict
        Fully resolved merged config (file sources only; CLI overrides not applied).
    """
    path  = Path(path).resolve()
    _seen = _seen or set()

    if str(path) in _seen:
        logger.warning("[config] Cycle detected — skipping already-loaded: %s", path)
        return {}
    _seen.add(str(path))

    raw = load_json(path)

    if not _is_top_level(raw):
        # It's a plain partial — return as-is
        logger.debug("[config] Partial: %s", path.name)
        return raw

    logger.debug("[config] Top-level orchestrator: %s", path.name)
    merged: dict = {}

    # 1. Named config files (in list order)
    for ref in raw.get("configs", []):
        ref_path = (path.parent / ref).resolve()
        child = resolve_config_chain(ref_path, _seen=_seen)
        merged = deep_merge(merged, child)
        logger.debug("[config] Merged: %s", ref_path.name)

    # 2. Config directories (alphabetical file order within each dir)
    for ref_dir in raw.get("config_dirs", []):
        dir_path = (path.parent / ref_dir).resolve()
        child = _load_dir_partials(dir_path)
        merged = deep_merge(merged, child)
        logger.debug("[config] Merged dir: %s", dir_path.name)

    # 3. Inline keys in the top-level file itself (strip orchestrator keys)
    inline = {k: v for k, v in raw.items() if k not in _TOP_LEVEL_KEYS}
    if inline:
        merged = deep_merge(merged, inline)
        logger.debug("[config] Applied inline overrides from top-level file.")

    return merged


def load_config(
    path: Path | str = "./dataset_pipeline.config.json",
    cli_overrides: list[str] | None = None,
) -> dict:
    """Load, merge, and apply CLI overrides to produce the final config dict.

    This is the single public entrypoint for the config system.

    Parameters
    ----------
    path:
        Path to the top-level config file (or any partial).
        Defaults to './dataset_pipeline.config.json'.
    cli_overrides:
        Optional list of 'dot.key=value' strings from the command line.
        These are applied last and override all file-sourced values.
        Example: ['download.start=2024-01-01', 'instrument.symbol=NQ']

    Returns
    -------
    dict
        Fully resolved config dict, ready for resolve_to_dataclasses().

    Example
    -------
    >>> cfg = load_config("./dataset_pipeline.config.json",
    ...                   cli_overrides=["instrument.symbol=NQ"])
    >>> cfg["instrument"]["symbol"]
    'NQ'
    """
    config = resolve_config_chain(path)
    logger.info("[config] Loaded config chain from: %s", path)

    # Substitute environment variables in string values (${ENV_VAR} syntax)
    config = _expand_env_vars(config)

    if cli_overrides:
        for override in cli_overrides:
            if "=" not in override:
                raise ValueError(
                    f"CLI override must be in 'key.path=value' format, got: '{override}'"
                )
            dotted_key, _, value = override.partition("=")
            config = set_dotted(config, dotted_key.strip(), value.strip())
            logger.debug("[config] CLI override: %s = %r", dotted_key, _auto_cast(value))

    return config


def _expand_env_vars(obj: Any) -> Any:
    """Recursively substitute ${ENV_VAR} placeholders in string values."""
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(v) for v in obj]
    if isinstance(obj, str) and "${" in obj:
        return os.path.expandvars(obj)
    return obj


# ── Dataclass translation ───────────────────────────────────────────────────────

def resolve_to_dataclasses(cfg: dict) -> dict:
    """Translate the flat merged config dict into typed dataclass instances.

    Returns a dict of named dataclass instances ready to be passed directly
    to pipeline functions.  Missing keys fall back to dataclass defaults.

    Parameters
    ----------
    cfg:
        Merged config dict from load_config().

    Returns
    -------
    dict with keys:
        'download'   → DownloadConfig
        'conversion' → ConversionConfig
        'memory'     → MemoryConfig
        'instrument' → InstrumentMeta
        'provenance' → ProvenanceMeta
        'strategy'   → StrategyMeta
        'compute'    → ComputeMeta

    Example
    -------
    >>> dc = resolve_to_dataclasses(load_config("./dataset_pipeline.config.json"))
    >>> dc['download'].start
    '2015-01-01'
    """
    from .downloader import DownloadConfig          # noqa: PLC0415
    from .converter  import ConversionConfig        # noqa: PLC0415
    from .memory     import MemoryConfig            # noqa: PLC0415
    from .metadata   import (                       # noqa: PLC0415
        InstrumentMeta, ProvenanceMeta, StrategyMeta, ComputeMeta
    )
    from pathlib import Path                        # noqa: PLC0415

    def _pick(section: str, key: str, default: Any) -> Any:
        return get_dotted(cfg, f"{section}.{key}", default)

    inst = cfg.get("instrument", {})
    dl   = cfg.get("download", {})
    tx   = cfg.get("transform", {})
    dt   = tx.get("dtypes", {})
    pq   = tx.get("parquet", {})
    mem  = tx.get("memory", {})
    tr   = cfg.get("transport", {})
    bat  = tr.get("batch", {})
    prov = cfg.get("provenance", {})
    strat = cfg.get("strategy", {})
    comp  = cfg.get("compute", {})

    download_cfg = DownloadConfig(
        dataset        = dl.get("dataset",         inst.get("dataset",      "GLBX.MDP3")),
        symbol         = dl.get("symbol",          inst.get("contract",     "ES.c.0")),
        stype_in       = dl.get("stype_in",        inst.get("stype_in",     "continuous")),
        schema         = dl.get("schema",                                   "ohlcv-1m"),
        start          = dl.get("start",                                    "2015-01-01"),
        end            = dl.get("end",                                      "2025-12-31T23:59"),
        encoding       = dl.get("encoding",                                 "dbn"),
        compression    = dl.get("compression",                              "zstd"),
        split_duration = dl.get("split_duration",                           "month"),
        output_dir     = Path(dl.get("output_dir",                         "./data/dbn")),
        poll_interval_s= int(dl.get("poll_interval_s",                     10)),
    )

    conversion_cfg = ConversionConfig(
        price_cols         = dt.get("price_cols",      ["open", "high", "low", "close"]),
        volume_col         = dt.get("volume_col",      "volume"),
        timestamp_col      = dt.get("timestamp_col",   "ts_event"),
        price_dtype        = dt.get("price_dtype",     "float32"),
        volume_dtype       = dt.get("volume_dtype",    "uint32"),
        timestamp_dtype    = dt.get("timestamp_dtype", "int64"),
        parquet_compression= pq.get("compression",     "snappy"),
        fallback_csv       = bool(pq.get("fallback_csv", False)),
        row_group_size     = int(pq.get("row_group_size", 500_000)),
    )

    memory_cfg = MemoryConfig(
        warn_threshold    = float(bat.get("warn_threshold",    mem.get("warn_threshold",    0.75))),
        hard_threshold    = float(bat.get("hard_threshold",    mem.get("hard_threshold",    0.85))),
        bytes_per_value   = int(mem.get("bytes_per_value",   4)),
        n_columns         = int(mem.get("n_columns",         5)),
        safety_multiplier = float(mem.get("safety_multiplier", 1.5)),
    )

    instrument_meta = InstrumentMeta(
        symbol         = inst.get("symbol",        "ES"),
        contract_month = inst.get("contract_month",""),
        tick_size      = float(inst.get("tick_size",      0.25)),
        point_value    = float(inst.get("point_value",    50.0)),
        exchange       = inst.get("exchange",      "CME"),
        asset_class    = inst.get("asset_class",   "Futures"),
    )

    provenance_meta = ProvenanceMeta(
        data_provider   = prov.get("data_provider",  dl.get("data_provider", "databento")),
        dataset_schema  = prov.get("dataset_schema", dl.get("schema",        "ohlcv-1m")),
        timezone        = prov.get("timezone",                                "UTC"),
        is_adjusted     = bool(prov.get("is_adjusted",                       True)),
        aggregation     = prov.get("aggregation",    dl.get("schema",        "ohlcv-1m")),
        timestamp_unit  = prov.get("timestamp_unit",                         "ns"),
    )

    strategy_meta = StrategyMeta(
        strategy_name  = strat.get("strategy_name",  ""),
        parameter_grid = strat.get("parameter_grid", {}),
        vbt_version    = strat.get("vbt_version",    ""),
        split_id       = strat.get("split_id",       ""),
        creation_date  = strat.get("creation_date",  ""),
    )

    compute_meta = ComputeMeta(
        row_group_size  = comp.get("row_group_size", pq.get("row_group_size", "256MB")),
        sorted_by       = comp.get("sorted_by",      "timestamp"),
        is_sorted       = bool(comp.get("is_sorted", True)),
        price_dtype     = comp.get("price_dtype",    dt.get("price_dtype", "float32")),
        timestamp_dtype = comp.get("timestamp_dtype", dt.get("timestamp_dtype", "int64")),
    )

    return {
        "download":   download_cfg,
        "conversion": conversion_cfg,
        "memory":     memory_cfg,
        "instrument": instrument_meta,
        "provenance": provenance_meta,
        "strategy":   strategy_meta,
        "compute":    compute_meta,
    }
