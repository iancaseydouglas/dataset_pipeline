"""
pipeline.py — Top-level orchestrator for the dataset pipeline.

The entire pipeline — download, convert, validate — is driven by a single
config file path.  All behaviour is determined by config; no other arguments
are required.

Entry point
-----------
    run_pipeline(config_path, cli_overrides, stages, dry_run)

Stages
------
The pipeline is split into named stages so callers can run partial workflows:
  'download'  — fetch DBN files from DataBento
  'convert'   — DBN → Parquet (+ optional CSV fallback)
  'validate'  — verify Parquet files and print a metadata summary

Stages are run in order.  Passing stages=['convert'] skips download and
operates on whatever DBN files already exist in the configured input directory.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_ALL_STAGES = ("download", "convert", "validate")


def run_pipeline(
    config_path: str | Path = "./dataset_pipeline.config.json",
    cli_overrides: Optional[list[str]] = None,
    stages: Optional[list[str]] = None,
    *,
    dry_run: bool = False,
    api_key: Optional[str] = None,
) -> dict:
    """Run the full (or partial) dataset pipeline from a single config file.

    Parameters
    ----------
    config_path:
        Path to the top-level JSON config file.
        Defaults to './dataset_pipeline.config.json'.
        The file may reference other config files and/or directories of
        partial configs — see config.py for the full resolution rules.
    cli_overrides:
        Optional list of 'dot.key=value' strings that override any value in
        the resolved config.  Example: ['download.start=2024-01-01',
        'instrument.symbol=NQ', 'transform.parquet.compression=zstd']
    stages:
        List of pipeline stages to execute.  Defaults to all stages in order:
        ['download', 'convert', 'validate'].
        Pass a subset to resume from a given point, e.g. ['convert', 'validate']
        to re-convert already-downloaded DBN files without re-downloading.
    dry_run:
        If True, resolves config and prints a plan but makes no API calls
        and writes no files.
    api_key:
        DataBento API key.  If omitted, falls back to the DATABENTO_API_KEY
        environment variable.

    Returns
    -------
    dict with keys:
        'config'        → fully resolved config dict
        'dataclasses'   → typed dataclass instances (see config.resolve_to_dataclasses)
        'dbn_files'     → list[Path] of downloaded DBN files (or [] if stage skipped)
        'parquet_files' → list[Path] of written Parquet files (or [] if stage skipped)

    Example
    -------
    Minimal — run everything with defaults:
    >>> from dataset_pipeline import run_pipeline
    >>> result = run_pipeline()

    Dry run first:
    >>> result = run_pipeline(dry_run=True)

    Override instrument at call-time (no config edit needed):
    >>> result = run_pipeline(cli_overrides=["instrument.symbol=NQ",
    ...                                      "instrument.contract=NQ.c.0",
    ...                                      "instrument.tick_size=0.25"])

    Convert only (skip download):
    >>> result = run_pipeline(stages=["convert", "validate"])
    """
    from .config     import load_config, resolve_to_dataclasses  # noqa: PLC0415
    from .downloader import run_download                          # noqa: PLC0415
    from .converter  import convert_directory                     # noqa: PLC0415
    from .metadata   import build_metadata                        # noqa: PLC0415
    from .utils      import list_dbn_files, list_parquet_files, format_bytes  # noqa: PLC0415
    from .memory     import estimate_ram_bytes, count_param_combinations  # noqa: PLC0415

    stages = stages or list(_ALL_STAGES)
    _validate_stages(stages)

    # ── 1. Resolve config ─────────────────────────────────────────────────────
    cfg = load_config(config_path, cli_overrides)
    dc  = resolve_to_dataclasses(cfg)

    _log_config_summary(cfg, dc)

    if dry_run:
        logger.info("[pipeline] dry_run=True — exiting before any I/O.")
        print("[dry-run] Config resolved successfully. No files written.")
        _print_plan(cfg, dc, stages)
        return {"config": cfg, "dataclasses": dc, "dbn_files": [], "parquet_files": []}

    dbn_files:     list[Path] = []
    parquet_files: list[Path] = []

    # ── 2. Download stage ─────────────────────────────────────────────────────
    if "download" in stages:
        logger.info("[pipeline] Stage: download")
        dbn_files = run_download(dc["download"], api_key=api_key)
    else:
        logger.info("[pipeline] Skipping download — scanning existing DBN files.")
        dbn_files = list_dbn_files(dc["download"].output_dir)
        logger.info("[pipeline] Found %d existing DBN file(s).", len(dbn_files))

    # ── 3. Convert stage ──────────────────────────────────────────────────────
    if "convert" in stages:
        logger.info("[pipeline] Stage: convert")

        parquet_dir = Path(cfg.get("transform", {}).get("parquet", {}).get(
            "output_dir", "./data/parquet"
        ))

        shared_meta = build_metadata(
            dc["instrument"],
            dc["provenance"],
            dc["strategy"],
            dc["compute"],
        )

        parquet_files = convert_directory(
            input_dir  = dc["download"].output_dir,
            output_dir = parquet_dir,
            cfg        = dc["conversion"],
            metadata   = shared_meta,
        )
    else:
        logger.info("[pipeline] Skipping convert — scanning existing Parquet files.")
        parquet_dir   = Path(cfg.get("transform", {}).get("parquet", {}).get(
            "output_dir", "./data/parquet"
        ))
        parquet_files = list_parquet_files(parquet_dir)
        logger.info("[pipeline] Found %d existing Parquet file(s).", len(parquet_files))

    # ── 4. Validate stage ─────────────────────────────────────────────────────
    if "validate" in stages:
        logger.info("[pipeline] Stage: validate")
        _validate_parquet_files(parquet_files)

    logger.info(
        "[pipeline] Complete. %d DBN files, %d Parquet files.",
        len(dbn_files), len(parquet_files),
    )

    return {
        "config":        cfg,
        "dataclasses":   dc,
        "dbn_files":     dbn_files,
        "parquet_files": parquet_files,
    }


# ── Internal helpers ────────────────────────────────────────────────────────────

def _validate_stages(stages: list[str]) -> None:
    unknown = set(stages) - set(_ALL_STAGES)
    if unknown:
        raise ValueError(
            f"Unknown stage(s): {unknown}. "
            f"Valid stages: {list(_ALL_STAGES)}"
        )


def _log_config_summary(cfg: dict, dc: dict) -> None:
    from .utils import format_bytes  # noqa: PLC0415
    dl = dc["download"]
    logger.info(
        "[pipeline] Symbol=%s  Schema=%s  Period=%s → %s  Encoding=%s/%s",
        dl.symbol, dl.schema, dl.start, dl.end, dl.encoding, dl.compression,
    )


def _print_plan(cfg: dict, dc: dict, stages: list[str]) -> None:
    dl  = dc["download"]
    tx  = cfg.get("transform", {})
    pq  = tx.get("parquet", {})
    mem = dc["memory"]

    print("\n── Pipeline Plan ──────────────────────────────────────────────────")
    print(f"  Stages        : {', '.join(stages)}")
    print(f"  Symbol        : {dl.symbol}  ({dl.stype_in})")
    print(f"  Period        : {dl.start} → {dl.end}")
    print(f"  Schema        : {dl.schema}")
    print(f"  Encoding      : {dl.encoding} / {dl.compression}")
    print(f"  DBN output    : {dl.output_dir}")
    print(f"  Parquet output: {pq.get('output_dir', './data/parquet')}")
    print(f"  Price dtype   : {dc['conversion'].price_dtype}")
    print(f"  RAM warn at   : {mem.warn_threshold:.0%} of total")
    print("──────────────────────────────────────────────────────────────────\n")


def _validate_parquet_files(parquet_files: list[Path]) -> None:
    """Quick validation: check each Parquet file can be opened and has metadata."""
    from .metadata import read_metadata  # noqa: PLC0415

    if not parquet_files:
        logger.warning("[validate] No Parquet files to validate.")
        return

    ok = 0
    for f in parquet_files:
        try:
            meta = read_metadata(f)
            symbol = meta.get("symbol", "?")
            schema = meta.get("dataset_schema", "?")
            logger.info("[validate] ✓ %s  symbol=%s  schema=%s", f.name, symbol, schema)
            ok += 1
        except Exception as exc:
            logger.error("[validate] ✗ %s — %s", f.name, exc)

    logger.info("[validate] %d/%d files passed validation.", ok, len(parquet_files))
