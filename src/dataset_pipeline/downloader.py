"""
downloader.py — DataBento batch download pipeline.

Wraps the DataBento Historical batch API:
  estimate_cost → submit_job → wait_for_completion → download_files

All behaviour is driven by DownloadConfig.  The top-level run_download()
function composes the four primitives for the common case.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class DownloadConfig:
    """All parameters that control what DataBento returns and how it's saved.

    Attributes
    ----------
    dataset:
        DataBento dataset identifier. Default is CME Globex MDP 3.0.
    symbol:
        Instrument symbol. 'ES.c.0' is the continuous front-month E-mini S&P 500.
    stype_in:
        Symbology type. Use 'continuous' for roll-adjusted continuous contracts.
    schema:
        Data schema / bar type. 'ohlcv-1m' = 1-minute OHLCV bars.
        Other common values: 'ohlcv-5m', 'ohlcv-1d', 'trades', 'mbo', 'mbp-1'.
    start:
        Inclusive start date/datetime string (ISO 8601).
    end:
        Inclusive end date/datetime string (ISO 8601).
    encoding:
        Wire format. 'dbn' (DataBento Binary) is recommended — smallest files,
        no floating-point rounding risk. 'csv' available as fallback.
    compression:
        'zstd' is strongly preferred — best ratio, fast decompression.
        'none' supported but not recommended for large downloads.
    split_duration:
        Granularity of file splitting. 'month' aligns naturally with backtesting
        workflow and keeps per-file RAM usage predictable.
        Other values: 'day', 'week', 'year', 'none'.
    output_dir:
        Local directory where downloaded files will be saved.
    poll_interval_s:
        Seconds between job-status polling requests while waiting for completion.
    """
    dataset: str         = "GLBX.MDP3"
    symbol: str          = "ES.c.0"
    stype_in: str        = "continuous"
    schema: str          = "ohlcv-1m"
    start: str           = "2015-01-01"
    end: str             = "2025-12-31T23:59"
    encoding: str        = "dbn"
    compression: str     = "zstd"
    split_duration: str  = "month"
    output_dir: Path     = field(default_factory=lambda: Path("./data"))
    poll_interval_s: int = 10

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir)


# ── Client ──────────────────────────────────────────────────────────────────────

def get_client(api_key: Optional[str] = None):
    """Return an authenticated DataBento Historical client.

    Looks up the key from (in order):
      1. The ``api_key`` argument if provided.
      2. The ``DATABENTO_API_KEY`` environment variable.

    Raises
    ------
    EnvironmentError
        If no key is found.
    ImportError
        If the ``databento`` package is not installed.
    """
    try:
        import databento as db  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "The 'databento' package is required. Install with: pip install databento"
        ) from exc

    key = api_key or os.environ.get("DATABENTO_API_KEY")
    if not key:
        raise EnvironmentError(
            "No DataBento API key found.\n"
            "Pass api_key= directly, or set the DATABENTO_API_KEY environment variable:\n"
            "  export DATABENTO_API_KEY=db-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        )
    return db.Historical(key)


# ── Pipeline steps ──────────────────────────────────────────────────────────────

def estimate_cost(client, cfg: DownloadConfig) -> float:
    """Query DataBento for the cost estimate of a job described by *cfg*.

    Note: get_cost only accepts dataset, start, end, symbols, schema,
    stype_in, and limit — encoding/compression/split_duration are batch-only
    parameters and are not passed here.

    Returns
    -------
    float
        Estimated cost in USD.  Does **not** submit a job.
    """
    cost: float = client.metadata.get_cost(
        dataset=cfg.dataset,
        symbols=cfg.symbol,
        stype_in=cfg.stype_in,
        schema=cfg.schema,
        start=cfg.start,
        end=cfg.end,
    )
    logger.info("[cost estimate] $%.4f USD", cost)
    return cost


def submit_job(client, cfg: DownloadConfig):
    """Submit a batch job to DataBento and return the job object.

    The job is asynchronous — this function returns immediately after
    submission. Use wait_for_completion() to block until the files are ready.

    Returns
    -------
    databento.BatchJob
    """
    logger.info("[submit] Submitting batch job for %s %s %s→%s …",
                cfg.symbol, cfg.schema, cfg.start, cfg.end)
    job = client.batch.submit_job(
        dataset=cfg.dataset,
        symbols=cfg.symbol,
        stype_in=cfg.stype_in,
        schema=cfg.schema,
        start=cfg.start,
        end=cfg.end,
        encoding=cfg.encoding,
        compression=cfg.compression,
        split_duration=cfg.split_duration,
    )
    logger.info("[submit] Job ID: %s  State: %s", job["id"], job["state"])
    return job


def wait_for_completion(client, job_id: str, poll_interval_s: int = 10):
    """Block until the DataBento batch job reaches a terminal state.

    Parameters
    ----------
    client:
        Authenticated DataBento Historical client.
    job_id:
        The ``id`` field returned by submit_job().
    poll_interval_s:
        Seconds to sleep between polling requests. Lower values increase
        API call count; 10 s is a reasonable default for large jobs.

    Returns
    -------
    str
        The completed job_id (pass directly to download_files).

    Raises
    ------
    RuntimeError
        If the job ends in 'expired' or 'failed' state.
    """
    logger.info("[poll] Waiting for job %s …", job_id)
    while True:
        # Poll done bucket first — cheapest check
        done_ids = [j["id"] for j in client.batch.list_jobs("done")]
        if job_id in done_ids:
            logger.info("[poll] State: done")
            return job_id  # callers only need the id to download

        # Check for terminal failure states
        failed_jobs = [j for j in client.batch.list_jobs("expired") if j["id"] == job_id]
        if failed_jobs:
            state = failed_jobs[0]["state"]
            raise RuntimeError(
                f"Job {job_id} ended in non-success state: '{state}'. "
                "Check the DataBento dashboard for details."
            )

        # Still queued or processing
        active = [j for j in client.batch.list_jobs("queued,processing") if j["id"] == job_id]
        state = active[0]["state"] if active else "unknown"
        logger.info("[poll] State: %s", state)
        time.sleep(poll_interval_s)


def download_files(client, job_id: str, output_dir: Path) -> list[Path]:
    """Download all files for a completed batch job to *output_dir*.

    Uses the list[Path] returned directly by client.batch.download rather
    than globbing — consistent with the official Databento example.

    Note: the library places files under {output_dir}/{job_id}/ automatically.

    Parameters
    ----------
    client:
        Authenticated DataBento Historical client.
    job_id:
        The completed job ID.
    output_dir:
        Parent destination directory. The library creates a {job_id}/
        subdirectory inside it automatically.

    Returns
    -------
    list[Path]
        Sorted list of downloaded file paths as returned by the API client.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("[download] Saving files to: %s", output_dir.resolve())
    downloaded_files: list[Path] = client.batch.download(
        job_id=job_id,
        output_dir=output_dir,
    )

    files = sorted(downloaded_files)
    logger.info("[download] %d file(s) downloaded.", len(files))
    for f in files:
        size_mb = f.stat().st_size / 1_048_576
        logger.info("  %s  (%.1f MB)", f.name, size_mb)

    return files

    return files


# ── Top-level orchestrator ──────────────────────────────────────────────────────

def run_download(
    cfg: DownloadConfig,
    *,
    api_key: Optional[str] = None,
    dry_run: bool = False,
    resume_job_id: Optional[str] = None,
) -> list[Path]:
    """Full download pipeline: estimate → submit → poll → download.

    Parameters
    ----------
    cfg:
        DownloadConfig controlling what data to request and where to save it.
    api_key:
        Optional API key override (falls back to DATABENTO_API_KEY env var).
    dry_run:
        If True, only prints the cost estimate and returns an empty list.
    resume_job_id:
        If provided, skips submission and re-downloads an already-completed job.

    Returns
    -------
    list[Path]
        Downloaded file paths, or empty list on dry run.

    Example
    -------
    >>> from es_pipeline import DownloadConfig, run_download
    >>> cfg = DownloadConfig(start="2023-01-01", end="2023-12-31")
    >>> files = run_download(cfg, dry_run=True)  # check cost first
    >>> files = run_download(cfg)                # real run
    """
    client = get_client(api_key)

    cost = estimate_cost(client, cfg)
    print(f"[cost estimate] ${cost:.4f} USD")

    if dry_run:
        print("[dry-run] Exiting without submitting.")
        return []

    if resume_job_id:
        logger.info("[mode] Re-downloading existing job: %s", resume_job_id)
        return download_files(client, resume_job_id, cfg.output_dir)

    job = submit_job(client, cfg)
    completed_job_id = wait_for_completion(client, job["id"], cfg.poll_interval_s)
    files = download_files(client, completed_job_id, cfg.output_dir)
    logger.info("[done] All files downloaded successfully.")
    return files
