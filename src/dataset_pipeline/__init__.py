"""
dataset_pipeline — DataBento → Parquet pipeline for VectorBT PRO grid sweeps.

Entirely config-driven. The only required argument to run the full pipeline
is the path to a top-level JSON config file:

    from dataset_pipeline import run_pipeline
    run_pipeline("./dataset_pipeline.config.json")

Or from the command line:

    python -m dataset_pipeline [config_path] [--set key=value ...] [--dry-run]

Config system
-------------
    load_config(path, cli_overrides)   → resolved config dict
    deep_merge(base, override)         → pure deep merge
    set_dotted(d, "a.b.c", value)      → leaf override by dot-notation
    resolve_to_dataclasses(cfg)        → typed dataclass instances

Pipeline
--------
    run_pipeline(config_path, cli_overrides, stages, dry_run)

Modules
-------
    config      downloader    converter
    loader      memory        metadata    utils
"""

from .config import (
    load_config,
    deep_merge,
    set_dotted,
    get_dotted,
    resolve_to_dataclasses,
)
from .pipeline import run_pipeline
from .downloader import (
    DownloadConfig,
    estimate_cost,
    submit_job,
    wait_for_completion,
    download_files,
    run_download,
)
from .converter import (
    ConversionConfig,
    optimize_dtypes,
    dbn_to_parquet,
    dbn_to_csv,
    convert_directory,
)
from .loader import (
    load_parquet,
    load_month_range,
    stream_monthly,
    read_file_metadata,
)
from .memory import (
    MemoryConfig,
    estimate_ram_bytes,
    check_ram_gate,
    split_param_grid,
    batch_grid_generator,
)
from .metadata import (
    InstrumentMeta,
    ProvenanceMeta,
    StrategyMeta,
    ComputeMeta,
    build_metadata,
    inject_metadata,
    read_metadata,
)
from .utils import (
    list_dbn_files,
    list_parquet_files,
    month_range,
    format_bytes,
)

__all__ = [
    # pipeline
    "run_pipeline",
    # config
    "load_config", "deep_merge", "set_dotted", "get_dotted", "resolve_to_dataclasses",
    # configs
    "DownloadConfig", "ConversionConfig", "MemoryConfig",
    "InstrumentMeta", "ProvenanceMeta", "StrategyMeta", "ComputeMeta",
    # downloader
    "estimate_cost", "submit_job", "wait_for_completion", "download_files", "run_download",
    # converter
    "optimize_dtypes", "dbn_to_parquet", "dbn_to_csv", "convert_directory",
    # loader
    "load_parquet", "load_month_range", "stream_monthly", "read_file_metadata",
    # memory
    "estimate_ram_bytes", "check_ram_gate", "split_param_grid", "batch_grid_generator",
    # metadata
    "build_metadata", "inject_metadata", "read_metadata",
    # utils
    "list_dbn_files", "list_parquet_files", "month_range", "format_bytes",
]
