# dataset_pipeline

A config-driven DataBento → Parquet pipeline for VectorBT PRO grid sweeps on ES/NQ futures.

All pipeline behaviour — instruments, date ranges, storage settings, memory thresholds — is controlled entirely by JSON config files. No code changes are needed to switch instruments, environments, or date ranges. CLI overrides and environment variable substitution are supported for scripting and deployment.

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Config Architecture](#config-architecture)
4. [Examples](#examples)
5. [Directory Layout](#directory-layout)
6. [Pipeline Stages](#pipeline-stages)
7. [VBT Sweep Loop](#vbt-sweep-loop)
8. [Config Reference](#config-reference)
9. [API Reference](#api-reference)

---

## Installation

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- A [DataBento](https://databento.com) account and API key

### With uv (recommended)

```bash
gh repo clone iancaseydouglas/dataset_pipeline
cd dataset_pipeline
uv sync                  # creates .venv, installs all deps + the package
uv run -m dataset_pipeline --dry-run
```

`uv sync` reads `pyproject.toml`, resolves all dependencies, and installs the
package from `src/` into the local `.venv`. After this, `uv run` always uses
that venv regardless of what Python is active in your shell.

### With pip

```bash
git clone https://github.com/yourorg/dataset_pipeline.git
cd dataset_pipeline
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e .
python -m dataset_pipeline --dry-run
```

The `-e` (editable) flag means changes to `src/dataset_pipeline/` take effect
immediately without reinstalling.

### Set your API key

```bash
export DATABENTO_API_KEY=db-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Add this to your shell profile (`~/.zshrc`, `~/.bashrc`) or a `.env` file to
persist it. The key can also be passed explicitly to `run_pipeline(api_key=...)`
or via the `--api-key` CLI flag, but the environment variable is preferred for
scripts and automation.

---

## Quick Start

### 1. Check cost and validate config (no I/O)

```bash
python -m dataset_pipeline --dry-run
```

This resolves the full config chain, prints the pipeline plan, and queries DataBento for a cost estimate. No files are written and no job is submitted.

### 2. Run the full pipeline

```bash
python -m dataset_pipeline
```

Uses `./dataset_pipeline.config.json` by default. Downloads monthly DBN files, converts each to optimised Parquet (float32 prices, int64 timestamps, snappy compression), and validates the output.

### 3. Specify a config file

```bash
python -m dataset_pipeline ./my_project/pipeline.config.json
```

### 4. Override any config value at the command line

```bash
python -m dataset_pipeline \
  --set download.start=2023-01-01 \
  --set download.end=2023-12-31T23:59 \
  --set instrument.symbol=NQ \
  --set instrument.contract=NQ.c.0
```

CLI values are applied last and override all file-sourced values. Any dot-notation path into the config tree is valid.

### 5. Run specific stages only

```bash
# Skip download — re-convert existing DBN files
python -m dataset_pipeline --stages convert validate

# Validate only — check metadata on existing Parquet files
python -m dataset_pipeline --stages validate
```

### From Python

```python
from dataset_pipeline import run_pipeline

# Minimal — run everything with defaults
result = run_pipeline()

# Dry run
result = run_pipeline(dry_run=True)

# Override instrument without touching config files
result = run_pipeline(
    cli_overrides=[
        "instrument.symbol=NQ",
        "instrument.contract=NQ.c.0",
        "instrument.point_value=20.0",
    ]
)

# Convert and validate only (DBN files already downloaded)
result = run_pipeline(stages=["convert", "validate"])

# Result contains file lists and the resolved config
dbn_files     = result["dbn_files"]      # list[Path]
parquet_files = result["parquet_files"]  # list[Path]
cfg           = result["config"]         # resolved dict
dc            = result["dataclasses"]    # typed dataclass instances
```

---

## Config Architecture

The pipeline is driven by three functional config files that change at different rates, composed by a top-level orchestrator. The only file you need to edit day-to-day is the top-level one.

```
dataset_pipeline.config.json        ← top-level orchestrator
  └── configs/transform.json        ← how to store and optimise (changes per project)
  └── configs/transport.json        ← how to download (changes per environment)
  └── configs/instruments/ES.json   ← what to download (changes per job)
```

The three files map onto three real axes of change:

| File | Scope | Change when… |
|---|---|---|
| `instruments/ES.json` | Symbol, tick size, point value, exchange | Switching instruments or contracts |
| `transport.json` | Date range, encoding, compression, polling | Changing period or download settings |
| `transform.json` | dtypes, Parquet settings, memory thresholds | Tuning storage or performance |

### Deep merge

Config files are deep-merged in list order. Later entries override earlier ones at the leaf level; dicts are merged recursively; scalars and lists are replaced wholesale. This means partial files only need to specify the keys they want to override — everything else falls through from earlier files.

```
transform.json           sets all dtype and Parquet defaults
transport.json           overrides download-specific settings
instruments/ES.json      overrides instrument identity fields
CLI --set flags          override any individual leaf, applied last
```

### Directory of partials

Add `config_dirs` to the top-level config to compose variations without touching base files:

```json
{
  "configs": [
    "./configs/transform.json",
    "./configs/transport.json",
    "./configs/instruments/ES.json"
  ],
  "config_dirs": ["./configs/partials"]
}
```

Files in `config_dirs` are merged in **alphabetical order** within each directory. Use a numeric prefix to make override priority explicit and self-documenting:

```
configs/partials/
  10_date_2023_only.json     overrides download.start / end
  20_debug_csv.json          enables CSV fallback, sets debug output dir
```

Drop any partial into the directory and it is picked up on the next run — no code changes, no config file edits.

### Environment variable substitution

String values in any config file support `${ENV_VAR}` placeholders:

```json
{
  "download": {
    "output_dir": "${DATA_ROOT}/dbn"
  },
  "transform": {
    "parquet": {
      "output_dir": "${DATA_ROOT}/parquet"
    }
  }
}
```

---

## Examples

### Run ES 2015–2025, full pipeline

```bash
python -m dataset_pipeline
```

Uses all defaults as defined in the three base config files.

---

### Run NQ for 2023 only, no code changes

Create a top-level config that swaps the instrument and adds a date partial:

**`nq_2023.config.json`**
```json
{
  "configs": [
    "./configs/transform.json",
    "./configs/transport.json",
    "./configs/instruments/NQ.json"
  ],
  "config_dirs": ["./configs/partials"]
}
```

**`configs/partials/10_date_2023_only.json`**
```json
{
  "download": {
    "start": "2023-01-01",
    "end":   "2023-12-31T23:59"
  }
}
```

```bash
python -m dataset_pipeline ./nq_2023.config.json --dry-run
```

---

### Override a single value without editing any file

```bash
python -m dataset_pipeline --set download.end=2024-06-30T23:59
```

---

### Re-convert already-downloaded files with a different compression

```bash
python -m dataset_pipeline \
  --stages convert validate \
  --set transform.parquet.compression=zstd
```

This skips download, re-converts from existing DBN files, and validates the output.

---

### Enable CSV fallback for visual debugging

```bash
python -m dataset_pipeline \
  --set transform.parquet.fallback_csv=true \
  --set transform.parquet.output_dir=./data/debug
```

---

### Use the config system directly in Python

```python
from dataset_pipeline import load_config, resolve_to_dataclasses, deep_merge

# Load and inspect the resolved config
cfg = load_config("./dataset_pipeline.config.json")
dc  = resolve_to_dataclasses(cfg)

print(dc["download"].symbol)          # ES.c.0
print(dc["instrument"].tick_size)     # 0.25
print(dc["conversion"].price_dtype)   # float32
print(dc["memory"].warn_threshold)    # 0.75

# Programmatically merge an override dict (equivalent to --set flags)
override = {"download": {"start": "2024-01-01"}, "instrument": {"symbol": "NQ"}}
cfg2 = deep_merge(cfg, override)
```

---

### Check RAM before launching a sweep

```python
from dataset_pipeline import (
    estimate_ram_bytes, count_param_combinations,
    check_ram_gate, MemoryConfig, format_bytes
)

grid = {"ema_fast": [5, 10, 20], "ema_slow": [50, 100, 200], "atr_mult": [1.5, 2.0, 2.5]}
cfg  = MemoryConfig(bytes_per_value=4)

n   = count_param_combinations(grid)                  # 27
est = estimate_ram_bytes(n_rows=2_600_000, n_params=n, cfg=cfg)
print(f"Estimated: {format_bytes(est)}")

# Warn only (default)
check_ram_gate(2_600_000, n, cfg)

# Auto-batch: returns number of batches needed, or None if safe
n_batches = check_ram_gate(2_600_000, n, cfg, auto_batch=True)
```

---

## Directory Layout

```
dataset_pipeline/                      ← git repo root
│
├── pyproject.toml                     Package definition. Declares dependencies,
│                                       entry points, and tells the build backend
│                                       to look in src/ for the package source.
│                                       Run `uv sync` once after cloning.
│
├── .gitignore                         Ignores .venv/, data/, .env, caches.
│
├── dataset_pipeline.config.json       Default top-level config orchestrator.
│                                       Lists which config files and partial
│                                       directories to merge, and in what order.
│                                       Resolved relative to cwd at runtime.
│
├── src/
│   └── dataset_pipeline/              Python package — installed into .venv
│       │                               by `uv sync`. Import as:
│       │                               `from dataset_pipeline import ...`
│       │                               or run as:
│       │                               `uv run -m dataset_pipeline`
│       │
│       ├── __init__.py                Public API surface.
│       ├── __main__.py                CLI entry point.
│       ├── config.py                  Config engine: load_config, deep_merge,
│       │                               set_dotted, resolve_to_dataclasses.
│       ├── pipeline.py                run_pipeline — top-level orchestrator.
│       ├── downloader.py              DataBento batch API wrapper.
│       ├── converter.py               DBN → Parquet + dtype optimisation.
│       ├── loader.py                  load_parquet, load_month_range,
│       │                               stream_monthly.
│       ├── memory.py                  RAM estimation, gatekeeper, grid
│       │                               splitter, batch_grid_generator.
│       ├── metadata.py                Parquet metadata dataclasses and
│       │                               build / inject / read helpers.
│       └── utils.py                   month_range, format_bytes, file listing.
│
├── configs/
│   ├── instruments/
│   │   ├── ES.json                    E-mini S&P 500 — symbol, tick size,
│   │   │                               point value, exchange, dataset ID.
│   │   ├── NQ.json                    E-mini NASDAQ-100 — same structure,
│   │   │                               different values. Swap into top-level
│   │   │                               config to change instrument.
│   │   └── RTY.json                   (add your own as needed)
│   │
│   ├── transport.json                 DataBento API settings — date range,
│   │                                   schema, encoding, compression, output
│   │                                   directory, polling interval.
│   │
│   ├── transform.json                 Data transformation settings — dtype
│   │                                   mapping, Parquet compression, row group
│   │                                   size, output directory, RAM thresholds.
│   │
│   └── partials/
│       ├── 10_date_2023_only.json     Restricts download.start / end to 2023.
│       ├── 20_debug_csv.json          Enables CSV fallback + debug output dir.
│       └── 30_zstd_parquet.json       Overrides Parquet compression to zstd.
│
├── data/                              Gitignored. Created on first run.
│   ├── dbn/                           Raw .dbn.zst files from DataBento.
│   │   └── ES_2023_01.dbn.zst         One file per calendar month.
│   │
│   └── parquet/                       Converted, optimised Parquet files.
│       └── ES_2023_01.parquet         float32 prices, int64 timestamps,
│                                       schema metadata in file footer.
│
├── notebooks/
│   └── sweep_analysis.ipynb           Example VBT sweep using stream_monthly
│                                       and batch_grid_generator.
│
└── results/
    └── ...                            Sweep stats, exported portfolios, charts.
```

### Why `src/` layout?

Without `src/`, Python can accidentally import the local `dataset_pipeline/`
folder directly from the working directory instead of the installed package in
`.venv/`. This causes subtle bugs — edits to source files appear to take effect
without reinstalling, import order changes depending on cwd, and the package
behaves differently when installed by a user elsewhere. The `src/` layout makes
the installed package the *only* importable copy.

### Config composition patterns

**Switch instrument** — change one line in the top-level `configs` list:
```json
"./configs/instruments/NQ.json"     ← was ES.json
```

**Run a date sub-range** — add a partial to `config_dirs`, no base file edits:
```json
"config_dirs": ["./configs/partials"]
```

**Multiple environments** — keep separate top-level configs:
```
pipeline_dev.config.json     points to debug partials, local data dirs
pipeline_prod.config.json    full date range, production output dirs
```
Both reference the same base instrument and transform files. Only the differences are specified in each.

---

## Pipeline Stages

| Stage | What it does |
|---|---|
| `download` | Estimates cost, submits DataBento batch job, polls until complete, downloads DBN files |
| `convert` | Reads each DBN file, optimises dtypes, injects Parquet metadata, writes `.parquet` |
| `validate` | Opens each Parquet file footer, logs symbol + schema metadata, reports pass/fail count |

Run all stages (default):
```bash
python -m dataset_pipeline
```

Resume from a given stage:
```bash
python -m dataset_pipeline --stages convert validate
python -m dataset_pipeline --stages validate
```

---

## VBT Sweep Loop

`batch_grid_generator` wraps the full memory gatekeeper. It estimates the sweep footprint, and either yields the whole grid at once (if it fits) or splits the Cartesian product into safe-sized chunks with a `gc.collect()` call between each.

```python
from dataset_pipeline import stream_monthly, batch_grid_generator

grid = {"ema_fast": [5, 10, 20], "ema_slow": [50, 100, 200]}

for month, df in stream_monthly("./data/parquet"):
    for sub_grid in batch_grid_generator(grid, n_rows=len(df)):
        portfolio = vbt.Portfolio.from_signals(df["close"], **sub_grid)
        results.append(portfolio.stats())
    del df   # explicit release before loading next month
```

**RAM reference:** 1,000 combinations × 10 years of 1-min ES data at float32 ≈ **73 GB**. The gatekeeper will auto-batch this on any typical workstation without intervention.

---

## Config Reference

### `instrument` keys

| Key | Type | Default | Description |
|---|---|---|---|
| `symbol` | string | `"ES"` | Root symbol for metadata tagging |
| `contract` | string | `"ES.c.0"` | DataBento continuous contract identifier |
| `stype_in` | string | `"continuous"` | Symbology type |
| `dataset` | string | `"GLBX.MDP3"` | DataBento dataset (CME Globex MDP 3.0) |
| `tick_size` | float | `0.25` | Minimum price increment |
| `point_value` | float | `50.0` | Dollar value per full point (ES = $50/pt) |
| `exchange` | string | `"CME"` | Primary listing exchange |
| `asset_class` | string | `"Futures"` | Asset class for VBT fee model routing |

### `download` keys

| Key | Type | Default | Description |
|---|---|---|---|
| `schema` | string | `"ohlcv-1m"` | Bar type. Also: `ohlcv-5m`, `ohlcv-1d`, `trades`, `mbo` |
| `start` | string | `"2015-01-01"` | Inclusive start date (ISO 8601) |
| `end` | string | `"2025-12-31T23:59"` | Inclusive end date (ISO 8601) |
| `encoding` | string | `"dbn"` | Wire format. `dbn` preferred over `csv` |
| `compression` | string | `"zstd"` | File compression. `zstd` recommended |
| `split_duration` | string | `"month"` | File granularity. Also: `day`, `week`, `year` |
| `poll_interval_s` | int | `10` | Seconds between batch job status polls |
| `output_dir` | string | `"./data/dbn"` | Where downloaded DBN files are saved |

### `transform.dtypes` keys

| Key | Type | Default | Description |
|---|---|---|---|
| `price_cols` | list | `["open","high","low","close"]` | Columns cast to `price_dtype` |
| `price_dtype` | string | `"float32"` | 4 bytes/value — sufficient for ES $0.25 tick precision |
| `volume_dtype` | string | `"uint32"` | Non-negative volume; saves space vs int64 |
| `timestamp_dtype` | string | `"int64"` | Unix nanoseconds — zero parsing cost vs ISO strings |
| `timestamp_col` | string | `"ts_event"` | Column name for the primary timestamp |

### `transform.parquet` keys

| Key | Type | Default | Description |
|---|---|---|---|
| `compression` | string | `"snappy"` | Codec. `snappy` = best speed/ratio for columnar numeric data |
| `row_group_size` | int | `500000` | Rows per row group — controls predicate pushdown granularity |
| `output_dir` | string | `"./data/parquet"` | Where Parquet files are written |
| `fallback_csv` | bool | `false` | Also write a `.csv` alongside each Parquet file |

### `transport.batch` keys

| Key | Type | Default | Description |
|---|---|---|---|
| `warn_threshold` | float | `0.75` | Fraction of total RAM at which a warning is emitted |
| `hard_threshold` | float | `0.85` | Fraction at which batching is forced (or error raised) |
| `gc_between_batches` | bool | `true` | Call `gc.collect()` between sweep batches |

---

## API Reference

### `run_pipeline`
```python
run_pipeline(
    config_path="./dataset_pipeline.config.json",
    cli_overrides=None,    # list of "dot.key=value" strings
    stages=None,           # subset of ["download", "convert", "validate"]
    dry_run=False,
    api_key=None,
) -> dict                  # {config, dataclasses, dbn_files, parquet_files}
```

### `load_config`
```python
load_config(
    path="./dataset_pipeline.config.json",
    cli_overrides=None,    # list of "dot.key=value" strings
) -> dict                  # fully resolved config
```

### `stream_monthly`
```python
stream_monthly(
    data_dir,
    columns=None,          # load only these columns
    filters=None,          # PyArrow predicate pushdown filters
    start_month=None,      # "YYYY-MM" range filter
    end_month=None,
) -> Generator[(stem, DataFrame)]
```

### `batch_grid_generator`
```python
batch_grid_generator(
    grid,                  # {"param": [values, ...], ...}
    n_rows,                # rows in current dataset slice
    cfg=None,              # MemoryConfig
    gc_between_batches=True,
) -> Generator[dict]       # yields sub-grid dicts safe to pass to VBT
```

### `deep_merge`
```python
deep_merge(base, override) -> dict   # pure, neither input mutated
```

### `set_dotted`
```python
set_dotted(d, "a.b.c", value) -> dict   # returns new dict with leaf set
```
