# dataset_pipeline

A composable, config-driven **Databento → Parquet** pipeline, with loaders and a RAM gatekeeper for **vectorized** grid sweeps.

- **Config-driven.** Instruments, date ranges, storage settings and memory thresholds all live in JSON. Switching from ES to NQ, or from 2023 to 2015–2025, needs no code change.
- **Composable.** Small config files are deep-merged in order; CLI `--set` flags and `${ENV_VAR}` placeholders override on top.
- **Sweep-ready storage.** Monthly Parquet files with `float32` prices, `uint32` volume, a UTC `DatetimeIndex`, and instrument/provenance metadata stamped in the file footer.
- **RAM-aware sweeps.** Estimate a grid's memory footprint before you run it, and split it into safe batches automatically.
- **Data included.** The repo ships ES 1-minute bars for 2015-01 → 2025-12 (132 monthly files, 3.83 M rows, ~62 MB), so you can try everything below without a Databento account.

**Contents:** [Try it](#try-it-no-api-key-needed) · [Install](#installation) · [Quick start](#quick-start) · [Examples](#examples) · [Config architecture](#config-architecture) · [Pipeline stages](#pipeline-stages) · [Data format](#data-format) · [Sweep loop](#sweep-loop-with-vectorbt-pro) · [Layout](#repository-layout) · [Config reference](#config-reference) · [API reference](#api-reference) · [Development](#development)

---

## Try it (no API key needed)

```bash
git clone https://github.com/iancaseydouglas/dataset_pipeline.git
cd dataset_pipeline
uv sync
```

```python
from dataset_pipeline import load_month_range

df = load_month_range("./data/parquet", "2023-01", "2023-03",
                      columns=["open", "high", "low", "close", "volume"])
print(df.shape)        # (87258, 5)
print(df.index.name)   # ts_event  (UTC DatetimeIndex)
```

---

## Installation

**Prerequisites:** Python 3.10+, [uv](https://docs.astral.sh/uv/) (recommended) or pip. Downloading new data also needs a [Databento](https://databento.com) account and API key.

**With uv**

```bash
git clone https://github.com/iancaseydouglas/dataset_pipeline.git
cd dataset_pipeline
uv sync                      # creates .venv and installs the package + dependencies
uv run dataset-pipeline --dry-run
```

**With pip**

```bash
git clone https://github.com/iancaseydouglas/dataset_pipeline.git
cd dataset_pipeline
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -e .
python -m dataset_pipeline --dry-run
```

The package is installed from `src/`, so `uv run -m dataset_pipeline`, `python -m dataset_pipeline` and the `dataset-pipeline` command are all equivalent.

**API key** (only needed for the download stage)

```bash
export DATABENTO_API_KEY=db-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Put it in your shell profile or a `.env` file (gitignored) to persist it. `--api-key` and `run_pipeline(api_key=...)` also work, but the environment variable is preferred.

**VectorBT PRO** is *not* installed by this package. The pipeline itself never imports it; only the [sweep loop](#sweep-loop-with-vectorbt-pro) does. Install it separately under your own VectorBT PRO license.

---

## Quick start

**1. Validate the config (no network, no files written)**

```bash
python -m dataset_pipeline --dry-run
```

```
[dry-run] Config resolved successfully. No files written.

── Pipeline Plan ──────────────────────────────────────────────────
  Stages        : download, convert, validate
  Symbol        : ES.c.0  (continuous)
  Period        : 2015-01-01 → 2025-12-31T23:59
  Schema        : ohlcv-1m
  Encoding      : dbn / zstd
  DBN output    : data/dbn
  Parquet output: ./data/parquet
  Price dtype   : float32
  RAM warn at   : 75% of total
──────────────────────────────────────────────────────────────────
```

**2. Check the Databento cost (network, nothing submitted)*