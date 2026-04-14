"""
__main__.py — CLI entrypoint for dataset_pipeline.

Usage
-----
    # Run with default config path
    python -m dataset_pipeline

    # Specify a config file
    python -m dataset_pipeline ./my_pipeline.config.json

    # Override any config value with dot-notation
    python -m dataset_pipeline --set download.start=2024-01-01 --set instrument.symbol=NQ

    # Dry run (resolve config and print plan, no I/O)
    python -m dataset_pipeline --dry-run

    # Run specific stages only
    python -m dataset_pipeline --stages convert validate

    # Combine all of the above
    python -m dataset_pipeline ./configs/pipeline.config.json \\
        --stages download convert \\
        --set download.start=2023-01-01 \\
        --set instrument.symbol=NQ \\
        --dry-run
"""

import argparse
import logging
import sys

DEFAULT_CONFIG = "./dataset_pipeline.config.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dataset_pipeline",
        description="DataBento → Parquet pipeline driven entirely by JSON config files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "config",
        nargs="?",
        default=DEFAULT_CONFIG,
        help=(
            "Path to the top-level JSON config file. "
            f"Default: {DEFAULT_CONFIG}"
        ),
    )
    parser.add_argument(
        "--set",
        metavar="KEY=VALUE",
        dest="overrides",
        action="append",
        default=[],
        help=(
            "Override any config value using dot-notation. "
            "May be specified multiple times. "
            "Example: --set download.start=2024-01-01 --set instrument.symbol=NQ"
        ),
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["download", "convert", "validate"],
        default=None,
        help=(
            "Pipeline stages to run (space-separated). "
            "Default: all stages in order. "
            "Example: --stages convert validate"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Resolve config and print plan without making API calls or writing files.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help=(
            "DataBento API key. Overrides the DATABENTO_API_KEY environment variable. "
            "Prefer the environment variable in production."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity. Default: INFO.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args   = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        from .pipeline import run_pipeline  # noqa: PLC0415
        run_pipeline(
            config_path   = args.config,
            cli_overrides = args.overrides or None,
            stages        = args.stages,
            dry_run       = args.dry_run,
            api_key       = args.api_key,
        )
        return 0
    except FileNotFoundError as exc:
        print(f"\n[error] Config file not found: {exc}", file=sys.stderr)
        print("  Check the path or create the file from the provided examples.", file=sys.stderr)
        return 2
    except MemoryError as exc:
        print(f"\n[error] Memory gate: {exc}", file=sys.stderr)
        print("  Reduce parameter grid size or add --set memory.hard_threshold=0.95", file=sys.stderr)
        return 3
    except Exception as exc:
        logging.exception("[error] Pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
