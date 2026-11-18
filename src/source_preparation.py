"""Stable project entry point for preparing raw source batches.

The batch-generation implementation lives in prepare_batches.py. This module
provides the project-assigned source_preparation.py interface while keeping
the existing implementation in one place.
"""
from pathlib import Path

from prepare_batches import main as _prepare_batches


def prepare_sources(xlsx: Path, out: Path = Path("data/raw")) -> None:
    """Generate transaction batches and the product catalog from the source XLSX."""
    _prepare_batches(Path(xlsx), Path(out))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Prepare RecoMart source batches.")
    parser.add_argument("--xlsx", type=Path, required=True, help="Path to UCI Online Retail.xlsx")
    parser.add_argument("--out", type=Path, default=Path("data/raw"), help="Output staging directory")
    args = parser.parse_args()
    prepare_sources(args.xlsx, args.out)
