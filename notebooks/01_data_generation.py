"""01 - Source data preparation.

Run from the repository root:
    python notebooks/01_data_generation.py --xlsx "data/source/Online Retail.xlsx"

This step creates staged CSV batches under data/raw. It does not load data into
Bronze; delivery and ingestion are handled by notebook 02.
"""
import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from source_preparation import prepare_sources


def main(xlsx: Path, out: Path) -> None:
    xlsx = xlsx.expanduser().resolve()
    out = out.expanduser().resolve()

    if not xlsx.is_file():
        raise FileNotFoundError(
            f"Source workbook not found: {xlsx}\n"
            "Download the UCI Online Retail workbook and pass its path with --xlsx."
        )

    print(f"Source workbook: {xlsx}")
    print(f"Staging output:  {out}")
    prepare_sources(xlsx, out)

    transaction_batches = sorted(
        (out / "uci_online_retail" / "transactions").glob("**/transactions_batch_*.csv")
    )
    catalog_batches = sorted(
        (out / "uci_online_retail" / "product_catalog").glob("**/product_catalog_batch_*.csv")
    )
    print("\nPreparation summary")
    print(f"Transaction batch files (including redelivery): {len(transaction_batches)}")
    print(f"Product catalog files: {len(catalog_batches)}")
    print("Source preparation completed. Next: run notebooks/02_bronze_ingestion.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RecoMart source data generation.")
    parser.add_argument(
        "--xlsx",
        type=Path,
        default=Path("data/source/Online Retail.xlsx"),
        help="Path to the UCI Online Retail XLSX workbook.",
    )
    parser.add_argument("--out", type=Path, default=Path("data/raw"), help="Staging output directory.")
    args = parser.parse_args()
    main(args.xlsx, args.out)
