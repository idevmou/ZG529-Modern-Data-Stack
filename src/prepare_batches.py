"""Split the UCI Online Retail dataset into incoming CSV batches for the Bronze demo.

Raw landing layout (source / type / period partition):
    <out>/uci_online_retail/transactions/dt=YYYY-MM/transactions_batch_NNN.csv
    <out>/uci_online_retail/product_catalog/dt=YYYY-MM-DD/product_catalog_batch_001.csv

Usage:
    python src/prepare_batches.py --xlsx "data/source/Online Retail.xlsx" --out data/raw
"""
import argparse
import csv
from pathlib import Path

import pandas as pd

SOURCE = "uci_online_retail"
REDELIVERY_ROWS = 1000
REDELIVERY_FROM_BATCH = 2


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def main(xlsx: Path, out: Path) -> None:
    # Read everything as text so the raw files stay faithful to the source.
    df = pd.read_excel(xlsx, dtype=str)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["CustomerID"] = df["CustomerID"].str.replace(r"\.0$", "", regex=True)
    df["period"] = df["InvoiceDate"].str[:7]

    base = out / SOURCE
    batches = {}
    for n, (period, part) in enumerate(df.groupby("period", sort=True), start=1):
        part = part.drop(columns="period")
        write_csv(part, base / "transactions" / f"dt={period}" / f"transactions_batch_{n:03d}.csv")
        batches[n] = (period, part)

    # Late re-delivery of rows that were already ingested: exercises duplicate prevention.
    n = len(batches) + 1
    period, source_part = batches[REDELIVERY_FROM_BATCH]
    write_csv(
        source_part.head(REDELIVERY_ROWS),
        base / "transactions" / f"dt={period}" / f"transactions_batch_{n:03d}_redelivery.csv",
    )

    # Second source: product catalog derived from the transactions.
    clean = df.assign(UnitPrice=pd.to_numeric(df["UnitPrice"], errors="coerce"))
    catalog = (
        clean.dropna(subset=["StockCode"])
        .groupby("StockCode")
        .agg(
            Description=("Description", lambda s: s.mode().iat[0] if s.notna().any() else None),
            AvgUnitPrice=("UnitPrice", "mean"),
            FirstSeen=("InvoiceDate", "min"),
        )
        .reset_index()
        .round({"AvgUnitPrice": 2})
    )
    write_csv(catalog, base / "product_catalog" / f"dt={df['InvoiceDate'].max()[:10]}" / "product_catalog_batch_001.csv")

    print(f"transactions: {len(df):,} rows in {len(batches)} monthly batches + 1 re-delivery batch")
    print(f"product_catalog: {len(catalog):,} products")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    args = ap.parse_args()
    main(args.xlsx, args.out)
