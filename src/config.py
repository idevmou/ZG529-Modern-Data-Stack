"""Paths, source definitions and tunables for the Bronze layer (pure Python)."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

SOURCE = "uci_online_retail"
API_SOURCE = "mock_popularity_api"
STAGING = DATA / "raw"          # batch files produced by prepare_batches.py (not yet delivered)
LANDING = DATA / "landing"      # batches "arrive" here; this is what gets ingested
API_LANDING = LANDING / API_SOURCE / "product_popularity"   # raw JSON pages fetched from the API
REJECTED = DATA / "rejected"    # files that failed ingestion, plus malformed lines
LOG_DIR = DATA / "logs"
LAKE = DATA / "lake" / "bronze"  # Bronze Delta tables

LOG_TABLE = LAKE / "ingestion_log"

MAX_ATTEMPTS = 3
RETRY_BASE_DELAY_SEC = 2
API_TIMEOUT_SEC = 5
API_PAGE_SIZE = 500
API_PATH = "/api/v1/products/popularity"

# Bronze keeps every source column as a string; typing happens in Silver.
SOURCES = {
    "transactions": {
        "kind": "csv", "source": SOURCE,
        "columns": ["InvoiceNo", "StockCode", "Description", "Quantity",
                    "InvoiceDate", "UnitPrice", "CustomerID", "Country"],
        "table": LAKE / "transactions",
    },
    "product_catalog": {
        "kind": "csv", "source": SOURCE,
        "columns": ["StockCode", "Description", "AvgUnitPrice", "FirstSeen"],
        "table": LAKE / "product_catalog",
    },
    "product_popularity": {
        "kind": "api", "source": API_SOURCE,
        "columns": ["stock_code", "popularity_score", "sentiment_score", "units_sold", "as_of"],
        "table": LAKE / "product_popularity",
    },
}
