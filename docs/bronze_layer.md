# Bronze Layer – Ingestion and Raw Storage

*Owner: Member 2 · Code: [src/ingestion.py](../src/ingestion.py), [src/config.py](../src/config.py) · Demo: [notebooks/02_bronze_ingestion.py](../notebooks/02_bronze_ingestion.py)*

## 1. Purpose
The Bronze layer lands source data exactly as received, adds ingestion metadata, and guarantees that no source record is stored twice. Cleaning and typing are deferred to Silver.

**Stack:** pure Python 3 – `pyarrow` (CSV reading), `deltalake` (Delta Lake tables without Spark/Java), `hashlib`, `logging`. No Spark, Java or cloud service is required.

## 2. Sources and raw storage layout

| Source | Type | Origin | Ingestion mode | Rows |
|---|---|---|---|---|
| `uci_online_retail` | `transactions` | UCI Online Retail (id 352), split into 13 monthly CSV batches + 1 re-delivery batch | CSV file batches | 541,909 |
| `uci_online_retail` | `product_catalog` | Derived from the same data (distinct StockCode, modal Description, mean UnitPrice, first-seen date) | CSV file | 4,070 |
| `mock_popularity_api` | `product_popularity` | Mock REST API ([src/mock_api.py](../src/mock_api.py)): per-product popularity score, sentiment score, units sold; one snapshot per week | HTTP/JSON, paginated, polled periodically | 4,070 per snapshot |

The API stands in for the "external API (sentiment/popularity scores)" in the assignment. Scores are derived deterministically from the transactions and drift slightly with each snapshot.

Batches are produced by [src/prepare_batches.py](../src/prepare_batches.py). Files are partitioned by source, type and period:

```
data/
├── raw/uci_online_retail/                 # staged batches (not yet delivered)
├── landing/
│   ├── uci_online_retail/                 # delivered CSV batches = what gets ingested
│   │   ├── transactions/dt=YYYY-MM/transactions_batch_NNN.csv
│   │   └── product_catalog/dt=YYYY-MM-DD/product_catalog_batch_001.csv
│   └── mock_popularity_api/product_popularity/dt=YYYY-MM-DD/snapshot_NNN_page_PPP.json   # raw API pages
├── lake/bronze/                           # Bronze Delta tables
│   ├── transactions/   product_catalog/   product_popularity/   ingestion_log/
├── rejected/<type>/                       # failed files and malformed lines
└── logs/ingestion_<timestamp>.log         # execution logs (+ ingestion_log_export.csv)
```

## 3. Ingestion design

### 3.1 CSV sources
1. **Discovery.** Files in `landing/` with no `SUCCESS` entry in `ingestion_log` are pending (file-level idempotency).
2. **Header validation.** The CSV header must equal the expected columns, otherwise the batch is rejected (schema mismatch).
3. **Read.** `pyarrow.csv` with an explicit all-string schema; empty fields become NULL; malformed lines are skipped, counted (`rows_corrupt`) and written to `rejected/`.
4. **Metadata.** Each record gets `_batch_id`, `_source_file`, `_source_system`, `_record_type`, `_ingest_ts`, `_record_hash`, `_occurrence_no`.
5. **Duplicate prevention.** `_record_hash` is the SHA-256 of all source columns. `_occurrence_no` numbers identical rows within one file. A Delta `MERGE … WHEN NOT MATCHED THEN INSERT` adds only rows whose (`_record_hash`, `_occurrence_no`) is not already in Bronze.
   Re-delivering a file (or part of one) inserts nothing, while genuine duplicate lines inside a source file are kept for Silver to profile.
6. **Error handling.** Transient errors are retried up to 3 times with exponential backoff (2 s, 4 s). Data errors such as a schema mismatch are not retried. Failed files are moved to `rejected/` and the pipeline continues.

### 3.2 REST API source
1. **Periodic fetching.** `poll_api()` fetches on a fixed interval; each cycle requests the snapshot the API currently serves.
2. **Pagination and timeouts.** Each snapshot is fetched page by page (500 items/page) with a 5 s timeout per request.
3. **Retry.** Network errors and HTTP 5xx are retried per request with exponential backoff (2 s, 4 s) up to 3 attempts; 4xx responses fail immediately. `attempts` in `ingestion_log` is the highest attempt count any single request needed.
4. **Raw storage.** Every page is saved unchanged as JSON under `landing/mock_popularity_api/...` before parsing (audit trail).
5. **Bronze load.** Items are stored as strings with the same metadata columns. `as_of` is part of the record hash, so re-fetching a snapshot inserts nothing, while a new snapshot adds new rows (history of scores is kept).
6. **Local endpoints bypass system proxies** (corporate proxies otherwise break `127.0.0.1` calls).

### 3.3 Logging (both sources)
Every step goes to the console and a timestamped log file; one row per file/snapshot goes to the `ingestion_log` Delta table.

## 4. Tables (Delta, under `data/lake/bronze/`)

| Table | Description |
|---|---|
| `transactions` | Raw transaction lines + metadata columns |
| `product_catalog` | Raw product catalog + metadata columns |
| `product_popularity` | Raw API snapshots (`stock_code, popularity_score, sentiment_score, units_sold, as_of`) + metadata columns |
| `ingestion_log` | `batch_id, record_type, source_file, run_mode, status, rows_read, rows_inserted, rows_duplicate_skipped, rows_corrupt, attempts, error_message, start_ts, end_ts, duration_sec` |

Read from Python with `DeltaTable("data/lake/bronze/transactions").to_pyarrow_table()`.

## 5. Batch processing demonstration and results

Run: `python notebooks/02_bronze_ingestion.py --reset` (about a minute; most of it is deliberate retry back-off).

| Run | Scenario | Result |
|---|---|---|
| A | First delivery: catalog + batch 001 | 4,070 + 42,481 rows inserted |
| B | Incremental: batches 002, 003 | 35,147 + 27,707 inserted |
| C | Forced re-run of batch 002 | 35,147 read, 0 inserted, 35,147 duplicates skipped |
| D | Re-delivery file with 1,000 old rows | 0 inserted, 1,000 duplicates skipped |
| E | File with wrong header | `FAILED`, not retried, moved to `rejected/` |
| F | Batches 004–013 | 436,574 inserted, 0 duplicates |
| G | Whole pipeline re-run | Every file skipped (already ingested) |
| H | API polling, 3 cycles against a flaky server (every 3rd request returns 503) | Snapshot 1: 4,070 inserted · same snapshot again: 0 inserted, 4,070 skipped · snapshot 2: 4,070 inserted; each cycle needed 2 attempts |
| I | API unreachable | `FAILED` after 3 attempts with 2 s / 4 s back-off, logged |

Final checks: 541,909 transactions, 4,070 products, 8,140 popularity rows (2 snapshots), 0 duplicate keys in every table (`Bronze verification PASSED`).
Bronze preserved the source faithfully: 135,080 NULL `CustomerID`, 1,454 NULL `Description`, 5,268 exact duplicate lines.

## 6. Known data-quality issues passed to Silver
Missing `CustomerID` (135,080 rows), missing `Description` (1,454), 5,268 exact duplicate source lines, cancelled invoices (`InvoiceNo` starting with `C`), negative `Quantity`, zero `UnitPrice`. The popularity table holds several snapshots per product; Silver/Gold should pick the latest `as_of` for current popularity.

## 7. Evidence ([docs/evidence/](evidence/))
Generated from the clean `--reset` run of 18 Nov 2026 (15:27). Each item is a `.png` (for the report) with a `.txt` twin (copy-paste).

| File | Shows |
|---|---|
| `execution_log.log` | Full execution log of the run (deliverable: ingestion execution logs) |
| `ingestion_log_export.csv` | Export of the `ingestion_log` Delta table |
| `01_ingestion_log_table` | One row per batch/snapshot: status, rows read/inserted/skipped, attempts, duration |
| `02_batch_processing_log` | Batch results: incremental load, re-run (0 inserted), re-delivery (1,000 skipped) |
| `03_retry_and_failure_log` | API 503 retries, API down after 3 attempts, bad-header rejection |
| `04_bronze_tables_summary` | Row counts, schema, NULLs, 0 duplicate keys, rows per batch |
| `05_delta_history_transactions` | One Delta `MERGE` commit per batch |
| `06_folder_layout` | Partitioned landing zone, Bronze tables, rejected files, logs |
| `07_raw_api_page_sample` | Raw API JSON page stored unchanged |

Re-running `python notebooks/02_bronze_ingestion.py --reset` reproduces the same results.
