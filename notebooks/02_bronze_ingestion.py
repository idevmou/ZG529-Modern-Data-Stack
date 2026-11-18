"""02 - Bronze ingestion demo (pure Python: pyarrow + deltalake).

Run from the repo root:
    python notebooks/02_bronze_ingestion.py            # run the full demo
    python notebooks/02_bronze_ingestion.py --reset    # wipe Bronze tables/landing first, then run

Prerequisite: batch files exist in data/raw (python src/prepare_batches.py ...).
"""
import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import config
import ingestion as ing
import mock_api
from deltalake import DeltaTable

API_PORT = 8765


def section(title):
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def reset():
    for d in (config.LAKE, config.LANDING, config.REJECTED):
        shutil.rmtree(d, ignore_errors=True)


def main(do_reset):
    if do_reset:
        reset()
    log_file = ing.setup_logging()
    txn_dir = config.LANDING / config.SOURCE / "transactions"

    section("Run A - first delivery: product catalog + batch 001")
    ing.deliver("product_catalog_batch_001", "transactions_batch_001")
    ing.run_pending()
    ing.show_log(5)

    section("Run B - incremental load: batches 002 and 003 arrive")
    ing.deliver("transactions_batch_002", "transactions_batch_003")
    ing.run_pending()
    ing.show_log(5)

    section("Run C - forced re-run of batch 002 (expect 0 inserted)")
    ing.ingest_batch("transactions", txn_dir / "dt=2011-01" / "transactions_batch_002.csv", run_mode="RERUN")
    ing.show_log(3)

    section("Run D - a different file re-delivers 1,000 already-loaded records")
    ing.deliver("transactions_batch_014_redelivery")
    ing.run_pending()
    ing.show_log(3)

    section("Run E - failure handling: file with a wrong header")
    bad = txn_dir / "dt=2011-02" / "transactions_batch_099_bad_header.csv"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("InvoiceNo,StockCode,Qty\n999999,ABC,1\n", encoding="utf-8")
    ing.run_pending()
    ing.show_log(3)

    section("Run F - remaining monthly batches 004-013")
    ing.deliver(*[f"transactions_batch_{i:03d}" for i in range(4, 14)])
    ing.run_pending()
    ing.show_log(12)

    section("Run G - re-run the whole pipeline: everything is skipped")
    ing.run_pending()

    section("Run H - REST API source: periodic polling against a flaky server (every 3rd request returns 503)")
    server = mock_api.start_server(API_PORT, flaky=True)
    api_url = f"http://127.0.0.1:{API_PORT}"
    # Cycle 1 loads snapshot 1; cycle 2 sees the same data (nothing new); cycle 3 sees a new snapshot.
    ing.poll_api(api_url, snapshots=[1, 1, 2], interval_sec=2)
    ing.show_log(3)
    server.shutdown()

    section("Run I - REST API unavailable: retries with backoff, then FAILED and logged")
    ing.ingest_api_snapshot("http://127.0.0.1:9", snapshot=3)
    ing.show_log(1)

    section("Verification")
    pop = DeltaTable(str(config.SOURCES["product_popularity"]["table"])).to_pyarrow_table().to_pandas()
    pop_dups = int(pop.duplicated(["_record_hash", "_occurrence_no"]).sum())
    print(f"product_popularity={len(pop):,}  snapshots={sorted(pop['as_of'].unique())}  duplicate_keys={pop_dups}")
    assert len(pop) == 2 * 4070 and pop_dups == 0
    print(pop.head(3).drop(columns=["_source_file", "_record_hash"]).to_string(index=False))
    raw_pages = sorted(p.name for p in config.API_LANDING.glob("*/*.json"))
    print(f"raw API pages stored: {len(raw_pages)} (e.g. {raw_pages[0]})")
    tx = DeltaTable(str(config.SOURCES["transactions"]["table"])).to_pyarrow_table().to_pandas()
    cat = DeltaTable(str(config.SOURCES["product_catalog"]["table"])).to_pyarrow_table().to_pandas()
    dup_keys = int(tx.duplicated(["_record_hash", "_occurrence_no"]).sum())
    print(f"transactions={len(tx):,}  product_catalog={len(cat):,}  duplicate_keys={dup_keys}")
    assert len(tx) == 541909, len(tx)
    assert len(cat) == 4070, len(cat)
    assert dup_keys == 0
    print("Bronze verification PASSED")

    print("\nRows per batch:")
    print(tx.groupby("_batch_id").size().to_string())

    print("\nFull ingestion log:")
    log = ing.read_log().to_pandas().sort_values("start_ts")
    print(log.drop(columns=["source_file", "error_message", "start_ts", "end_ts"]).to_string(index=False))
    failed = log[log["status"] == "FAILED"]
    print("\nFailures:\n" + failed[["batch_id", "error_message"]].to_string(index=False))

    print("\nDelta history of transactions (latest 5 commits):")
    for h in DeltaTable(str(config.SOURCES["transactions"]["table"])).history()[:5]:
        print(h["version"], h["operation"], h.get("operationMetrics", {}).get("num_target_rows_inserted", ""))

    export = config.LOG_DIR / "ingestion_log_export.csv"
    log.to_csv(export, index=False)
    print(f"\nExported ingestion log to {export}\nRun log: {log_file}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true")
    main(ap.parse_args().reset)
