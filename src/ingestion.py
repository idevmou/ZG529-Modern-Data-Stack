"""Bronze ingestion: CSV batches -> Delta tables with metadata and duplicate prevention (pure Python)."""
import csv
import glob
import hashlib
import json
import logging
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
from deltalake import DeltaTable, write_deltalake

import config

logger = logging.getLogger("bronze")

LOG_SCHEMA = pa.schema([
    ("batch_id", pa.string()), ("record_type", pa.string()), ("source_file", pa.string()),
    ("run_mode", pa.string()), ("status", pa.string()), ("rows_read", pa.int64()),
    ("rows_inserted", pa.int64()), ("rows_duplicate_skipped", pa.int64()),
    ("rows_corrupt", pa.int64()), ("attempts", pa.int32()), ("error_message", pa.string()),
    ("start_ts", pa.timestamp("us", tz="UTC")), ("end_ts", pa.timestamp("us", tz="UTC")),
    ("duration_sec", pa.float64()),
])


def setup_logging():
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.info("Logging to %s", log_file)
    return log_file


def with_retry(fn, label=""):
    """Retry transient errors with exponential backoff; ValueError is a data problem and is not retried."""
    for attempt in range(1, config.MAX_ATTEMPTS + 1):
        try:
            return fn()
        except ValueError:
            raise
        except Exception as exc:
            if attempt == config.MAX_ATTEMPTS:
                raise
            delay = config.RETRY_BASE_DELAY_SEC * 2 ** (attempt - 1)
            logger.warning("%s failed (attempt %d/%d): %s -- retrying in %ds",
                           label, attempt, config.MAX_ATTEMPTS, exc, delay)
            time.sleep(delay)


def deliver(*stems):
    """Copy staged batches into the landing folder to simulate new files arriving."""
    for stem in stems:
        matches = glob.glob(str(config.STAGING / config.SOURCE / "*" / "*" / f"{stem}.csv"))
        if not matches:
            raise FileNotFoundError(stem)
        src = Path(matches[0])
        dest = config.LANDING / src.relative_to(config.STAGING)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest)
        logger.info("Delivered %s", dest)


def validate_header(path, columns):
    with open(path, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f), None)
    if header != columns:
        raise ValueError(f"Schema mismatch in {Path(path).name}: expected {columns}, found {header}")


def read_batch(name, path):
    """Read a CSV with an all-string schema. Returns (table, malformed_lines)."""
    cols = config.SOURCES[name]["columns"]
    malformed = []

    def on_invalid(row):
        malformed.append(row.text)
        return "skip"

    table = pacsv.read_csv(
        path,
        parse_options=pacsv.ParseOptions(invalid_row_handler=on_invalid),
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in cols},
            null_values=[""], strings_can_be_null=True),
    )
    return table, malformed


def add_metadata(table, name, batch_id, path):
    """Add lineage columns, a content hash and a per-file occurrence number.

    The hash covers every source column. The occurrence number counts identical rows inside one
    file, so genuine duplicate lines survive while re-delivered rows can be recognised.
    """
    cols = config.SOURCES[name]["columns"]
    seen, hashes, occurrence = {}, [], []
    for row in zip(*(table.column(c).to_pylist() for c in cols)):
        payload = "\x01".join("\x02" if v is None else v for v in row)
        h = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        seen[h] = seen.get(h, 0) + 1
        hashes.append(h)
        occurrence.append(seen[h])

    n = table.num_rows
    now = datetime.now(timezone.utc)
    meta = {
        "_source_file": pa.array([Path(path).as_posix()] * n, pa.string()),
        "_record_hash": pa.array(hashes, pa.string()),
        "_occurrence_no": pa.array(occurrence, pa.int64()),
        "_batch_id": pa.array([batch_id] * n, pa.string()),
        "_record_type": pa.array([name] * n, pa.string()),
        "_source_system": pa.array([config.SOURCES[name]["source"]] * n, pa.string()),
        "_ingest_ts": pa.array([now] * n, pa.timestamp("us", tz="UTC")),
    }
    for col, arr in meta.items():
        table = table.append_column(col, arr)
    return table


def merge_into_bronze(name, batch):
    """Insert only records not already in Bronze. Returns the number of rows inserted."""
    path = str(config.SOURCES[name]["table"])
    if not DeltaTable.is_deltatable(path):
        write_deltalake(path, batch, mode="error")
        return batch.num_rows
    metrics = (DeltaTable(path)
               .merge(source=batch, predicate="t._record_hash = s._record_hash "
                                              "AND t._occurrence_no = s._occurrence_no",
                      source_alias="s", target_alias="t")
               .when_not_matched_insert_all()
               .execute())
    return int(metrics["num_target_rows_inserted"])


def write_log_row(row):
    write_deltalake(str(config.LOG_TABLE), pa.Table.from_pylist([row], schema=LOG_SCHEMA), mode="append")


def _run(name, batch_id, source_ref, run_mode, work, retry=True, on_failure=None):
    """Run one ingestion unit, record the outcome in ingestion_log and return its status.

    `work(attempts)` returns (rows_read, rows_inserted, rows_corrupt) and updates attempts["n"].
    """
    start = datetime.now(timezone.utc)
    attempts, status, error = {"n": 0}, "SUCCESS", None
    rows_read = inserted = corrupt = 0
    logger.info("START  %s [%s] source=%s", batch_id, run_mode, source_ref)

    def counted():
        if retry:
            attempts["n"] += 1
        return work(attempts)

    try:
        rows_read, inserted, corrupt = with_retry(counted, label=batch_id) if retry else counted()
        logger.info("OK     %s read=%d inserted=%d duplicates_skipped=%d corrupt=%d",
                    batch_id, rows_read, inserted, rows_read - inserted, corrupt)
    except Exception as exc:
        status, error = "FAILED", str(exc)[:2000]
        logger.error("FAILED %s after %d attempt(s): %s", batch_id, attempts["n"], error)
        if on_failure:
            on_failure()

    end = datetime.now(timezone.utc)
    write_log_row({
        "batch_id": batch_id, "record_type": name, "source_file": source_ref,
        "run_mode": run_mode, "status": status, "rows_read": rows_read,
        "rows_inserted": inserted, "rows_duplicate_skipped": rows_read - inserted,
        "rows_corrupt": corrupt, "attempts": attempts["n"], "error_message": error,
        "start_ts": start, "end_ts": end, "duration_sec": (end - start).total_seconds(),
    })
    return status


def ingest_batch(name, path, run_mode="NORMAL"):
    path = Path(path)
    batch_id = path.stem

    def work(attempts):
        validate_header(path, config.SOURCES[name]["columns"])
        table, malformed = read_batch(name, path)
        if malformed:
            out = config.REJECTED / name / f"{batch_id}_malformed_lines.txt"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text("\n".join(malformed), encoding="utf-8")
        batch = add_metadata(table, name, batch_id, path)
        return batch.num_rows, merge_into_bronze(name, batch), len(malformed)

    def reject_file():
        dest = config.REJECTED / name / path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(dest))
        logger.info("Moved failed file to %s", dest)

    return _run(name, batch_id, path.as_posix(), run_mode, work, on_failure=reject_file)


def list_landing():
    return [(name, Path(p)) for name, cfg in config.SOURCES.items() if cfg["kind"] == "csv"
            for p in sorted(glob.glob(str(config.LANDING / config.SOURCE / name / "*" / "*.csv")))]


def read_log():
    if not DeltaTable.is_deltatable(str(config.LOG_TABLE)):
        return None
    return DeltaTable(str(config.LOG_TABLE)).to_pyarrow_table()


def succeeded_files():
    log = read_log()
    if log is None:
        return set()
    return {f for f, s in zip(log["source_file"].to_pylist(), log["status"].to_pylist()) if s == "SUCCESS"}


def run_pending():
    """Ingest every landing file that has not been ingested successfully yet."""
    done = succeeded_files()
    for name, path in list_landing():
        if path.as_posix() in done:
            logger.info("SKIP   %s (file already ingested)", path.name)
        else:
            ingest_batch(name, path)


def show_log(n=10):
    df = read_log().to_pandas().sort_values("start_ts").tail(n)
    cols = ["batch_id", "run_mode", "status", "rows_read", "rows_inserted",
            "rows_duplicate_skipped", "rows_corrupt", "attempts", "duration_sec"]
    print(df[cols].to_string(index=False))


# ---------------------------------------------------------------- REST API source

def _open(url):
    # Local endpoints must bypass any system/corporate proxy; external ones keep the default behaviour.
    host = urllib.parse.urlparse(url).hostname
    if host in ("127.0.0.1", "localhost"):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return opener.open(url, timeout=config.API_TIMEOUT_SEC)
    return urllib.request.urlopen(url, timeout=config.API_TIMEOUT_SEC)


def _fetch_page(url, attempts):
    """GET one page with a timeout. Network errors and HTTP 5xx are retried with backoff; 4xx fails fast."""
    for attempt in range(1, config.MAX_ATTEMPTS + 1):
        attempts["n"] = max(attempts["n"], attempt)
        try:
            with _open(url) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code < 500:
                raise ValueError(f"HTTP {exc.code} from {url}") from exc
            error = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            error = str(getattr(exc, "reason", exc))
        if attempt == config.MAX_ATTEMPTS:
            raise ConnectionError(f"{url} unreachable after {attempt} attempts: {error}")
        delay = config.RETRY_BASE_DELAY_SEC * 2 ** (attempt - 1)
        logger.warning("GET %s failed (attempt %d/%d): %s -- retrying in %ds",
                       url, attempt, config.MAX_ATTEMPTS, error, delay)
        time.sleep(delay)


def ingest_api_snapshot(base_url, snapshot, run_mode="NORMAL"):
    """Fetch every page of one popularity snapshot, store the raw JSON, and merge it into Bronze."""
    name = "product_popularity"
    cols = config.SOURCES[name]["columns"]
    batch_id = f"{name}_snapshot_{snapshot:03d}"
    ref = f"{base_url}{config.API_PATH}?snapshot={snapshot}"

    def work(attempts):
        items, page, total_pages, raw_dir = [], 1, 1, None
        while page <= total_pages:
            body = _fetch_page(f"{ref}&page={page}&page_size={config.API_PAGE_SIZE}", attempts)
            payload = json.loads(body)
            raw_dir = config.API_LANDING / f"dt={payload['as_of']}"
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / f"snapshot_{snapshot:03d}_page_{page:03d}.json").write_bytes(body)
            items.extend(payload["items"])
            total_pages, page = payload["total_pages"], page + 1
        if any(not set(cols) <= set(item) for item in items):
            raise ValueError(f"Schema mismatch in API response: expected fields {cols}")
        table = pa.table({c: pa.array([None if i[c] is None else str(i[c]) for i in items], pa.string())
                          for c in cols})
        batch = add_metadata(table, name, batch_id, raw_dir)
        return batch.num_rows, merge_into_bronze(name, batch), 0

    return _run(name, batch_id, ref, run_mode, work, retry=False)


def poll_api(base_url, snapshots, interval_sec=2):
    """Periodic fetching: one fetch per cycle, `interval_sec` apart. Each cycle sees the snapshot the API serves then."""
    for i, snapshot in enumerate(snapshots, start=1):
        if i > 1:
            time.sleep(interval_sec)
        logger.info("POLL cycle %d/%d (snapshot %d)", i, len(snapshots), snapshot)
        ingest_api_snapshot(base_url, snapshot, run_mode="POLL")
