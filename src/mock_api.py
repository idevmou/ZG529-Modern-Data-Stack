"""Mock REST API serving product popularity/sentiment snapshots (stands in for an external API).

Endpoint:  GET /api/v1/products/popularity?snapshot=<n>&page=<p>&page_size=<s>
Scores are derived deterministically from the transaction data, and drift a little with each snapshot.

Standalone:  python src/mock_api.py --port 8765 [--flaky]
--flaky makes every 3rd request fail with HTTP 503 so client retries can be demonstrated.
"""
import argparse
import glob
import hashlib
import json
import math
import threading
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

import config

BASE_DATE = date(2011, 12, 9)
SNAPSHOT_INTERVAL_DAYS = 7
PATH = "/api/v1/products/popularity"


def _unit_hash(text):
    return int(hashlib.sha256(text.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF


def snapshot_date(snapshot):
    return (BASE_DATE + timedelta(days=SNAPSHOT_INTERVAL_DAYS * (snapshot - 1))).isoformat()


def load_base_data():
    """Return a DataFrame with one row per catalog product and its total units sold."""
    catalog_files = glob.glob(str(config.STAGING / config.SOURCE / "product_catalog" / "*" / "*.csv"))
    codes = pacsv.read_csv(
        catalog_files[0],
        convert_options=pacsv.ConvertOptions(include_columns=["StockCode"], column_types={"StockCode": pa.string()}),
    ).to_pandas()
    frames = []
    for path in sorted(glob.glob(str(config.STAGING / config.SOURCE / "transactions" / "*" / "*batch_[0-9][0-9][0-9].csv"))):
        frames.append(pacsv.read_csv(
            path,
            convert_options=pacsv.ConvertOptions(
                include_columns=["StockCode", "Quantity"],
                column_types={"StockCode": pa.string(), "Quantity": pa.int64()}),
        ).to_pandas())
    units = pd.concat(frames).assign(Quantity=lambda d: d["Quantity"].clip(lower=0)).groupby("StockCode")["Quantity"].sum()
    codes["units_sold"] = codes["StockCode"].map(units).fillna(0).astype(int)
    return codes.sort_values("StockCode").reset_index(drop=True)


def build_item(code, base_units, max_units, snapshot):
    units = int(round(base_units * (1 + 0.02 * (snapshot - 1))))
    popularity = 100 * math.log1p(units) / math.log1p(max_units)
    popularity += (_unit_hash(f"{code}:{snapshot}") - 0.5) * 4 * (snapshot > 1)
    sentiment = 1 + 4 * _unit_hash(code)
    return {
        "stock_code": code,
        "popularity_score": round(min(max(popularity, 0), 100), 2),
        "sentiment_score": round(sentiment, 2),
        "units_sold": units,
        "as_of": snapshot_date(snapshot),
    }


class PopularityAPI:
    def __init__(self, flaky=False):
        self.base = load_base_data()
        self.max_units = int(self.base["units_sold"].max())
        self.flaky = flaky
        self.requests = 0
        self._lock = threading.Lock()

    def next_request_fails(self):
        with self._lock:
            self.requests += 1
            return self.flaky and self.requests % 3 == 0

    def page(self, snapshot, page, page_size):
        total = len(self.base)
        total_pages = max(1, math.ceil(total / page_size))
        rows = self.base.iloc[(page - 1) * page_size: page * page_size]
        items = [build_item(c, u, self.max_units, snapshot) for c, u in zip(rows["StockCode"], rows["units_sold"])]
        return {"snapshot": snapshot, "as_of": snapshot_date(snapshot), "page": page, "page_size": page_size,
                "total_pages": total_pages, "total_items": total, "items": items}


def _make_handler(api):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, payload):
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path != PATH:
                return self._send(404, {"error": "not found"})
            if api.next_request_fails():
                return self._send(503, {"error": "service temporarily unavailable"})
            q = parse_qs(url.query)
            try:
                snapshot = int(q.get("snapshot", ["1"])[0])
                page = int(q.get("page", ["1"])[0])
                page_size = int(q.get("page_size", ["500"])[0])
                assert snapshot >= 1 and page >= 1 and 1 <= page_size <= 1000
            except (ValueError, AssertionError):
                return self._send(400, {"error": "invalid parameters"})
            self._send(200, api.page(snapshot, page, page_size))

        def log_message(self, *args):
            pass

    return Handler


def start_server(port=8765, flaky=False):
    """Start the API on a background thread and return the server (call .shutdown() to stop)."""
    server = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(PopularityAPI(flaky)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--flaky", action="store_true")
    args = ap.parse_args()
    srv = start_server(args.port, args.flaky)
    print(f"Serving http://127.0.0.1:{args.port}{PATH}  (flaky={args.flaky}); Ctrl+C to stop")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        srv.shutdown()
