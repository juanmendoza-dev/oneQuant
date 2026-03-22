"""
Coinbase Advanced Trade REST API — historical OHLCV fetcher.
Fetches 2 years of BTC-USD candles for 5m, 15m, and 1h timeframes.
Run once: python -m historical.fetch
"""

import sys
import time
import hashlib
import hmac
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import urllib.request
import urllib.parse
import json

# Allow running as `python -m historical.fetch` from onequant/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import COINBASE_API_KEY, COINBASE_API_SECRET, DATABASE_PATH
from database.db import init_db, get_connection

BASE_URL = "https://api.coinbase.com"

# Coinbase granularity values
GRANULARITIES = {
    "5m": ("FIVE_MINUTE", 300),
    "15m": ("FIFTEEN_MINUTE", 900),
    "1h": ("ONE_HOUR", 3600),
}

# Max candles per request
MAX_CANDLES = 300

TWO_YEARS_SECONDS = 2 * 365 * 24 * 3600


def _sign(method: str, path: str, body: str = "") -> dict:
    timestamp = str(int(time.time()))
    message = timestamp + method.upper() + path + body
    signature = hmac.new(
        COINBASE_API_SECRET.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    return {
        "CB-ACCESS-KEY": COINBASE_API_KEY,
        "CB-ACCESS-SIGN": signature,
        "CB-ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json",
    }


def fetch_candles(product_id: str, start: int, end: int, granularity: str) -> list:
    path = f"/api/v3/brokerage/products/{product_id}/candles"
    params = urllib.parse.urlencode({
        "start": str(start),
        "end": str(end),
        "granularity": granularity,
    })
    full_path = f"{path}?{params}"
    headers = _sign("GET", full_path)
    url = f"{BASE_URL}{full_path}"

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())

    return data.get("candles", [])


def run():
    init_db()
    conn = get_connection()
    now = int(time.time())
    start_ts = now - TWO_YEARS_SECONDS

    for tf_label, (granularity, seconds) in GRANULARITIES.items():
        print(f"\n{'='*60}")
        print(f"Fetching {tf_label} candles (granularity={granularity})")
        print(f"{'='*60}")

        window = MAX_CANDLES * seconds
        current_start = start_ts
        total_inserted = 0
        total_skipped = 0

        while current_start < now:
            current_end = min(current_start + window, now)

            try:
                candles = fetch_candles("BTC-USD", current_start, current_end, granularity)
            except Exception as e:
                print(f"  Error fetching {tf_label} @ {current_start}: {e}")
                time.sleep(5)
                continue

            inserted = 0
            for c in candles:
                ts = int(c["start"])
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO btc_candles "
                        "(timestamp, open, high, low, close, volume, timeframe) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (ts, float(c["open"]), float(c["high"]),
                         float(c["low"]), float(c["close"]),
                         float(c["volume"]), tf_label),
                    )
                    if conn.total_changes:
                        inserted += 1
                except sqlite3.IntegrityError:
                    pass

            conn.commit()
            total_inserted += inserted
            total_skipped += len(candles) - inserted

            pct = min(100, ((current_end - start_ts) / (now - start_ts)) * 100)
            start_dt = datetime.fromtimestamp(current_start, tz=timezone.utc)
            print(
                f"  {tf_label} | {start_dt:%Y-%m-%d %H:%M} | "
                f"+{inserted} candles | {pct:.1f}% done"
            )

            current_start = current_end

            # Rate limiting — Coinbase allows ~10 req/s
            time.sleep(0.15)

        print(f"  {tf_label} complete: {total_inserted} inserted, {total_skipped} duplicates skipped")

    print("\nHistorical fetch complete.")


if __name__ == "__main__":
    run()
