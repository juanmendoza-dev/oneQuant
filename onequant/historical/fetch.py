"""Coinbase Advanced Trade historical OHLCV candle fetcher.

Fetches BTC-USD candles for 5m, 15m, and 1h timeframes going back to
2016-01-01 (maximum useful Coinbase history). Paginates through the
Coinbase API (max 300 candles per request), skips timestamps already
in the database, and displays a progress bar.

Usage:
    cd onequant/
    python -m historical.fetch
"""

import argparse
import asyncio
import hashlib
import hmac
import logging
import sys
import time
from pathlib import Path
from typing import Any

import aiohttp
from tqdm import tqdm

# Ensure parent package is importable when run as __main__
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import config
from database.db import close_db, init_db, insert_candles_bulk, insert_system_log

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODULE_NAME: str = "historical"
BASE_URL: str = "https://api.coinbase.com"
DEFAULT_PRODUCT_ID: str = "BTC-USD"
CANDLES_PER_REQUEST: int = 300
RATE_LIMIT_DELAY: float = 0.35  # seconds between API calls

# Coinbase granularity names → (api_granularity, seconds, db_timeframe)
TIMEFRAME_MAP: dict[str, tuple[str, int]] = {
    "5m": ("FIVE_MINUTE", 300),
    "15m": ("FIFTEEN_MINUTE", 900),
    "1h": ("ONE_HOUR", 3600),
}

SECONDS_PER_YEAR: int = 365 * 24 * 3600
# 2016-01-01 00:00:00 UTC — maximum useful Coinbase BTC-USD history
EARLIEST_TIMESTAMP: int = 1451606400

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(MODULE_NAME)


def _setup_logging() -> None:
    """Configure file and console logging for the historical module."""
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    Path("logs").mkdir(exist_ok=True)

    fh = logging.FileHandler("logs/historical.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _auth_headers(method: str, path: str, body: str = "") -> dict[str, str]:
    """Generate Coinbase Advanced Trade authentication headers."""
    timestamp = str(int(time.time()))
    message = timestamp + method.upper() + path + body
    signature = hmac.new(
        config.COINBASE_API_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "CB-ACCESS-KEY": config.COINBASE_API_KEY,
        "CB-ACCESS-SIGN": signature,
        "CB-ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------


async def _fetch_candles_page(
    session: aiohttp.ClientSession,
    start: int,
    end: int,
    granularity: str,
    product_id: str = DEFAULT_PRODUCT_ID,
) -> list[dict[str, Any]]:
    """Fetch a single page of candles from the Coinbase API."""
    path = (
        f"/api/v3/brokerage/market/products/{product_id}/candles"
        f"?start={start}&end={end}&granularity={granularity}"
    )
    headers = _auth_headers("GET", path)
    async with session.get(BASE_URL + path, headers=headers) as resp:
        if resp.status != 200:
            text = await resp.text()
            logger.error("Candles API %s: %s", resp.status, text)
            return []
        data = await resp.json()
        return data.get("candles", [])


async def _fetch_timeframe(
    tf_label: str,
    granularity: str,
    interval: int,
    product_id: str = DEFAULT_PRODUCT_ID,
) -> int:
    """Fetch all historical candles for a single timeframe. Returns insert count."""
    now = int(time.time())
    earliest = EARLIEST_TIMESTAMP
    page_span = CANDLES_PER_REQUEST * interval

    # Calculate total pages for progress bar
    total_pages = ((now - earliest) // page_span) + 1
    total_inserted = 0

    logger.info("Fetching %s %s candles from %d to %d (%d pages)", product_id, tf_label, earliest, now, total_pages)

    async with aiohttp.ClientSession() as session:
        current_end = now
        with tqdm(total=total_pages, desc=f"  {tf_label}", unit="page") as pbar:
            while current_end > earliest:
                current_start = max(current_end - page_span, earliest)

                try:
                    candles = await _fetch_candles_page(
                        session, current_start, current_end, granularity, product_id
                    )
                except Exception as exc:
                    msg = f"Fetch error ({tf_label}): {exc}"
                    logger.error(msg)
                    try:
                        await insert_system_log(MODULE_NAME, "ERROR", msg)
                    except Exception:
                        pass
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                    pbar.update(1)
                    current_end = current_start
                    continue

                if candles:
                    rows = []
                    for c in candles:
                        try:
                            rows.append((
                                int(c["start"]),
                                tf_label,
                                float(c["open"]),
                                float(c["high"]),
                                float(c["low"]),
                                float(c["close"]),
                                float(c["volume"]),
                            ))
                        except (KeyError, ValueError, TypeError) as exc:
                            logger.warning("Skipping malformed candle: %s", exc)

                    if rows:
                        inserted = await insert_candles_bulk(rows, symbol=product_id)
                        total_inserted += inserted

                pbar.update(1)
                current_end = current_start
                await asyncio.sleep(RATE_LIMIT_DELAY)

    return total_inserted


async def run_historical_fetch(product_id: str = DEFAULT_PRODUCT_ID) -> None:
    """Fetch historical candles for all timeframes and store in the database."""
    _setup_logging()
    await init_db()

    print("=" * 60)
    print("oneQuant — Historical OHLCV Fetcher")
    print(f"Product: {product_id}")
    years = (int(time.time()) - EARLIEST_TIMESTAMP) / SECONDS_PER_YEAR
    print(f"Target lookback: {years:.1f} years (back to 2016-01-01)")
    print("=" * 60)

    grand_total = 0
    for tf_label, (granularity, interval) in TIMEFRAME_MAP.items():
        inserted = await _fetch_timeframe(tf_label, granularity, interval, product_id)
        logger.info("%s %s: inserted %d candles", product_id, tf_label, inserted)
        grand_total += inserted

    print(f"\nTotal candles inserted: {grand_total}")
    logger.info("Historical fetch complete — %d total candles inserted", grand_total)
    await close_db()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch historical OHLCV candles")
    parser.add_argument(
        "--symbol",
        default=DEFAULT_PRODUCT_ID,
        help=f"Coinbase product ID to fetch (default: {DEFAULT_PRODUCT_ID})",
    )
    args = parser.parse_args()
    asyncio.run(run_historical_fetch(product_id=args.symbol))
