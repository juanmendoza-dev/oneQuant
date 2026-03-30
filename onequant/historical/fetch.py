"""Binance.US historical OHLCV candle fetcher.

Fetches BTCUSD candles for 5m, 15m, 1h, and 1d timeframes going back to
2020-01-01. Paginates through the Binance.US API (max 1000 candles per
request), skips timestamps already in the database, and displays a progress
bar. Logs progress every 10,000 candles.

Usage:
    cd onequant/
    python -m historical.fetch
"""

import argparse
import asyncio
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
BASE_URL: str = "https://api.binance.us/api/v3/klines"
DEFAULT_SYMBOL: str = "BTCUSD"
CANDLES_PER_REQUEST: int = 1000
RATE_LIMIT_DELAY: float = 0.25  # seconds between API calls

# Binance interval strings → (interval_str, seconds_per_candle, db_timeframe)
TIMEFRAME_MAP: dict[str, tuple[str, int]] = {
    "5m": ("5m", 300),
    "15m": ("15m", 900),
    "1h": ("1h", 3600),
    "1d": ("1d", 86400),
}

# 2020-01-01 00:00:00 UTC
EARLIEST_TIMESTAMP_MS: int = 1577836800000
LOG_EVERY_N: int = 10000

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
# Fetch logic
# ---------------------------------------------------------------------------


async def _fetch_candles_page(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
) -> list[list]:
    """Fetch a single page of candles from the Binance.US API."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time_ms,
        "endTime": end_time_ms,
        "limit": CANDLES_PER_REQUEST,
    }
    try:
        async with session.get(BASE_URL, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error("Klines API %s: %s", resp.status, text)
                return []
            return await resp.json()
    except Exception as exc:
        logger.error("Fetch error: %s", exc)
        return []


async def _fetch_timeframe(
    tf_label: str,
    interval: str,
    interval_seconds: int,
    symbol: str = DEFAULT_SYMBOL,
) -> int:
    """Fetch all historical candles for a single timeframe. Returns insert count."""
    now_ms = int(time.time() * 1000)
    page_span_ms = CANDLES_PER_REQUEST * interval_seconds * 1000

    # Calculate total pages for progress bar
    total_pages = ((now_ms - EARLIEST_TIMESTAMP_MS) // page_span_ms) + 1
    total_inserted = 0
    running_total = 0

    logger.info(
        "Fetching %s %s candles from 2020-01-01 to now (%d pages)",
        symbol, tf_label, total_pages,
    )

    async with aiohttp.ClientSession() as session:
        current_start_ms = EARLIEST_TIMESTAMP_MS
        with tqdm(total=total_pages, desc=f"  {tf_label}", unit="page") as pbar:
            while current_start_ms < now_ms:
                current_end_ms = min(current_start_ms + page_span_ms, now_ms)

                try:
                    klines = await _fetch_candles_page(
                        session, symbol, interval, current_start_ms, current_end_ms,
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
                    current_start_ms = current_end_ms
                    continue

                if klines:
                    rows = []
                    for k in klines:
                        try:
                            # Binance kline format: [open_time, o, h, l, c, vol, close_time, ...]
                            ts = int(k[0]) // 1000  # ms → seconds
                            rows.append((
                                ts,
                                tf_label,
                                float(k[1]),  # open
                                float(k[2]),  # high
                                float(k[3]),  # low
                                float(k[4]),  # close
                                float(k[5]),  # volume
                            ))
                        except (IndexError, ValueError, TypeError) as exc:
                            logger.warning("Skipping malformed kline: %s", exc)

                    if rows:
                        inserted = await insert_candles_bulk(rows, symbol=symbol)
                        total_inserted += inserted
                        running_total += len(rows)

                        if running_total % LOG_EVERY_N < len(rows):
                            logger.info(
                                "%s %s: %d candles processed, %d inserted",
                                symbol, tf_label, running_total, total_inserted,
                            )

                pbar.update(1)
                current_start_ms = current_end_ms
                await asyncio.sleep(RATE_LIMIT_DELAY)

    return total_inserted


async def run_historical_fetch(symbol: str = DEFAULT_SYMBOL) -> None:
    """Fetch historical candles for all timeframes and store in the database."""
    _setup_logging()
    await init_db()

    print("=" * 60)
    print("oneQuant — Historical OHLCV Fetcher (Binance.US)")
    print(f"Symbol: {symbol}")
    print("Lookback: 2020-01-01 to today")
    print(f"Timeframes: {', '.join(TIMEFRAME_MAP.keys())}")
    print("=" * 60)

    grand_total = 0
    for tf_label, (interval, interval_seconds) in TIMEFRAME_MAP.items():
        inserted = await _fetch_timeframe(tf_label, interval, interval_seconds, symbol)
        logger.info("%s %s: inserted %d candles", symbol, tf_label, inserted)
        print(f"  {tf_label}: {inserted:,} candles inserted")
        grand_total += inserted

    print(f"\nTotal candles inserted: {grand_total:,}")
    logger.info("Historical fetch complete — %d total candles inserted", grand_total)
    await close_db()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch historical OHLCV candles from Binance.US")
    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help=f"Binance.US symbol to fetch (default: {DEFAULT_SYMBOL})",
    )
    args = parser.parse_args()
    asyncio.run(run_historical_fetch(symbol=args.symbol))
