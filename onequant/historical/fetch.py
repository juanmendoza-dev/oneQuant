"""Kraken historical OHLCV candle fetcher.

Fetches BTC/USD candles for 5m, 15m, and 1h timeframes going back to
2016-01-01 using the Kraken public OHLC endpoint. Paginates using the
'since' parameter, skips timestamps already in the database, and
displays a progress bar.

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
from typing import Any, Optional

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
BASE_URL: str = "https://api.kraken.com"
DEFAULT_PAIR: str = "XBTUSD"
RATE_LIMIT_DELAY: float = 1.0  # Kraken public rate limit: ~1 req/sec

# Kraken interval values (minutes) → db timeframe label
TIMEFRAME_MAP: dict[str, int] = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
}

SECONDS_PER_YEAR: int = 365 * 24 * 3600
# 2016-01-01 00:00:00 UTC — target start
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
# Fetch logic
# ---------------------------------------------------------------------------


async def _fetch_ohlc_page(
    session: aiohttp.ClientSession,
    pair: str,
    interval: int,
    since: Optional[int] = None,
) -> tuple[list[list], Optional[int]]:
    """Fetch a single page of OHLC data from Kraken.

    Returns (candles_list, last_timestamp) where last_timestamp is the
    'since' value for the next page, or None if no more data.
    """
    params: dict[str, Any] = {"pair": pair, "interval": interval}
    if since is not None:
        params["since"] = since

    async with session.get(f"{BASE_URL}/0/public/OHLC", params=params) as resp:
        if resp.status != 200:
            text = await resp.text()
            logger.error("OHLC API %s: %s", resp.status, text)
            return [], None
        data = await resp.json()
        errors = data.get("error", [])
        if errors:
            logger.error("Kraken API error: %s", errors)
            return [], None

        result = data.get("result", {})
        last = result.get("last")

        # Find the candle data (key is pair name, not "last")
        candles = []
        for key, value in result.items():
            if key != "last" and isinstance(value, list):
                candles = value
                break

        return candles, last


async def _fetch_timeframe(
    tf_label: str,
    interval: int,
    pair: str = DEFAULT_PAIR,
) -> int:
    """Fetch all historical candles for a single timeframe. Returns insert count."""
    now = int(time.time())
    interval_seconds = interval * 60
    total_candles_estimate = (now - EARLIEST_TIMESTAMP) // interval_seconds
    # Kraken returns ~720 candles per page
    total_pages_estimate = max(1, total_candles_estimate // 720)
    total_inserted = 0

    logger.info(
        "Fetching %s %s candles from %d to %d (~%d pages)",
        pair, tf_label, EARLIEST_TIMESTAMP, now, total_pages_estimate,
    )

    since = EARLIEST_TIMESTAMP

    async with aiohttp.ClientSession() as session:
        with tqdm(total=total_pages_estimate, desc=f"  {tf_label}", unit="page") as pbar:
            while since is not None:
                try:
                    candles, new_since = await _fetch_ohlc_page(
                        session, pair, interval, since
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
                    break

                if candles:
                    rows = []
                    for c in candles:
                        try:
                            # Kraken OHLC: [time, open, high, low, close, vwap, volume, count]
                            rows.append((
                                int(c[0]),
                                tf_label,
                                float(c[1]),
                                float(c[2]),
                                float(c[3]),
                                float(c[4]),
                                float(c[6]),  # volume is index 6
                            ))
                        except (IndexError, ValueError, TypeError) as exc:
                            logger.warning("Skipping malformed candle: %s", exc)

                    if rows:
                        inserted = await insert_candles_bulk(rows, symbol="BTC-USD")
                        total_inserted += inserted

                pbar.update(1)

                # Stop if we've caught up or no new since value
                if new_since is None or (since is not None and new_since == since):
                    break
                since = new_since

                await asyncio.sleep(RATE_LIMIT_DELAY)

    return total_inserted


async def run_historical_fetch(pair: str = DEFAULT_PAIR) -> None:
    """Fetch historical candles for all timeframes and store in the database."""
    _setup_logging()
    await init_db()

    print("=" * 60)
    print("oneQuant — Historical OHLCV Fetcher (Kraken)")
    print(f"Pair: {pair}")
    years = (int(time.time()) - EARLIEST_TIMESTAMP) / SECONDS_PER_YEAR
    print(f"Target lookback: {years:.1f} years (back to 2016-01-01)")
    print("=" * 60)

    grand_total = 0
    for tf_label, interval in TIMEFRAME_MAP.items():
        inserted = await _fetch_timeframe(tf_label, interval, pair)
        logger.info("%s %s: inserted %d candles", pair, tf_label, inserted)
        grand_total += inserted

    print(f"\nTotal candles inserted: {grand_total}")
    logger.info("Historical fetch complete — %d total candles inserted", grand_total)
    await close_db()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch historical OHLCV candles from Kraken")
    parser.add_argument(
        "--pair",
        default=DEFAULT_PAIR,
        help=f"Kraken pair to fetch (default: {DEFAULT_PAIR})",
    )
    args = parser.parse_args()
    asyncio.run(run_historical_fetch(pair=args.pair))
