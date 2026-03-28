"""oneQuant v0.1 — data pipeline entry point.

Starts all data feeds concurrently:
  - Kraken WebSocket v2 (real-time BTC/USD candles)
  - Crypto news headline poller (every 15 minutes)
  - Fear & Greed Index poller (every 15 minutes)

Usage:
    cd onequant/
    python main.py
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

from config import config
from database.db import close_db, get_table_count, init_db, insert_system_log
from feeds.kraken_ws import run_kraken_ws
from feeds.news import run_fear_greed_poller, run_news_poller
from paper_trading.paper_engine import run_paper_engine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODULE_NAME: str = "main"
BANNER: str = "oneQuant v0.1 — data pipeline running"
TABLES: list[str] = ["btc_candles", "news_feed", "fear_greed", "system_log", "paper_trades"]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(MODULE_NAME)


def _setup_logging() -> None:
    """Configure file and console logging for the main module."""
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    Path("logs").mkdir(exist_ok=True)

    fh = logging.FileHandler("logs/main.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Startup helpers
# ---------------------------------------------------------------------------


async def _print_table_counts() -> None:
    """Print the current row count for every tracked table."""
    print("\nDatabase row counts:")
    for table in TABLES:
        count = await get_table_count(table)
        print(f"  {table}: {count:,}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    """Initialize the database and run all feeds concurrently."""
    _setup_logging()

    print(BANNER)
    print("=" * len(BANNER))

    await init_db()
    await _print_table_counts()

    logger.info("Starting all data feeds")
    await insert_system_log(MODULE_NAME, "INFO", "Data pipeline started")

    tasks = [
        asyncio.create_task(run_kraken_ws(), name="kraken_ws"),
        asyncio.create_task(run_news_poller(), name="crypto_news"),
        asyncio.create_task(run_fear_greed_poller(), name="fear_greed"),
        asyncio.create_task(run_paper_engine(), name="paper_engine"),
    ]

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows does not support add_signal_handler for SIGTERM
            pass

    try:
        # On Windows, KeyboardInterrupt will be raised directly
        done, pending = await asyncio.wait(
            [asyncio.create_task(shutdown_event.wait()), *tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down")
    finally:
        # Cancel all feed tasks
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await insert_system_log(MODULE_NAME, "INFO", "Data pipeline stopped")
        await close_db()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown complete")
    except Exception as exc:
        # Last-resort logging for truly unhandled exceptions
        logging.getLogger(MODULE_NAME).critical("Unhandled exception: %s", exc)
        sys.exit(1)
