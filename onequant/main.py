"""oneQuant v0.2 — data pipeline entry point.

Starts all data feeds concurrently:
  - Binance.US WebSocket (real-time BTCUSD 5m + 15m candles)
  - Crypto news headline poller (every 15 minutes)
  - Fear & Greed Index poller (every 15 minutes)

Safety system initialized on startup:
  - Kill switch check
  - Circuit breakers (daily 5%, weekly 10%)
  - Fee monitor (0% maker verification)
  - Order validator (LIMIT only)
  - Position sizer (2% risk rule)

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
from feeds.binance_rest import get_ticker
from feeds.binance_ws import run_binance_ws
from feeds.news import run_fear_greed_poller, run_news_poller
from paper_trading.paper_engine import run_paper_engine
from safety.kill_switch import is_kill_switch_active, get_kill_switch_reason
from safety.circuit_breaker import CircuitBreaker
from safety.fee_monitor import FeeMonitor
from safety.order_validator import OrderValidator
from safety.position_sizer import PositionSizer
from strategies.market_maker import MarketMakerStrategy

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODULE_NAME: str = "main"
BANNER: str = "oneQuant v0.2 — data pipeline running (Binance.US)"
TABLES: list[str] = ["btc_candles", "news_feed", "fear_greed", "system_log", "paper_trades",
                      "market_maker_trades", "market_maker_stats"]
ACCOUNT_BALANCE: float = 300.0  # approximate starting capital

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(MODULE_NAME)


def _setup_logging() -> None:
    """Configure file and console logging for the main module and market maker."""
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    Path("logs").mkdir(exist_ok=True)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")

    fh = logging.FileHandler("logs/main.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    # Also configure market maker and safety loggers so they output to console/file
    for name in ("strategies.market_maker", "circuit_breaker", "order_validator"):
        sub = logging.getLogger(name)
        sub.setLevel(logging.DEBUG)
        sub.addHandler(fh)
        sub.addHandler(ch)


# ---------------------------------------------------------------------------
# Startup helpers
# ---------------------------------------------------------------------------


async def run_market_maker(circuit_breaker: CircuitBreaker, order_validator: OrderValidator) -> None:
    """Run the market maker strategy in a loop."""
    mm = MarketMakerStrategy(
        capital_usd=config.MM_CAPITAL_USD,
        spread_pct=config.MM_SPREAD_PCT,
        paper_trading=config.MM_PAPER_TRADING,
        circuit_breaker=circuit_breaker,
        order_validator=order_validator,
    )

    await insert_system_log(MODULE_NAME, "INFO", "Market maker started")
    logger.info("Market maker task started — %s mode", "PAPER" if config.MM_PAPER_TRADING else "LIVE")

    cycle_count = 0
    while True:
        try:
            price = await get_ticker()
            if price is None:
                logger.warning("Market maker: failed to get price, skipping cycle")
                await asyncio.sleep(config.MM_ORDER_REFRESH_SEC)
                continue

            await mm.run_cycle(price)
            cycle_count += 1

            # Log status every 10 minutes (20 cycles at 30s)
            if cycle_count % 20 == 0:
                status = mm.get_status()
                logger.info(
                    "MM Status: %d round trips, $%.4f spread collected, "
                    "BTC inventory: %.6f, USD: $%.2f",
                    status["total_round_trips"],
                    status["total_spread_collected"],
                    status["btc_inventory"],
                    status["usd_inventory"],
                )

        except Exception as exc:
            logger.error("Market maker cycle error: %s", exc)
            await insert_system_log(MODULE_NAME, "ERROR", f"Market maker error: {exc}")

        await asyncio.sleep(config.MM_ORDER_REFRESH_SEC)


async def _print_table_counts() -> None:
    """Print the current row count for every tracked table."""
    print("\nDatabase row counts:")
    for table in TABLES:
        count = await get_table_count(table)
        print(f"  {table}: {count:,}")
    print()


def _init_safety() -> tuple[CircuitBreaker, FeeMonitor, OrderValidator, PositionSizer]:
    """Initialize all safety components. Exits if kill switch is active."""
    # Check kill switch first
    if is_kill_switch_active():
        reason = get_kill_switch_reason()
        logger.critical("KILL SWITCH ACTIVE — refusing to start: %s", reason)
        print(f"KILL SWITCH ACTIVE — refusing to start\n{reason}")
        sys.exit(1)

    circuit_breaker = CircuitBreaker(account_balance=ACCOUNT_BALANCE)
    fee_monitor = FeeMonitor()
    order_validator = OrderValidator()
    position_sizer = PositionSizer()

    # Check circuit breakers
    allowed, cb_reason = circuit_breaker.is_trading_allowed()
    if not allowed:
        logger.warning("Circuit breaker active on startup: %s", cb_reason)
        print(f"WARNING: Circuit breaker active: {cb_reason}")
        print("Data feeds will still run. Trading is paused.")
    else:
        logger.info("Circuit breakers: ALL CLEAR")

    logger.info("Safety system initialized")
    logger.info("  Kill switch: INACTIVE")
    logger.info("  Circuit breakers: %s", "ACTIVE — " + cb_reason if not allowed else "CLEAR")
    logger.info("  Fee monitor: READY (expecting 0%% maker)")
    logger.info("  Order validator: READY (LIMIT only)")
    logger.info("  Position sizer: READY (2%% risk rule)")
    print("Safety system initialized")

    return circuit_breaker, fee_monitor, order_validator, position_sizer


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    """Initialize the database, safety system, and run all feeds concurrently."""
    _setup_logging()

    print(BANNER)
    print("=" * len(BANNER))

    # Initialize safety system before anything else
    circuit_breaker, fee_monitor, order_validator, position_sizer = _init_safety()

    await init_db()
    await _print_table_counts()

    logger.info("Starting all data feeds")
    await insert_system_log(MODULE_NAME, "INFO", "Data pipeline started — safety system active")

    tasks = [
        asyncio.create_task(run_binance_ws(), name="binance_ws"),
        asyncio.create_task(run_news_poller(), name="crypto_news"),
        asyncio.create_task(run_fear_greed_poller(), name="fear_greed"),
        asyncio.create_task(run_paper_engine(), name="paper_engine"),
        asyncio.create_task(run_market_maker(circuit_breaker, order_validator), name="market_maker"),
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
            pass

    try:
        done, pending = await asyncio.wait(
            [asyncio.create_task(shutdown_event.wait()), *tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down")
    finally:
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
        logging.getLogger(MODULE_NAME).critical("Unhandled exception: %s", exc)
        sys.exit(1)
