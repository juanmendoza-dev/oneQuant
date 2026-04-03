"""Market Maker Strategy for BTC/USD on Binance.US.

Places buy and sell limit orders around current price.
Collects the spread when both sides fill.
Uses 0% maker fees for pure profit.

Paper trading mode: simulates fills using real price data.
Live trading mode: places real orders on Binance.US.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from config import config
from database.db import insert_mm_trade, insert_system_log
from feeds.binance_rest import get_ticker
from safety.circuit_breaker import CircuitBreaker
from safety.fee_monitor import FeeMonitor
from safety.kill_switch import is_kill_switch_active
from safety.order_validator import OrderValidator

logger = logging.getLogger("market_maker")

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class MMOrder:
    """Represents a single market maker order."""
    side: str
    price: float
    quantity_btc: float
    value_usd: float
    order_id: Optional[str] = None
    filled: bool = False
    fill_price: float = 0.0
    fill_time: int = 0


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------


class MarketMakerStrategy:
    """Market making strategy — places bid/ask around mid price.

    Inventory management:
    - Track BTC inventory (net position)
    - If BTC value > max_inventory_pct of capital: skip buy orders
    - If BTC value < (1 - max_inventory_pct) of capital: skip sell orders
    """

    STRATEGY_NAME = "market_maker"

    def __init__(
        self,
        capital_usd: float,
        spread_pct: float,
        paper_trading: bool,
        circuit_breaker: CircuitBreaker,
        fee_monitor: FeeMonitor,
        order_validator: OrderValidator,
        max_inventory_pct: float = 0.8,
    ):
        self.capital_usd = capital_usd
        self.spread_pct = spread_pct
        self.paper_trading = paper_trading
        self.circuit_breaker = circuit_breaker
        self.fee_monitor = fee_monitor
        self.order_validator = order_validator
        self.max_inventory_pct = max_inventory_pct

        self.btc_inventory: float = 0.0
        self.usd_inventory: float = capital_usd
        self.active_buy: Optional[MMOrder] = None
        self.active_sell: Optional[MMOrder] = None
        self.total_round_trips: int = 0
        self.total_spread_collected: float = 0.0
        self.best_spread: float = 0.0
        self.last_price: float = 0.0
        self._cycle_count: int = 0

        mode = "PAPER" if paper_trading else "LIVE"
        logger.info(
            "Market Maker initialized: $%.2f capital, %.3f%% spread, %s mode",
            capital_usd, spread_pct * 100, mode,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _order_quantity(self, price: float) -> float:
        """Order size = 40% of capital in BTC."""
        order_value = self.capital_usd * 0.4
        return round(order_value / price, 6)

    def _bid_ask(self, mid: float) -> tuple[float, float]:
        """Return (bid, ask) around mid price."""
        half = self.spread_pct / 2
        return round(mid * (1 - half), 2), round(mid * (1 + half), 2)

    def _inventory_value(self, price: float) -> float:
        """BTC inventory value in USD."""
        return self.btc_inventory * price

    def _should_skip_buy(self, price: float) -> bool:
        """Skip buys if BTC inventory is too high."""
        inv_pct = self._inventory_value(price) / self.capital_usd if self.capital_usd > 0 else 0
        return inv_pct > self.max_inventory_pct

    def _should_skip_sell(self, price: float) -> bool:
        """Skip sells if BTC inventory is too low (nothing to sell)."""
        return self.btc_inventory <= 0 and self.active_buy is None

    # ------------------------------------------------------------------
    # Paper fill simulation
    # ------------------------------------------------------------------

    async def _paper_check_fills(self, price: float) -> None:
        """Simulate fills: buy fills if price <= bid, sell fills if price >= ask."""
        now = int(time.time())

        if self.active_buy and not self.active_buy.filled:
            if price <= self.active_buy.price:
                self.active_buy.filled = True
                self.active_buy.fill_price = self.active_buy.price
                self.active_buy.fill_time = now
                self.btc_inventory += self.active_buy.quantity_btc
                self.usd_inventory -= self.active_buy.value_usd
                logger.info(
                    "[PAPER] BUY filled: %.6f BTC @ $%.2f",
                    self.active_buy.quantity_btc, self.active_buy.price,
                )
                await insert_mm_trade(
                    timestamp=now, side="BUY",
                    price=self.active_buy.price,
                    quantity_btc=self.active_buy.quantity_btc,
                    value_usd=self.active_buy.value_usd,
                    fee_usd=0.0, order_id="paper",
                    status="FILLED", paper_trade=True,
                )

        if self.active_sell and not self.active_sell.filled:
            if price >= self.active_sell.price:
                self.active_sell.filled = True
                self.active_sell.fill_price = self.active_sell.price
                self.active_sell.fill_time = now
                self.btc_inventory -= self.active_sell.quantity_btc
                self.usd_inventory += self.active_sell.value_usd
                logger.info(
                    "[PAPER] SELL filled: %.6f BTC @ $%.2f",
                    self.active_sell.quantity_btc, self.active_sell.price,
                )
                await insert_mm_trade(
                    timestamp=now, side="SELL",
                    price=self.active_sell.price,
                    quantity_btc=self.active_sell.quantity_btc,
                    value_usd=self.active_sell.value_usd,
                    fee_usd=0.0, order_id="paper",
                    status="FILLED", paper_trade=True,
                )

    # ------------------------------------------------------------------
    # Round trip recording
    # ------------------------------------------------------------------

    async def _check_round_trip(self) -> None:
        """If both buy and sell filled, record the spread profit."""
        if not (self.active_buy and self.active_sell):
            return
        if not (self.active_buy.filled and self.active_sell.filled):
            return

        spread_usd = (
            (self.active_sell.fill_price - self.active_buy.fill_price)
            * self.active_buy.quantity_btc
        )
        self.total_round_trips += 1
        self.total_spread_collected += spread_usd
        if spread_usd > self.best_spread:
            self.best_spread = spread_usd

        logger.info(
            "Round trip #%d: +$%.4f spread (total: $%.4f)",
            self.total_round_trips, spread_usd, self.total_spread_collected,
        )

        self.circuit_breaker.record_trade_pnl(
            self.STRATEGY_NAME, spread_usd, won=spread_usd > 0,
        )

        # Clear for next pair
        self.active_buy = None
        self.active_sell = None

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    async def run_cycle(self, current_price: float) -> None:
        """One market-making cycle. Called every MM_ORDER_REFRESH_SEC."""
        self._cycle_count += 1

        # Safety gate
        allowed, reason = self.circuit_breaker.is_trading_allowed(self.STRATEGY_NAME)
        if not allowed:
            logger.warning("Market maker paused: %s", reason)
            return

        if is_kill_switch_active():
            logger.warning("Market maker paused: kill switch active")
            return

        self.last_price = current_price
        bid, ask = self._bid_ask(current_price)
        qty = self._order_quantity(current_price)

        # --- paper mode ---
        if self.paper_trading:
            await self._paper_check_fills(current_price)
            await self._check_round_trip()

            # Place new buy if needed
            if not self.active_buy and not self._should_skip_buy(current_price):
                self.active_buy = MMOrder(
                    side="BUY", price=bid,
                    quantity_btc=qty, value_usd=qty * bid,
                )
                logger.info("[PAPER] BUY order: %.6f BTC @ $%.2f", qty, bid)

            # Place new sell if needed
            if not self.active_sell and not self._should_skip_sell(current_price):
                self.active_sell = MMOrder(
                    side="SELL", price=ask,
                    quantity_btc=qty, value_usd=qty * ask,
                )
                logger.info("[PAPER] SELL order: %.6f BTC @ $%.2f", qty, ask)

            # Re-quote if price moved >0.5% from existing orders
            if self.active_buy and not self.active_buy.filled:
                drift = abs(current_price - self.active_buy.price) / current_price
                if drift > 0.005:
                    self.active_buy = MMOrder(
                        side="BUY", price=bid,
                        quantity_btc=qty, value_usd=qty * bid,
                    )
                    logger.info("[PAPER] BUY re-quoted: %.6f BTC @ $%.2f", qty, bid)

            if self.active_sell and not self.active_sell.filled:
                drift = abs(current_price - self.active_sell.price) / current_price
                if drift > 0.005:
                    self.active_sell = MMOrder(
                        side="SELL", price=ask,
                        quantity_btc=qty, value_usd=qty * ask,
                    )
                    logger.info("[PAPER] SELL re-quoted: %.6f BTC @ $%.2f", qty, ask)

        # --- live mode (future) ---
        else:
            # TODO: implement live order placement via binance_rest
            logger.warning("Live trading not yet implemented")

        # Status log every 10 minutes (20 cycles at 30s)
        if self._cycle_count % 20 == 0:
            mode = "PAPER" if self.paper_trading else "LIVE"
            logger.info(
                "MM Status [%s]: %d round trips, $%.4f spread, "
                "BTC inv=%.6f, USD inv=$%.2f, price=$%.2f",
                mode, self.total_round_trips, self.total_spread_collected,
                self.btc_inventory, self.usd_inventory, current_price,
            )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> dict:
        """Current market maker state for API/dashboard."""
        return {
            "strategy": self.STRATEGY_NAME,
            "mode": "PAPER" if self.paper_trading else "LIVE",
            "capital_usd": self.capital_usd,
            "btc_inventory": self.btc_inventory,
            "usd_inventory": round(self.usd_inventory, 2),
            "total_round_trips": self.total_round_trips,
            "total_spread_collected": round(self.total_spread_collected, 4),
            "best_spread_usd": round(self.best_spread, 4),
            "active_buy_price": self.active_buy.price if self.active_buy else None,
            "active_sell_price": self.active_sell.price if self.active_sell else None,
            "last_price": self.last_price,
            "spread_pct": self.spread_pct * 100,
        }


# ---------------------------------------------------------------------------
# Async runner (called from main.py)
# ---------------------------------------------------------------------------


async def run_market_maker(
    circuit_breaker: CircuitBreaker,
    fee_monitor: FeeMonitor,
    order_validator: OrderValidator,
) -> None:
    """Asyncio task: run market maker cycles forever."""
    from pathlib import Path
    Path("logs").mkdir(exist_ok=True)
    if not logger.handlers:
        fh = logging.FileHandler("logs/market_maker.log")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
        logger.setLevel(logging.DEBUG)

    mm = MarketMakerStrategy(
        capital_usd=config.MM_CAPITAL_USD,
        spread_pct=config.MM_SPREAD_PCT,
        paper_trading=config.MM_PAPER_TRADING,
        circuit_breaker=circuit_breaker,
        fee_monitor=fee_monitor,
        order_validator=order_validator,
        max_inventory_pct=config.MM_MAX_INVENTORY_PCT,
    )

    # Store reference for API access
    run_market_maker._instance = mm

    refresh = config.MM_ORDER_REFRESH_SEC

    while True:
        try:
            price = await get_ticker()
            if price is not None and price > 0:
                await mm.run_cycle(price)
            else:
                logger.warning("Could not fetch price — skipping cycle")
        except asyncio.CancelledError:
            logger.info("Market maker task cancelled — shutting down")
            return
        except Exception as exc:
            logger.error("Market maker error: %s", exc)
            try:
                await insert_system_log("market_maker", "ERROR", str(exc))
            except Exception:
                pass

        await asyncio.sleep(refresh)


# Accessor for API
run_market_maker._instance = None
