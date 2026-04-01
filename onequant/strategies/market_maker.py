"""
Market Maker Strategy for BTC/USD on Binance.US.
Places buy and sell limit orders around current price.
Collects the spread when both sides fill.
Uses 0% maker fees for pure profit.

Paper trading mode: simulates fills using real price data.
Live trading mode: places real orders on Binance.US.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional

from database.db import insert_mm_trade

logger = logging.getLogger(__name__)


@dataclass
class MMOrder:
    side: str           # BUY or SELL
    price: float
    quantity_btc: float
    value_usd: float
    order_id: Optional[str] = None
    filled: bool = False
    fill_price: float = 0.0
    fill_time: str = ""


class MarketMakerStrategy:
    """
    Market making strategy.

    Logic:
    1. Get current BTC/USD mid price
    2. Calculate bid = mid * (1 - spread/2)
    3. Calculate ask = mid * (1 + spread/2)
    4. Place LIMIT buy at bid
    5. Place LIMIT sell at ask
    6. Monitor for fills every 30 seconds
    7. When buy fills: wait for sell to fill
    8. When both fill: record round trip profit
    9. Replace orders immediately
    10. Check safety system before every cycle

    Inventory management:
    - Track BTC inventory (net position)
    - If inventory > 80% of capital: skip buy orders
    - If inventory < 20% of capital: skip sell orders
    - This prevents one-sided inventory buildup
    """

    STRATEGY_NAME = "market_maker"

    def __init__(
        self,
        capital_usd: float,
        spread_pct: float,
        paper_trading: bool,
        circuit_breaker,
        order_validator,
        db_conn=None,
        binance_rest=None,
    ):
        self.capital_usd = capital_usd
        self.spread_pct = spread_pct
        self.paper_trading = paper_trading
        self.circuit_breaker = circuit_breaker
        self.order_validator = order_validator
        self.db = db_conn
        self.binance = binance_rest

        self.btc_inventory = 0.0
        self.usd_inventory = capital_usd
        self.active_buy_order: Optional[MMOrder] = None
        self.active_sell_order: Optional[MMOrder] = None
        self.total_round_trips = 0
        self.total_spread_collected = 0.0
        self.last_price = 0.0

        logger.info(
            "Market Maker initialized: "
            "$%.2f capital, %.3f%% spread, %s mode",
            capital_usd, spread_pct * 100,
            "PAPER" if paper_trading else "LIVE",
        )

    def _get_order_quantity(self, price: float) -> float:
        """Calculate order size in BTC."""
        order_value = self.capital_usd * 0.4
        return round(order_value / price, 6)

    def _calculate_spread(self, mid_price: float) -> tuple[float, float]:
        """Returns (bid_price, ask_price)."""
        half_spread = self.spread_pct / 2
        bid = mid_price * (1 - half_spread)
        ask = mid_price * (1 + half_spread)
        return round(bid, 2), round(ask, 2)

    def _inventory_allows_buy(self) -> bool:
        """Check if BTC inventory is below max threshold."""
        if self.last_price <= 0:
            return True
        btc_value = self.btc_inventory * self.last_price
        total_value = btc_value + self.usd_inventory
        if total_value <= 0:
            return False
        return (btc_value / total_value) < 0.8

    def _inventory_allows_sell(self) -> bool:
        """Check if we have BTC to sell."""
        return self.btc_inventory > 0

    async def _paper_check_fills(self, current_price: float):
        """
        Simulate fills in paper trading mode.
        Buy order fills if current price <= bid price.
        Sell order fills if current price >= ask price.
        """
        now = datetime.now(timezone.utc).isoformat()

        if (self.active_buy_order and
                not self.active_buy_order.filled):
            if current_price <= self.active_buy_order.price:
                self.active_buy_order.filled = True
                self.active_buy_order.fill_price = self.active_buy_order.price
                self.active_buy_order.fill_time = now
                self.btc_inventory += self.active_buy_order.quantity_btc
                self.usd_inventory -= self.active_buy_order.value_usd
                logger.info(
                    "[PAPER] BUY filled: %.6f BTC @ $%.2f",
                    self.active_buy_order.quantity_btc,
                    self.active_buy_order.price,
                )
                await insert_mm_trade(
                    timestamp=now,
                    side="BUY",
                    price=self.active_buy_order.price,
                    quantity_btc=self.active_buy_order.quantity_btc,
                    value_usd=self.active_buy_order.value_usd,
                    fee_usd=0.0,
                    order_id="PAPER",
                    status="FILLED",
                    paper_trade=True,
                )

        if (self.active_sell_order and
                not self.active_sell_order.filled):
            if current_price >= self.active_sell_order.price:
                self.active_sell_order.filled = True
                self.active_sell_order.fill_price = self.active_sell_order.price
                self.active_sell_order.fill_time = now
                self.btc_inventory -= self.active_sell_order.quantity_btc
                self.usd_inventory += self.active_sell_order.value_usd
                logger.info(
                    "[PAPER] SELL filled: %.6f BTC @ $%.2f",
                    self.active_sell_order.quantity_btc,
                    self.active_sell_order.price,
                )
                await insert_mm_trade(
                    timestamp=now,
                    side="SELL",
                    price=self.active_sell_order.price,
                    quantity_btc=self.active_sell_order.quantity_btc,
                    value_usd=self.active_sell_order.value_usd,
                    fee_usd=0.0,
                    order_id="PAPER",
                    status="FILLED",
                    paper_trade=True,
                )

    async def _record_round_trip(self):
        """Record completed round trip profit."""
        if (not self.active_buy_order or
                not self.active_sell_order):
            return
        if (not self.active_buy_order.filled or
                not self.active_sell_order.filled):
            return

        spread_usd = (
            self.active_sell_order.fill_price -
            self.active_buy_order.fill_price
        ) * self.active_buy_order.quantity_btc

        self.total_round_trips += 1
        self.total_spread_collected += spread_usd

        logger.info(
            "Round trip #%d: +$%.4f spread collected (total: $%.4f)",
            self.total_round_trips, spread_usd, self.total_spread_collected,
        )

        # Record in circuit breaker
        self.circuit_breaker.record_trade_pnl(
            self.STRATEGY_NAME,
            spread_usd,
            won=spread_usd > 0,
        )

        # Update the sell trade record with spread collected
        now = datetime.now(timezone.utc).isoformat()
        await insert_mm_trade(
            timestamp=now,
            side="ROUND_TRIP",
            price=self.active_sell_order.fill_price,
            quantity_btc=self.active_buy_order.quantity_btc,
            value_usd=spread_usd,
            fee_usd=0.0,
            order_id="PAPER",
            status="FILLED",
            paper_trade=self.paper_trading,
            spread_collected_usd=spread_usd,
        )

    async def run_cycle(self, current_price: float):
        """
        Main market making cycle.
        Called every 30 seconds.
        """
        # Safety check first
        allowed, reason = self.circuit_breaker.is_trading_allowed(
            self.STRATEGY_NAME
        )
        if not allowed:
            logger.warning("Market maker paused: %s", reason)
            return

        self.last_price = current_price
        bid, ask = self._calculate_spread(current_price)
        qty = self._get_order_quantity(current_price)

        if self.paper_trading:
            # Check for paper fills
            await self._paper_check_fills(current_price)

            # Record completed round trips
            if (self.active_buy_order and
                    self.active_sell_order and
                    self.active_buy_order.filled and
                    self.active_sell_order.filled):
                await self._record_round_trip()
                self.active_buy_order = None
                self.active_sell_order = None

            # Place new orders if needed
            if not self.active_buy_order and self._inventory_allows_buy():
                self.active_buy_order = MMOrder(
                    side="BUY",
                    price=bid,
                    quantity_btc=qty,
                    value_usd=qty * bid,
                )
                logger.info("[PAPER] BUY order: %.6f BTC @ $%.2f", qty, bid)

            if not self.active_sell_order and self._inventory_allows_sell():
                self.active_sell_order = MMOrder(
                    side="SELL",
                    price=ask,
                    quantity_btc=qty,
                    value_usd=qty * ask,
                )
                logger.info("[PAPER] SELL order: %.6f BTC @ $%.2f", qty, ask)
            elif not self.active_sell_order and not self._inventory_allows_sell():
                # First cycle: place sell anyway to allow round trips
                if self.total_round_trips == 0 and self.active_buy_order:
                    self.active_sell_order = MMOrder(
                        side="SELL",
                        price=ask,
                        quantity_btc=qty,
                        value_usd=qty * ask,
                    )
                    logger.info("[PAPER] SELL order (initial): %.6f BTC @ $%.2f", qty, ask)

        else:
            # LIVE TRADING
            # Cancel stale orders if price moved too much
            if self.active_buy_order:
                price_move = abs(
                    current_price - self.active_buy_order.price
                ) / current_price
                if price_move > 0.005:  # 0.5% move
                    await self.binance.cancel_order(
                        self.active_buy_order.order_id
                    )
                    self.active_buy_order = None

            if self.active_sell_order:
                price_move = abs(
                    current_price - self.active_sell_order.price
                ) / current_price
                if price_move > 0.005:
                    await self.binance.cancel_order(
                        self.active_sell_order.order_id
                    )
                    self.active_sell_order = None

            # Place fresh orders
            if not self.active_buy_order and self._inventory_allows_buy():
                valid, reason = self.order_validator.validate(
                    order_type="LIMIT",
                    side="BUY",
                    quantity_btc=qty,
                    price=bid,
                    stop_loss_price=bid * 0.97,
                    account_balance=self.capital_usd,
                )
                if valid:
                    order = await self.binance.place_limit_order(
                        side="BUY",
                        quantity=qty,
                        price=bid,
                    )
                    if order:
                        self.active_buy_order = MMOrder(
                            side="BUY",
                            price=bid,
                            quantity_btc=qty,
                            value_usd=qty * bid,
                            order_id=order.get("orderId"),
                        )

            if not self.active_sell_order and self._inventory_allows_sell():
                valid, reason = self.order_validator.validate(
                    order_type="LIMIT",
                    side="SELL",
                    quantity_btc=qty,
                    price=ask,
                    stop_loss_price=ask * 1.03,
                    account_balance=self.capital_usd,
                )
                if valid:
                    order = await self.binance.place_limit_order(
                        side="SELL",
                        quantity=qty,
                        price=ask,
                    )
                    if order:
                        self.active_sell_order = MMOrder(
                            side="SELL",
                            price=ask,
                            quantity_btc=qty,
                            value_usd=qty * ask,
                            order_id=order.get("orderId"),
                        )

    def get_status(self) -> dict:
        """Returns current market maker status."""
        return {
            "strategy": self.STRATEGY_NAME,
            "mode": "PAPER" if self.paper_trading else "LIVE",
            "capital_usd": self.capital_usd,
            "btc_inventory": self.btc_inventory,
            "usd_inventory": round(self.usd_inventory, 2),
            "total_round_trips": self.total_round_trips,
            "total_spread_collected": round(self.total_spread_collected, 4),
            "active_buy_order": self.active_buy_order.price if self.active_buy_order else None,
            "active_sell_order": self.active_sell_order.price if self.active_sell_order else None,
            "last_price": self.last_price,
        }
